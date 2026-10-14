"""Tests run against a real Postgres (SKIP LOCKED, triggers and leases are the point).
Set TEST_DATABASE_URL; each test gets its own schema."""

from __future__ import annotations

import json
import os
import random
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from erpsync import db
from erpsync.crm_client import CrmClient
from erpsync.crm_mock import CrmState, create_crm_app, sign
from erpsync.inbox import InboxProcessor
from erpsync.relay import Relay
from erpsync.service import Settings, create_app

URL = os.environ.get("TEST_DATABASE_URL", "postgresql://postgres:test@localhost:55432/erpsync_test")
SECRET = "test-secret"


@pytest.fixture
def pool() -> Iterator[ConnectionPool[db.Conn]]:
    schema = f"t_{uuid.uuid4().hex[:10]}"
    admin = db.make_pool(URL, size=1)
    with admin.connection() as conn:
        conn.execute(f"create schema {schema}")
    p: ConnectionPool[db.Conn] = ConnectionPool(
        URL,
        min_size=1,
        max_size=10,
        kwargs={"row_factory": db.dict_row, "options": f"-c search_path={schema}"},
        open=True,
    )
    p.wait()
    db.migrate(p)
    yield p
    p.close()
    with admin.connection() as conn:
        conn.execute(f"drop schema {schema} cascade")
    admin.close()


class FlakyTransport(httpx.BaseTransport):
    """Lets the request reach the CRM, then pretends the response never came back."""

    def __init__(self, inner: httpx.BaseTransport, rate: float, rng: random.Random) -> None:
        self.inner, self.rate, self.rng = inner, rate, rng

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self.inner.handle_request(request)
        if request.method != "GET" and self.rng.random() < self.rate:
            response.read()
            raise httpx.ReadTimeout("simulated timeout after the CRM committed", request=request)
        return response


class Harness:
    def __init__(self, pool: ConnectionPool[db.Conn], seed: int = 0) -> None:
        self.pool = pool
        self.rng = random.Random(seed)
        self.crm_state = CrmState()
        crm_app = create_crm_app(self.crm_state)
        # Two clients on the same CRM: the sync's (which can be made flaky) and the one the
        # tests use to play CRM users clicking in the UI (always reliable).
        sync_http = TestClient(crm_app, base_url="http://crm")
        self.flaky = FlakyTransport(sync_http._transport, 0.0, self.rng)
        sync_http._transport = self.flaky
        self.crm = CrmClient(sync_http)
        self.crm_http = TestClient(crm_app, base_url="http://crm")
        self.relay = Relay(pool, self.crm, base_delay=0, max_delay=0, rng=self.rng)
        self.inbox = InboxProcessor(pool)
        settings = Settings(webhook_secret=SECRET, database_url=URL)
        self.service = TestClient(create_app(settings, pool))
        self.delivered = 0
        self.dropped = 0

    # --- ERP side (plain SQL, as the ERP application would write) ---
    def erp_create(self, **values: Any) -> str:
        values = {"email": None, "phone": None, "credit_limit": 0, "status": "active", **values}
        with self.pool.connection() as conn:
            row = conn.execute(
                """insert into customers (name, email, phone, credit_limit, status)
                   values (%(name)s, %(email)s, %(phone)s, %(credit_limit)s, %(status)s)
                   returning id""",
                values,
            ).fetchone()
        assert row is not None
        return str(row["id"])

    def erp_update(self, customer_id: str, **changes: Any) -> None:
        sets = ", ".join(f"{k} = %({k})s" for k in changes)
        with self.pool.connection() as conn:
            conn.execute(
                f"update customers set {sets} where id = %(id)s", {**changes, "id": customer_id}
            )

    def erp(self, customer_id: str) -> dict[str, Any]:
        with self.pool.connection() as conn:
            row = conn.execute("select * from customers where id = %s", (customer_id,)).fetchone()
        assert row is not None
        return dict(row)

    def sql(self, query: str, *args: Any) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            cur = conn.execute(query, args)
            return [dict(r) for r in cur] if cur.description else []

    # --- CRM side ---
    def crm_edit(self, crm_id: str, **changes: Any) -> dict[str, Any]:
        return dict(self.crm_http.post(f"/_ui/contacts/{crm_id}", json=changes).json())

    def deliver(self, *, duplicate: float = 0.0, drop: float = 0.0, shuffle: bool = False) -> int:
        events = self.crm_state.drain_events()
        batch = []
        for e in events:
            if self.rng.random() < drop:
                self.dropped += 1
                continue
            batch.append(e)
            if self.rng.random() < duplicate:
                batch.append(e)
        if shuffle:
            self.rng.shuffle(batch)
        for e in batch:
            body = json.dumps(e).encode()
            r = self.service.post(
                "/webhooks/crm",
                content=body,
                headers={"X-CRM-Signature": sign(SECRET, body), "Content-Type": "application/json"},
            )
            assert r.status_code == 202, r.text
        self.delivered += len(batch)
        return len(batch)

    def drain(self, max_rounds: int = 10_000, **delivery: Any) -> None:
        """Run relay, webhooks and inbox until nothing is left to do."""
        for _ in range(max_rounds):
            self.sql("update outbox set available_at = now() where status = 'pending'")
            did = self.relay.run_once()
            did = self.deliver(**delivery) > 0 or did
            while self.inbox.run_once():
                did = True
            if not did:
                return
        raise AssertionError("sync did not quiesce")


@pytest.fixture
def h(pool: ConnectionPool[db.Conn]) -> Harness:
    return Harness(pool)
