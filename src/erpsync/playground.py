"""Chaos playground: one whole sync scenario inside a single request, for the live demo.

It does what the chaos test does - edits on both sides while the CRM misbehaves and webhooks
are duplicated, reordered and lost - but counts everything so a page can show what happened.
No workers, no docker: the mock CRM runs in-process, the relay and inbox are stepped by hand
and each run gets its own Postgres schema, dropped afterwards.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.testclient import TestClient
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from erpsync import db
from erpsync.backstop import compare, run_backstop
from erpsync.crm_client import CrmClient
from erpsync.crm_mock import CrmState, Faults, create_crm_app, sign
from erpsync.inbox import InboxProcessor
from erpsync.policy import FIELDS
from erpsync.relay import Relay
from erpsync.service import Settings, create_app

NAMES = ["Acme", "Globex", "Initech", "Umbrella", "Hooli", "Stark", "Wayne", "Tyrell"]
SECRET = "playground-secret"
LOG_LIMIT = 160


class Scenario(BaseModel):
    """Knobs for one run. Rates are probabilities per request / per webhook."""

    seed: int = Field(default=1, ge=0, le=100_000)
    customers: int = Field(default=20, ge=1, le=60, description="ERP customers seeded first")
    operations: int = Field(default=150, ge=10, le=300, description="random edits on both sides")
    error_rate: float = Field(default=0.15, ge=0, le=0.5, description="CRM answers 500")
    rate_limit_rate: float = Field(default=0.05, ge=0, le=0.5, description="CRM answers 429")
    commit_then_fail_rate: float = Field(default=0.10, ge=0, le=0.5, description="saved, then 504")
    lost_response_rate: float = Field(default=0.05, ge=0, le=0.5, description="saved, reply lost")
    webhook_duplicate: float = Field(default=0.20, ge=0, le=1)
    webhook_drop: float = Field(default=0.05, ge=0, le=0.5)
    webhook_shuffle: bool = True
    bad_data_rate: float = Field(default=0.05, ge=0, le=0.5, description="creates with a bad email")


class _CountingTransport(httpx.BaseTransport):
    """Sits between the sync and the in-process CRM: counts what the CRM answered and, for a
    share of successful writes, pretends the reply never came back (the CRM did commit)."""

    def __init__(self, inner: httpx.BaseTransport, rng: random.Random, lost_rate: float, note: Any):
        self.inner, self.rng, self.lost_rate, self.note = inner, rng, lost_rate, note
        self.calls = 0
        self.by_status: dict[int, int] = {}
        self.lost = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self.inner.handle_request(request)
        self.calls += 1
        self.by_status[response.status_code] = self.by_status.get(response.status_code, 0) + 1
        what = f"{request.method} {request.url.path}"
        if response.status_code == 504:
            self.note(f"CRM saved {what} but answered 504 - the retry must not create a duplicate")
        elif response.status_code == 412:
            self.note(f"CRM answered 412 to {what}: the contact changed under us, re-reading")
        elif response.status_code == 422:
            self.note(f"CRM rejected {what} (422 invalid email) - dead-lettered, not retried")
        if (
            request.method != "GET"
            and response.status_code < 300
            and self.rng.random() < self.lost_rate
        ):
            response.read()
            self.lost += 1
            self.note(f"reply to {what} lost in transit after the CRM committed it")
            raise httpx.ReadTimeout("simulated timeout after the CRM committed", request=request)
        return response


@dataclass
class _Run:
    pool: ConnectionPool[db.Conn]
    scenario: Scenario
    log: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        s = self.scenario
        self.rng = random.Random(s.seed)
        self.state = CrmState()
        self.state.faults = Faults(s.error_rate, s.rate_limit_rate, s.commit_then_fail_rate, s.seed)
        self.state.rng.seed(s.seed)
        crm_app = create_crm_app(self.state)
        sync_http = TestClient(crm_app, base_url="http://crm")
        self.transport = _CountingTransport(
            sync_http._transport, self.rng, s.lost_response_rate, self.note
        )
        sync_http._transport = self.transport
        self.crm = CrmClient(sync_http)
        self.crm_ui = TestClient(crm_app, base_url="http://crm")  # CRM users: never fail
        self.relay = Relay(self.pool, self.crm, base_delay=0, max_delay=0, rng=self.rng)
        self.inbox = InboxProcessor(self.pool)
        self.service = TestClient(create_app(Settings(webhook_secret=SECRET), self.pool))
        self.poison: set[str] = set()

    def note(self, msg: str) -> None:
        if len(self.log) < LOG_LIMIT:
            self.log.append(msg)
        elif len(self.log) == LOG_LIMIT:
            self.log.append("… (log truncated)")

    def bump(self, key: str, by: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + by

    # --- ERP side: plain SQL, as the ERP application would write ---
    def erp_create(self, bad: bool) -> str:
        name = self.rng.choice(NAMES)
        values = {
            "name": name,
            "email": "not-an-email" if bad else f"x{self.rng.randint(1, 999)}@co.io",
            "credit_limit": self.rng.randint(0, 50) * 100,
        }
        with self.pool.connection() as conn:
            row = conn.execute(
                """insert into customers (name, email, credit_limit) values
                   (%(name)s, %(email)s, %(credit_limit)s) returning id""",
                values,
            ).fetchone()
        assert row is not None
        self.bump("erp_creates")
        if bad:
            self.poison.add(str(row["id"]))
            self.bump("bad_records")
            self.note(f"ERP: created {name} with an invalid email (the CRM will reject it)")
        return str(row["id"])

    def erp_update(self, customer_id: str) -> None:
        f = self.rng.choice(FIELDS)
        value: Any = {
            "name": f"{self.rng.choice(NAMES)} {self.rng.randint(1, 9)}",
            "email": f"erp{self.rng.randint(1, 999)}@co.io",
            "phone": f"555-{self.rng.randint(1000, 9999)}",
            "credit_limit": self.rng.randint(0, 50) * 100,
            "status": self.rng.choice(["active", "on_hold"]),
        }[f]
        with self.pool.connection() as conn:
            conn.execute(f"update customers set {f} = %s where id = %s", (value, customer_id))
        self.bump("erp_updates")

    # --- CRM side: people clicking in the CRM ---
    def crm_edit(self, crm_id: str) -> None:
        f = self.rng.choice(FIELDS)
        value = {
            "name": f"{self.rng.choice(NAMES)} CRM {self.rng.randint(1, 9)}",
            "email": f"crm{self.rng.randint(1, 999)}@co.io",
            "phone": f"555-{self.rng.randint(1000, 9999)}",
            "credit_limit": str(self.rng.randint(0, 50) * 100),
            "status": self.rng.choice(["active", "on_hold"]),
        }[f]
        self.crm_ui.post(f"/_ui/contacts/{crm_id}", json={f: value})
        self.bump("crm_edits")

    def crm_walk_in(self) -> None:
        self.crm_ui.post("/_ui/contacts", json={"name": f"Walk-in {self.rng.randint(1, 999)}"})
        self.bump("crm_walkins")

    def deliver(self, chaos: bool) -> int:
        s = self.scenario
        events = self.state.drain_events()
        self.bump("webhooks_emitted", len(events))
        batch = []
        for e in events:
            if chaos and self.rng.random() < s.webhook_drop:
                self.bump("webhooks_dropped")
                continue
            batch.append(e)
            if chaos and self.rng.random() < s.webhook_duplicate:
                batch.append(e)
                self.bump("webhooks_duplicated")
        if chaos and s.webhook_shuffle:
            self.rng.shuffle(batch)
        for e in batch:
            body = json.dumps(e).encode()
            r = self.service.post(
                "/webhooks/crm",
                content=body,
                headers={"X-CRM-Signature": sign(SECRET, body), "Content-Type": "application/json"},
            )
            if r.status_code != 202:
                raise RuntimeError(f"webhook rejected: {r.status_code} {r.text}")
            if not r.json()["stored"]:
                self.bump("webhooks_ignored_as_duplicate")
        self.bump("webhooks_delivered", len(batch))
        return len(batch)

    def drain(self, chaos: bool, deadline: float) -> None:
        """Relay, webhooks and inbox until nothing is left to do."""
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError("ran out of time before the sync settled")
            with (
                self.pool.connection() as conn
            ):  # skip backoff waits: the demo has no time for them
                conn.execute("update outbox set available_at = now() where status = 'pending'")
            did = self.relay.run_once()
            did = self.deliver(chaos) > 0 or did
            while self.inbox.run_once():
                did = True
            if not did:
                return

    def linked_contacts(self) -> list[str]:
        return [cid for cid, c in self.state.contacts.items() if c.get("erp_id")]

    def sql(self, query: str, *args: Any) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            return [dict(r) for r in conn.execute(query, args)]


def _run(pool: ConnectionPool[db.Conn], scenario: Scenario, budget_s: float) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + budget_s
    phases: dict[str, int] = {}

    def phase(name: str, since: float) -> float:
        now = time.monotonic()
        phases[name] = int((now - since) * 1000)
        return now

    r = _Run(pool, scenario)
    s = scenario
    t = time.monotonic()
    created = [r.erp_create(bad=False) for _ in range(s.customers)]
    r.note(f"seeded {s.customers} ERP customers; the outbox trigger queued them for the CRM")
    t = phase("seed", t)

    stopped_early = False
    for i in range(s.operations):
        if time.monotonic() > started + budget_s * 0.55:
            stopped_early = True
            r.note(
                f"stopped generating traffic after {i} operations to stay within the time budget"
            )
            break
        op = r.rng.random()
        linked = r.linked_contacts()
        if op < 0.12:
            cid = r.erp_create(bad=r.rng.random() < s.bad_data_rate)
            created.append(cid)
        elif op < 0.42:
            r.erp_update(r.rng.choice([c for c in created if c not in r.poison] or created))
        elif op < 0.70 and linked:
            r.crm_edit(r.rng.choice(linked))
        elif op < 0.75:
            r.crm_walk_in()
        else:
            # Let the sync make partial progress between edits, so they interleave.
            for _ in range(r.rng.randint(1, 4)):
                r.relay.run_once()
            r.deliver(chaos=True)
            for _ in range(r.rng.randint(1, 6)):
                r.inbox.run_once()
    t = phase("traffic", t)

    timed_out = False
    try:
        r.drain(chaos=True, deadline=deadline)
        t = phase("drain_with_faults", t)
        report = run_backstop(r.pool, r.crm)
        r.bump("webhooks_recovered_by_backstop", report.missed_crm_changes)
        if report.missed_crm_changes:
            r.note(
                f"backstop found {report.missed_crm_changes} CRM change(s) whose webhook was "
                "lost and queued them"
            )
        r.state.faults = Faults()
        r.transport.lost_rate = 0.0
        r.note("faults switched off; letting the sync finish")
        r.drain(chaos=False, deadline=deadline)
        t = phase("drain_clean", t)
        final_backstop = run_backstop(r.pool, r.crm)
    except TimeoutError as exc:
        timed_out = True
        r.note(str(exc))
        final_backstop = None
        t = phase("timed_out", t)

    diff = compare(r.pool, r.crm)
    outbox = {
        row["status"]: row
        for row in r.sql(
            """select status, count(*) as n, coalesce(sum(attempts), 0) as attempts
                 from outbox group by status"""
        )
    }
    inbox = {
        row["outcome"] or "pending": int(row["n"])
        for row in r.sql("select outcome, count(*) as n from inbox group by outcome")
    }
    dead = {
        row["customer_id"]
        for row in r.sql("select distinct customer_id::text from outbox where status = 'dead'")
    }
    customers = int(r.sql("select count(*) as n from customers")[0]["n"])
    contacts = len(r.state.contacts)
    phase("check", t)
    c = r.counts
    st = r.transport.by_status
    outbox_pending = int(outbox["pending"]["n"]) if "pending" in outbox else 0
    backstop_detail = (
        ""
        if final_backstop is None
        else f"{final_backstop.missed_crm_changes} missed, "
        f"{final_backstop.unsynced_erp_customers} unsynced"
    )

    proof = [
        {
            "check": "Every linked customer holds the same data in both systems",
            "ok": not diff["different"],
            "detail": f"{len(diff['different'])} differ",
        },
        {
            "check": "No contact exists on one side only (except rejected records)",
            "ok": set(diff["not_in_crm"]) == r.poison and not diff["not_in_erp"],
            "detail": f"{len(diff['not_in_crm'])} ERP-only, {len(diff['not_in_erp'])} CRM-only",
        },
        {
            "check": "No ERP customer was created twice in the CRM",
            "ok": not diff["duplicated_in_crm"],
            "detail": f"{len(diff['duplicated_in_crm'])} duplicated",
        },
        {
            "check": "Nothing left pending in the outbox or inbox",
            "ok": "pending" not in outbox and inbox.get("pending", 0) == 0,
            "detail": f"{outbox_pending} outbox, {inbox.get('pending', 0)} inbox",
        },
        {
            "check": "Only records the CRM rejected were dead-lettered",
            "ok": dead == r.poison,
            "detail": f"{len(dead)} dead-lettered, {len(r.poison)} rejected on purpose",
        },
        {
            "check": "Backstop finds nothing left to recover",
            "ok": final_backstop is not None
            and (final_backstop.missed_crm_changes, final_backstop.unsynced_erp_customers)
            == (0, 0),
            "detail": backstop_detail,
        },
    ]
    injected = (
        st.get(500, 0)
        + st.get(429, 0)
        + st.get(504, 0)
        + r.transport.lost
        + c.get("webhooks_dropped", 0)
        + c.get("webhooks_duplicated", 0)
    )
    any_fault_configured = any(
        [
            s.error_rate,
            s.rate_limit_rate,
            s.commit_then_fail_rate,
            s.lost_response_rate,
            s.webhook_drop,
            s.webhook_duplicate,
        ]
    )
    proof.append(
        {
            "check": "The chaos actually happened (otherwise convergence proves nothing)",
            "ok": injected > 0 or not any_fault_configured,
            "detail": f"{injected} faults injected",
        }
    )
    converged = not timed_out and all(p["ok"] for p in proof)

    return {
        "converged": converged,
        "timed_out": timed_out,
        "stopped_early": stopped_early,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "phases": phases,
        "edits": {
            k: c.get(k, 0)
            for k in ("erp_creates", "erp_updates", "crm_edits", "crm_walkins", "bad_records")
        },
        "crm": {
            "calls": r.transport.calls,
            "http_500": st.get(500, 0),
            "http_429": st.get(429, 0),
            "http_504_after_commit": st.get(504, 0),
            "lost_responses": r.transport.lost,
            "http_412_conflicts": st.get(412, 0),
            "http_422_rejected": st.get(422, 0),
        },
        "webhooks": {
            k.removeprefix("webhooks_"): c.get(k, 0)
            for k in (
                "webhooks_emitted",
                "webhooks_delivered",
                "webhooks_duplicated",
                "webhooks_dropped",
                "webhooks_ignored_as_duplicate",
                "webhooks_recovered_by_backstop",
            )
        },
        "outbox": {
            "sent": int(outbox["sent"]["n"]) if "sent" in outbox else 0,
            "dead": int(outbox["dead"]["n"]) if "dead" in outbox else 0,
            "pending": int(outbox["pending"]["n"]) if "pending" in outbox else 0,
            "failed_attempts": int(sum(int(v["attempts"]) for v in outbox.values())),
        },
        "inbox": inbox,
        "final": {
            "customers": customers,
            "contacts": contacts,
            "different": len(diff["different"]),
            "not_in_crm": len(diff["not_in_crm"]),
            "not_in_erp": len(diff["not_in_erp"]),
            "duplicated_in_crm": len(diff["duplicated_in_crm"]),
            "dead_letter_customers": len(dead),
        },
        "proof": proof,
        "log": r.log,
    }


def run_scenario(database_url: str, scenario: Scenario, budget_s: float = 45) -> dict[str, Any]:
    """Run one scenario in a throwaway schema and return what happened."""
    schema = f"run_{uuid.uuid4().hex[:10]}"  # "pg_" is reserved for system schemas
    admin = db.make_pool(database_url, size=1)
    try:
        with admin.connection() as conn:
            conn.execute(f"create schema {schema}")
        pool: ConnectionPool[db.Conn] = ConnectionPool(
            database_url,
            min_size=1,
            max_size=3,
            kwargs={"row_factory": dict_row, "options": f"-c search_path={schema}"},
            open=True,
        )
        try:
            pool.wait()
            db.migrate(pool)
            return _run(pool, scenario, budget_s)
        finally:
            pool.close()
            with admin.connection() as conn:
                conn.execute(f"drop schema {schema} cascade")
    finally:
        admin.close()


# ---- HTTP API -------------------------------------------------------------------------------

RUNS_DDL = """
create table if not exists playground_runs (
    id          uuid primary key,
    created_at  timestamptz not null default now(),
    scenario    jsonb not null,
    result      jsonb not null
)"""


def create_playground_app(database_url: str | None, budget_s: float = 45) -> FastAPI:
    app = FastAPI(title="erp-sync-starter playground")
    api = APIRouter(prefix="/api")
    ready = False

    def admin_pool() -> ConnectionPool[db.Conn]:
        nonlocal ready
        if not database_url:
            raise HTTPException(503, "no database connected (set DATABASE_URL)")
        pool = db.make_pool(database_url, size=1)
        if not ready:
            with pool.connection() as conn:
                conn.execute(RUNS_DDL)
            ready = True
        return pool

    def summary(row: dict[str, Any]) -> dict[str, Any]:
        res, sc = row["result"], row["scenario"]
        return {
            "id": str(row["id"]),
            "created_at": row["created_at"].isoformat(),
            "converged": res["converged"],
            "duration_ms": res["duration_ms"],
            "customers": res["final"]["customers"],
            "operations": sc["operations"],
            "faults": res["crm"]["http_500"]
            + res["crm"]["http_429"]
            + res["crm"]["http_504_after_commit"]
            + res["crm"]["lost_responses"]
            + res["webhooks"]["dropped"]
            + res["webhooks"]["duplicated"],
            "seed": sc["seed"],
        }

    @api.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "database": bool(database_url)}

    @api.post("/runs")
    def create_run(scenario: Scenario) -> dict[str, Any]:
        pool = admin_pool()  # 503 when there is no database
        assert database_url
        try:
            result = run_scenario(database_url, scenario, budget_s)
            run_id = uuid.uuid4()
            with pool.connection() as conn:
                conn.execute(
                    "insert into playground_runs (id, scenario, result) values (%s, %s, %s)",
                    (run_id, Jsonb(scenario.model_dump()), Jsonb(result)),
                )
        finally:
            pool.close()
        return {"id": str(run_id), "scenario": scenario.model_dump(), "result": result}

    @api.get("/runs")
    def list_runs(limit: int = 20) -> list[dict[str, Any]]:
        pool = admin_pool()
        try:
            with pool.connection() as conn:
                rows = list(
                    conn.execute(
                        "select * from playground_runs order by created_at desc limit %s",
                        (min(limit, 50),),
                    )
                )
        finally:
            pool.close()
        return [summary(dict(r)) for r in rows]

    @api.get("/runs/{run_id}")
    def get_run(run_id: uuid.UUID) -> dict[str, Any]:
        pool = admin_pool()
        try:
            with pool.connection() as conn:
                row = conn.execute(
                    "select * from playground_runs where id = %s", (run_id,)
                ).fetchone()
        finally:
            pool.close()
        if row is None:
            raise HTTPException(404, "no such run")
        return {
            "id": str(row["id"]),
            "created_at": row["created_at"].isoformat(),
            "scenario": row["scenario"],
            "result": row["result"],
        }

    app.include_router(api)
    return app
