"""The properties that matter for a sync are global, so test them globally: after any mix of
edits on both sides, failures, retries, duplicate / reordered / lost webhooks and concurrent
workers, both systems must hold the same data and nothing may be duplicated."""

from __future__ import annotations

import random
import socket
import threading
import time
from typing import Any

import httpx
import pytest
import uvicorn
from psycopg_pool import ConnectionPool

from erpsync import db
from erpsync.backstop import run_backstop
from erpsync.crm_client import CrmClient
from erpsync.crm_mock import CrmState, Faults, create_crm_app
from erpsync.policy import FIELDS, fields_of
from erpsync.relay import Relay
from tests.conftest import Harness

NAMES = ["Acme", "Globex", "Initech", "Umbrella", "Hooli", "Stark", "Wayne", "Tyrell"]


def assert_converged(h: Harness, poison: set[str]) -> dict[str, int]:
    customers = {str(r["id"]): r for r in h.sql("select * from customers")}
    contacts = h.crm_state.contacts
    by_crm = {r["crm_id"]: cid for cid, r in customers.items() if r["crm_id"]}

    assert h.sql("select count(*) as n from outbox where status = 'pending'") == [{"n": 0}]
    assert h.sql("select count(*) as n from inbox where processed_at is null") == [{"n": 0}]
    dead = {
        str(r["customer_id"])
        for r in h.sql("select distinct customer_id from outbox where status = 'dead'")
    }
    details = h.sql(
        "select customer_id::text, attempts, last_error from outbox where status = 'dead'"
    )
    assert dead == poison, [d for d in details if d["customer_id"] not in poison]

    # No duplicates: every CRM contact is linked to exactly one ERP customer and vice versa.
    assert set(by_crm) == set(contacts), "CRM contacts without an ERP customer (or the reverse)"
    erp_ids = [c["erp_id"] for c in contacts.values() if c.get("erp_id")]
    assert len(erp_ids) == len(set(erp_ids)), "an ERP customer was created twice in the CRM"

    for cid, row in customers.items():
        if cid in poison:
            assert row["crm_id"] is None
            continue
        assert fields_of(row) == fields_of(contacts[row["crm_id"]]), cid
    return {"customers": len(customers), "contacts": len(contacts), "dead": len(dead)}


@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_converges_under_chaos(pool: ConnectionPool[db.Conn], seed: int) -> None:
    h = Harness(pool, seed)
    rng = random.Random(seed)
    h.crm_state.faults = Faults(error_rate=0.15, rate_limit_rate=0.05, commit_then_fail_rate=0.1)
    h.crm_state.rng.seed(seed)
    h.flaky.rate = 0.05
    chaos: dict[str, Any] = {"duplicate": 0.2, "drop": 0.05, "shuffle": True}

    created: list[str] = []
    poison: set[str] = set()
    for _ in range(300):
        op = rng.random()
        linked = [
            r["crm_id"] for r in h.sql("select crm_id from customers where crm_id is not null")
        ]
        if op < 0.15 or not created:
            bad = rng.random() < 0.05
            cid = h.erp_create(
                name=rng.choice(NAMES),
                email="not-an-email" if bad else f"x{rng.randint(1, 999)}@co.io",
                credit_limit=rng.randint(0, 50) * 100,
            )
            created.append(cid)
            if bad:
                poison.add(cid)
        elif op < 0.45:
            cid = rng.choice([c for c in created if c not in poison] or created)
            field = rng.choice(FIELDS)
            value: Any = {
                "name": f"{rng.choice(NAMES)} {rng.randint(1, 9)}",
                "email": f"erp{rng.randint(1, 999)}@co.io",
                "phone": f"555-{rng.randint(1000, 9999)}",
                "credit_limit": rng.randint(0, 50) * 100,
                "status": rng.choice(["active", "on_hold"]),
            }[field]
            h.erp_update(cid, **{field: value})
        elif op < 0.72 and linked:
            field = rng.choice(FIELDS)
            value = {
                "name": f"{rng.choice(NAMES)} CRM {rng.randint(1, 9)}",
                "email": f"crm{rng.randint(1, 999)}@co.io",
                "phone": f"555-{rng.randint(1000, 9999)}",
                "credit_limit": str(rng.randint(0, 50) * 100),
                "status": rng.choice(["active", "on_hold"]),
            }[field]
            h.crm_edit(rng.choice(linked), **{field: value})
        elif op < 0.77:
            h.crm_http.post("/_ui/contacts", json={"name": f"Walk-in {rng.randint(1, 999)}"})
        else:
            # Let the sync make partial progress between edits, so they interleave.
            for _ in range(rng.randint(1, 4)):
                h.relay.run_once()
            h.deliver(**chaos)
            for _ in range(rng.randint(1, 6)):
                h.inbox.run_once()

    h.drain(**chaos)
    report = run_backstop(h.pool, h.crm)  # pick up whatever the dropped webhooks carried
    h.crm_state.faults = Faults()
    h.flaky.rate = 0.0
    h.drain()
    stats = assert_converged(h, poison)
    assert report.crm_contacts == stats["contacts"]

    final = run_backstop(h.pool, h.crm)
    assert (final.missed_crm_changes, final.unsynced_erp_customers) == (0, 0)
    # Prove the chaos actually happened, or the convergence above proves nothing.
    [retries] = h.sql("select coalesce(sum(attempts), 0) as n from outbox")
    [dupes] = h.sql("select count(*) as n from inbox")
    assert retries["n"] > 0, "no CRM failure was ever retried"
    assert h.delivered > dupes["n"] - report.missed_crm_changes, "no duplicate webhook delivered"
    assert h.dropped > 0, "no webhook was lost"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_concurrent_workers_never_race_on_a_customer(pool: ConnectionPool[db.Conn]) -> None:
    state = CrmState()
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(create_crm_app(state), port=port, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)

    h = Harness(pool)
    customers = [h.erp_create(name=f"Customer {i}", credit_limit=i) for i in range(150)]
    stop = threading.Event()
    errors: list[BaseException] = []

    def work() -> None:
        client = CrmClient(httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10))
        relay = Relay(pool, client, base_delay=0, max_delay=0)
        while not stop.is_set():
            try:
                if not relay.run_once():
                    time.sleep(0.01)
            except BaseException as exc:
                errors.append(exc)

    workers = [threading.Thread(target=work) for _ in range(4)]
    for w in workers:
        w.start()
    rng = random.Random(9)
    for _ in range(600):  # keep editing while the workers run
        h.erp_update(rng.choice(customers), credit_limit=rng.randint(1, 10_000))
    deadline = time.time() + 60
    while h.sql("select 1 from outbox where status = 'pending' limit 1") and time.time() < deadline:
        time.sleep(0.1)
    stop.set()
    for w in workers:
        w.join()
    server.should_exit = True

    assert not errors, errors
    assert h.sql("select count(*) as n from outbox where status <> 'sent'") == [{"n": 0}]
    assert len(state.contacts) == len(customers)  # no duplicate creates
    assert state.version_conflicts == 0  # leases: never two writers on one contact
    for row in h.sql("select * from customers"):
        assert fields_of(row) == fields_of(state.contacts[row["crm_id"]])
