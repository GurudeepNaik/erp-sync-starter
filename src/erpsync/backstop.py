"""Reconciliation backstop. Webhooks get lost - CRMs drop them during incidents, endpoints are
down during deploys. Periodically compare the CRM with what we last saw and feed any difference
through the normal inbox path, so there is one code path for applying CRM changes."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from erpsync.crm_client import CrmClient
from erpsync.db import Conn
from erpsync.policy import fields_of


@dataclass(frozen=True)
class BackstopReport:
    crm_contacts: int
    missed_crm_changes: int
    unsynced_erp_customers: int


def run_backstop(pool: ConnectionPool[Conn], crm: CrmClient) -> BackstopReport:
    contacts = crm.list_all()
    with pool.connection() as conn, conn.transaction():
        seen = {
            r["crm_id"]: (r["crm_snapshot"] or {}).get("version", 0)
            for r in conn.execute(
                """select c.crm_id, s.crm_snapshot from customers c
                     join sync_state s on s.customer_id = c.id where c.crm_id is not null"""
            )
        }
        missed = 0
        for contact in contacts:
            if contact["version"] > seen.get(contact["id"], 0):
                cur = conn.execute(
                    """insert into inbox (event_id, crm_id, crm_version, payload)
                       values (%s, %s, %s, %s) on conflict do nothing""",
                    (
                        f"backstop:{contact['id']}:{contact['version']}",
                        contact["id"],
                        contact["version"],
                        Jsonb(contact),
                    ),
                )
                missed += cur.rowcount
        # ERP customers that never reached the CRM and have nothing queued (e.g. their outbox
        # rows were dead-lettered and later fixed by hand).
        cur = conn.execute(
            """insert into outbox (customer_id, reason)
               select c.id, 'backstop' from customers c
                where c.crm_id is null
                  and not exists (
                      select 1 from outbox o
                       where o.customer_id = c.id and o.status in ('pending', 'dead'))"""
        )
        return BackstopReport(len(contacts), missed, cur.rowcount)


def compare(pool: ConnectionPool[Conn], crm: CrmClient) -> dict[str, list[str]]:
    """Read-only comparison of both systems. Empty lists mean they agree."""
    contacts = {c["id"]: c for c in crm.list_all()}
    with pool.connection() as conn:
        customers = list(conn.execute("select * from customers"))
    linked = {r["crm_id"] for r in customers if r["crm_id"]}
    erp_ids: list[str] = [str(c["erp_id"]) for c in contacts.values() if c.get("erp_id")]
    return {
        "not_in_crm": [str(r["id"]) for r in customers if not r["crm_id"]],
        "not_in_erp": sorted(set(contacts) - linked),
        "duplicated_in_crm": sorted({e for e in erp_ids if erp_ids.count(e) > 1}),
        "different": [
            str(r["id"])
            for r in customers
            if r["crm_id"] in contacts and fields_of(r) != fields_of(contacts[r["crm_id"]])
        ],
    }
