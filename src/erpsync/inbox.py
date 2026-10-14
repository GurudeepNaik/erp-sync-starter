"""Inbound: CRM -> ERP.

Webhooks are verified, written to the inbox and acknowledged immediately; a worker applies them
later. Storing first means a slow ERP never makes the CRM give up on us, and the primary key on
event_id turns at-least-once delivery into exactly-once processing.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from erpsync.db import Conn, as_sync
from erpsync.policy import FIELDS, resolve

log = logging.getLogger("erpsync.inbox")


class BadSignature(Exception):
    pass


def verify(
    secret: str, body: bytes, header: str, tolerance: int = 300, now: float | None = None
) -> None:
    """HMAC-SHA256 over 'timestamp.body'; rejects stale timestamps to stop replays."""
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts, mac = int(parts["t"]), parts["v1"]
    except (KeyError, ValueError) as exc:
        raise BadSignature("malformed signature header") from exc
    if abs((now if now is not None else time.time()) - ts) > tolerance:
        raise BadSignature("timestamp outside tolerance")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, mac):
        raise BadSignature("signature mismatch")


def store(pool: ConnectionPool[Conn], event: dict[str, Any]) -> bool:
    """Returns False for a duplicate delivery."""
    contact = event["contact"]
    with pool.connection() as conn:
        cur = conn.execute(
            """insert into inbox (event_id, crm_id, crm_version, payload)
               values (%s, %s, %s, %s) on conflict (event_id) do nothing""",
            (event["event_id"], contact["id"], contact["version"], Jsonb(contact)),
        )
        return cur.rowcount == 1


@dataclass
class InboxProcessor:
    pool: ConnectionPool[Conn]

    def run_once(self) -> bool:
        """Apply one stored event. Returns False when nothing could be processed."""
        with self.pool.connection() as conn, as_sync(conn):
            event = conn.execute(
                """select * from inbox
                    where processed_at is null
                      and not exists (  -- a relay worker is mid-flight on this customer
                          select 1 from customers c join sync_state s on s.customer_id = c.id
                           where (c.crm_id = inbox.crm_id
                                  or c.id::text = inbox.payload ->> 'erp_id')
                             and s.lease_until > now())
                    order by received_at
                    limit 1
                      for update skip locked"""
            ).fetchone()
            if event is None:
                return False
            outcome = self._apply(conn, event["payload"])
            conn.execute(
                "update inbox set processed_at = now(), outcome = %s where event_id = %s",
                (outcome, event["event_id"]),
            )
            return True

    def _apply(self, conn: Conn, c: dict[str, Any]) -> str:
        customer = conn.execute(
            """select c.*, s.crm_snapshot from customers c
                 join sync_state s on s.customer_id = c.id
                where c.crm_id = %s
                   or (c.crm_id is null and c.id::text = %s)
                  for update of c, s""",
            (c["id"], c.get("erp_id") or ""),
        ).fetchone()

        if customer is None:
            # Created by a person in the CRM: bring it into the ERP.
            row = conn.execute(
                """insert into customers (crm_id, name, email, phone, credit_limit, status)
                   values (%(id)s, %(name)s, %(email)s, %(phone)s,
                           coalesce(%(credit_limit)s::numeric, 0), coalesce(%(status)s, 'active'))
                   returning id""",
                {f: c.get(f) for f in ("id", *FIELDS)},
            ).fetchone()
            assert row is not None
            conn.execute(
                "insert into sync_state (customer_id, crm_snapshot) values (%s, %s) "
                "on conflict (customer_id) do update set crm_snapshot = excluded.crm_snapshot",
                (row["id"], Jsonb(c)),
            )
            # The ERP fills its own defaults (credit limit, status) - fields the ERP owns, so
            # the CRM has to be told, or the two systems disagree from the very first write.
            created = conn.execute("select * from customers where id = %s", (row["id"],)).fetchone()
            assert created is not None
            plan = resolve({f: created[f] for f in FIELDS}, created["updated_at"], c, None, c)
            if plan.crm_changes:
                conn.execute(
                    "insert into outbox (customer_id, reason) values (%s, 'correction')",
                    (row["id"],),
                )
            return "created"

        snapshot = customer["crm_snapshot"]
        if snapshot and c["version"] <= snapshot.get("version", 0):
            return "stale"  # duplicate, out of order, or already seen via the relay

        if customer["crm_id"] is None:
            conn.execute(
                "update customers set crm_id = %s where id = %s", (c["id"], customer["id"])
            )

        plan = resolve(
            {f: customer[f] for f in FIELDS},
            customer["updated_at"],
            c,
            datetime.fromisoformat(c["updated_at"]),
            snapshot,
        )
        if plan.erp_changes:
            sets = ", ".join(f"{f} = %({f})s" for f in plan.erp_changes)
            conn.execute(
                f"update customers set {sets} where id = %(id)s",
                {**plan.erp_changes, "id": customer["id"]},
            )
        conn.execute(
            "update sync_state set crm_snapshot = %s where customer_id = %s",
            (Jsonb(c), customer["id"]),
        )
        if plan.crm_changes:
            # The CRM holds a value the policy overrules (e.g. someone edited credit_limit):
            # queue a correction. Written explicitly because the capture trigger is muted here.
            conn.execute(
                "insert into outbox (customer_id, reason) values (%s, 'correction')",
                (customer["id"],),
            )
            return "corrected"
        return "applied" if plan.erp_changes else "echo"


def event_json(raw: bytes) -> dict[str, Any]:
    data = json.loads(raw)
    if not isinstance(data, dict) or "event_id" not in data or "contact" not in data:
        raise ValueError("not a CRM contact event")
    contact = data["contact"]
    if not isinstance(contact, dict) or "id" not in contact or "version" not in contact:
        raise ValueError("contact must have id and version")
    return data
