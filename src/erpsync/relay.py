"""Outbound relay: ERP -> CRM.

A worker claims one *customer* (not one event) with a lease, so several workers can run without
two of them ever writing the same CRM record at once. It sends the customer's *current* state,
which makes the events idempotent and order-insensitive: ten edits in a row become one PATCH.

The lease is taken in a short transaction and the CRM call happens outside any transaction -
holding a database transaction open across a network call is how lock queues and connection
exhaustion start. The final write checks the lease is still ours (fencing) before committing.
"""

from __future__ import annotations

import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from erpsync.crm_client import CrmClient, CrmError, Permanent, Retryable
from erpsync.db import Conn, as_sync
from erpsync.policy import FIELDS, fields_of, resolve, to_json

log = logging.getLogger("erpsync.relay")

CLAIM = """
update sync_state s
   set lease_owner = %(worker)s,
       lease_until = now() + make_interval(secs => %(lease)s)
 where s.customer_id = (
        select o.customer_id
          from outbox o
          join sync_state ss on ss.customer_id = o.customer_id
         where o.status = 'pending'
           and o.available_at <= now()
           and (ss.lease_until is null or ss.lease_until < now())
         order by o.id
         limit 1
           for update of ss skip locked)
returning s.customer_id,
          (select max(id) from outbox
            where customer_id = s.customer_id and status = 'pending') as high_water
"""

LOAD = """
select c.*, s.create_key, s.crm_snapshot
  from customers c join sync_state s on s.customer_id = c.id
 where c.id = %s
"""


class LeaseLost(Exception):
    pass


class ErpMoved(Exception):
    """The ERP row changed between our read and our commit. Nothing is committed - not the
    snapshot, not the 'sent' marks - and the customer is re-read straight away. Committing the
    snapshot anyway would make a CRM change look like an ERP change on the next pass."""


@dataclass
class Relay:
    pool: ConnectionPool[Conn]
    crm: CrmClient
    worker: str = field(default_factory=lambda: f"relay-{uuid.uuid4().hex[:8]}")
    lease_seconds: float = 30
    # Transient failures (timeouts, 5xx, 429) mean the CRM is having a bad time, not that the
    # data is bad: keep retrying for this long before giving up. Rejected data (4xx) is
    # dead-lettered immediately.
    give_up_after: float = 24 * 3600
    base_delay: float = 0.5
    max_delay: float = 300
    rng: random.Random = field(default_factory=random.Random)

    def run_once(self) -> bool:
        """Sync one customer. Returns False when there was nothing to do."""
        with self.pool.connection() as conn:
            claimed = conn.execute(
                CLAIM, {"worker": self.worker, "lease": self.lease_seconds}
            ).fetchone()
        if claimed is None:
            return False
        customer_id, high_water = claimed["customer_id"], claimed["high_water"]
        try:
            self._sync(customer_id, high_water)
        except Permanent as exc:
            log.warning("dead-lettering %s: %s", customer_id, exc)
            self._fail(customer_id, high_water, str(exc), dead=True)
        except Retryable as exc:
            self._fail(customer_id, high_water, str(exc), retry_after=exc.retry_after)
        except ErpMoved:
            self._release(customer_id)
        except LeaseLost:
            log.warning(
                "lease on %s expired mid-flight; another worker will finish it", customer_id
            )
        except CrmError as exc:  # pragma: no cover - every CrmError is one of the above
            self._fail(customer_id, high_water, str(exc))
        return True

    # ------------------------------------------------------------------------------------

    def _sync(self, customer_id: Any, high_water: int) -> None:
        with self.pool.connection() as conn:
            row = conn.execute(LOAD, (customer_id,)).fetchone()
        if row is None:
            return
        erp = {f: row[f] for f in FIELDS}

        if row["crm_id"] is None:
            # One idempotency key per customer, not per attempt: a create that timed out after
            # the CRM committed it returns the same record on retry instead of a duplicate.
            payload = {"erp_id": str(customer_id), **to_json(erp)}
            contact = self.crm.create(str(row["create_key"]), payload)
            # An idempotent replay returns the record as the *first* attempt created it. If the
            # ERP changed in between, those edits are not in it: send the difference now.
            # Until the record exists in the CRM, the ERP is the source for every field.
            behind = {f: v for f, v in fields_of(erp).items() if v != fields_of(contact)[f]}
            if behind:
                contact = self.crm.patch(contact["id"], contact["version"], to_json(behind))
            self._commit(
                customer_id, high_water, contact, erp_changes={}, read_at=row["updated_at"]
            )
            return

        current = self.crm.get(row["crm_id"])
        if current is None:
            raise Permanent(f"contact {row['crm_id']} was deleted in the CRM")
        plan = resolve(
            erp,
            row["updated_at"],
            current,
            _ts(current.get("updated_at")),
            row["crm_snapshot"],
        )
        contact = current
        if plan.crm_changes:
            # If-Match: if someone edits the contact between our GET and PATCH we get a 412
            # and retry from a fresh read, instead of overwriting their edit.
            contact = self.crm.patch(row["crm_id"], current["version"], to_json(plan.crm_changes))
        self._commit(customer_id, high_water, contact, plan.erp_changes, read_at=row["updated_at"])

    def _commit(
        self,
        customer_id: Any,
        high_water: int,
        contact: dict[str, Any],
        erp_changes: dict[str, Any],
        read_at: Any,
    ) -> None:
        with self.pool.connection() as conn, as_sync(conn):
            lease = conn.execute(
                "select lease_owner from sync_state where customer_id = %s for update",
                (customer_id,),
            ).fetchone()
            if lease is None or lease["lease_owner"] != self.worker:
                raise LeaseLost(str(customer_id))
            conn.execute(
                "update customers set crm_id = %s where id = %s and crm_id is null",
                (contact["id"], customer_id),
            )
            if erp_changes:
                # Only if the ERP row has not changed since we read it; if it has, abort the
                # whole commit and re-read (see ErpMoved).
                sets = ", ".join(f"{f} = %({f})s" for f in erp_changes)
                guarded = f"update customers set {sets} where id = %(id)s and updated_at = %(at)s"
                cur = conn.execute(guarded, {**erp_changes, "id": customer_id, "at": read_at})
                if cur.rowcount == 0:
                    raise ErpMoved(str(customer_id))
            conn.execute(
                """update sync_state
                      set crm_snapshot = %s, lease_owner = null, lease_until = null
                    where customer_id = %s""",
                (Jsonb(contact), customer_id),
            )
            conn.execute(
                """update outbox set status = 'sent', done_at = now()
                    where customer_id = %s and status = 'pending' and id <= %s""",
                (customer_id, high_water),
            )

    def _fail(
        self,
        customer_id: Any,
        high_water: int,
        error: str,
        retry_after: float | None = None,
        dead: bool = False,
    ) -> None:
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """select coalesce(max(attempts), 0) as n,
                          coalesce(extract(epoch from now() - min(created_at)), 0) as age
                     from outbox
                    where customer_id = %s and status = 'pending' and id <= %s""",
                (customer_id, high_water),
            ).fetchone()
            assert row is not None
            n = row["n"] + 1
            give_up = dead or float(row["age"]) >= self.give_up_after
            delay = retry_after if retry_after is not None else self._backoff(n)
            conn.execute(
                """update outbox
                      set attempts = %(n)s,
                          last_error = %(error)s,
                          status = case when %(dead)s then 'dead' else status end,
                          done_at = case when %(dead)s then now() else done_at end,
                          available_at = now() + make_interval(secs => %(delay)s)
                    where customer_id = %(id)s and status = 'pending' and id <= %(hw)s""",
                {"n": n, "error": error[:1000], "dead": give_up, "delay": delay,
                 "id": customer_id, "hw": high_water},
            )  # fmt: skip
            conn.execute(
                """update sync_state set lease_owner = null, lease_until = null
                    where customer_id = %s and lease_owner = %s""",
                (customer_id, self.worker),
            )

    def _release(self, customer_id: Any) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """update sync_state set lease_owner = null, lease_until = null
                    where customer_id = %s and lease_owner = %s""",
                (customer_id, self.worker),
            )

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter: spreads retries out so a CRM outage does not
        end with every worker hammering it in lock-step the moment it comes back."""
        return self.rng.uniform(0, min(self.max_delay, self.base_delay * 2**attempt))


def _ts(value: Any) -> Any:
    return datetime.fromisoformat(value) if isinstance(value, str) else value
