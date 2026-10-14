"""The sync service: webhook receiver, admin/metrics endpoints, and a few ERP endpoints that stand
in for the ERP's own application (they write plain SQL; the trigger does the rest)."""

import json
import logging
import threading
import time
from decimal import Decimal
from typing import Annotated, Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import PlainTextResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from erpsync import db
from erpsync.crm_client import CrmClient
from erpsync.inbox import BadSignature, InboxProcessor, event_json, store, verify
from erpsync.relay import Relay

log = logging.getLogger("erpsync")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ERPSYNC_")

    database_url: str = "postgresql://erpsync:erpsync@localhost:5432/erpsync"
    crm_url: str = "http://localhost:8001"
    webhook_secret: str = "change-me"
    max_webhook_bytes: int = 256 * 1024


class CustomerIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    email: str | None = Field(default=None, max_length=200)
    phone: str | None = Field(default=None, max_length=50)
    credit_limit: Decimal = Decimal(0)
    status: str = Field(default="active", pattern="^(active|on_hold|closed)$")


class CustomerPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    email: str | None = Field(default=None, max_length=200)
    phone: str | None = Field(default=None, max_length=50)
    credit_limit: Decimal | None = None
    status: str | None = Field(default=None, pattern="^(active|on_hold|closed)$")


def create_app(
    settings: Settings | None = None, pool: ConnectionPool[db.Conn] | None = None
) -> FastAPI:
    settings = settings or Settings()
    # No migration here: DDL takes an ACCESS EXCLUSIVE lock on the ERP table, and running it
    # from every booting process deadlocks against live writes. Run `erpsync migrate` once.
    pool = pool or db.make_pool(settings.database_url)
    app = FastAPI(title="erp-sync")

    @app.get("/health")
    def health() -> dict[str, str]:
        with pool.connection() as conn:
            conn.execute("select 1")
        return {"status": "ok"}

    @app.post("/webhooks/crm", status_code=status.HTTP_202_ACCEPTED)
    async def webhook(
        request: Request, x_crm_signature: Annotated[str, Header()]
    ) -> dict[str, Any]:
        body = await request.body()
        if len(body) > settings.max_webhook_bytes:
            raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "payload too large")
        try:
            verify(settings.webhook_secret, body, x_crm_signature)
            event = event_json(body)
        except BadSignature as exc:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc
        except (ValueError, json.JSONDecodeError) as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        stored = store(pool, event)
        return {"stored": stored}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        with pool.connection() as conn:
            outbox = {
                r["status"]: r["n"]
                for r in conn.execute("select status, count(*) as n from outbox group by status")
            }
            lag = conn.execute(
                """select coalesce(extract(epoch from now() - min(created_at)), 0) as s
                     from outbox where status = 'pending'"""
            ).fetchone()
            inbox = conn.execute(
                """select count(*) filter (where processed_at is null) as pending,
                          count(*) as total
                     from inbox"""
            ).fetchone()
        assert lag is not None and inbox is not None
        lines = [
            "# HELP erpsync_outbox_events Outbox events by status.",
            "# TYPE erpsync_outbox_events gauge",
            *(
                f'erpsync_outbox_events{{status="{s}"}} {outbox.get(s, 0)}'
                for s in ("pending", "sent", "dead")
            ),
            "# HELP erpsync_outbox_oldest_pending_seconds Age of the oldest unsent change.",
            "# TYPE erpsync_outbox_oldest_pending_seconds gauge",
            f"erpsync_outbox_oldest_pending_seconds {float(lag['s']):.3f}",
            "# HELP erpsync_inbox_pending Webhooks stored but not yet applied.",
            "# TYPE erpsync_inbox_pending gauge",
            f"erpsync_inbox_pending {inbox['pending']}",
            f"erpsync_inbox_total {inbox['total']}",
        ]
        return "\n".join(lines) + "\n"

    @app.get("/admin/dead-letters")
    def dead_letters() -> list[dict[str, Any]]:
        with pool.connection() as conn:
            return list(
                conn.execute(
                    """select o.customer_id::text, c.name, count(*) as events,
                              max(o.attempts) as attempts, max(o.last_error) as last_error,
                              max(o.done_at) as dead_since
                         from outbox o join customers c on c.id = o.customer_id
                        where o.status = 'dead'
                        group by o.customer_id, c.name order by dead_since desc"""
                )
            )

    @app.post("/admin/dead-letters/{customer_id}/replay")
    def replay(customer_id: str) -> dict[str, int]:
        with pool.connection() as conn:
            cur = conn.execute(
                """update outbox set status = 'pending', attempts = 0, available_at = now(),
                          done_at = null
                    where customer_id = %s::uuid and status = 'dead'""",
                (customer_id,),
            )
        if cur.rowcount == 0:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "no dead letters for that customer")
        return {"replayed": cur.rowcount}

    # ---- stand-ins for the ERP application -------------------------------------------------

    @app.get("/erp/customers")
    def customers() -> list[dict[str, Any]]:
        with pool.connection() as conn:
            return list(conn.execute("select * from customers order by name limit 500"))

    @app.post("/erp/customers", status_code=status.HTTP_201_CREATED)
    def create_customer(body: CustomerIn) -> dict[str, Any]:
        with pool.connection() as conn:
            row = conn.execute(
                """insert into customers (name, email, phone, credit_limit, status)
                   values (%(name)s, %(email)s, %(phone)s, %(credit_limit)s, %(status)s)
                   returning *""",
                body.model_dump(),
            ).fetchone()
        assert row is not None
        return dict(row)

    @app.patch("/erp/customers/{customer_id}")
    def update_customer(customer_id: str, body: CustomerPatch) -> Response:
        changes = body.model_dump(exclude_unset=True)
        if not changes:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "nothing to update")
        sets = ", ".join(f"{f} = %({f})s" for f in changes)
        with pool.connection() as conn:
            cur = conn.execute(
                f"update customers set {sets} where id = %(id)s::uuid",
                {**changes, "id": customer_id},
            )
        if cur.rowcount == 0:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "customer not found")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


def run_workers(
    settings: Settings,
    relays: int = 2,
    idle_sleep: float = 0.5,
    stop: threading.Event | None = None,
) -> None:
    """Relay and inbox workers in threads. Run as many processes of this as you like:
    leases and SKIP LOCKED keep them from stepping on each other."""
    pool = db.make_pool(settings.database_url, size=relays + 2)
    stop = stop or threading.Event()

    def loop(step: Any) -> None:
        while not stop.is_set():
            try:
                if not step():
                    time.sleep(idle_sleep)
            except Exception:
                log.exception("worker step failed")
                time.sleep(idle_sleep)

    threads = []
    for _ in range(relays):
        client = CrmClient(httpx.Client(base_url=settings.crm_url, timeout=10))
        threads.append(
            threading.Thread(target=loop, args=(Relay(pool, client).run_once,), daemon=True)
        )
    threads.append(
        threading.Thread(target=loop, args=(InboxProcessor(pool).run_once,), daemon=True)
    )
    for t in threads:
        t.start()
    stop.wait()
