"""A deliberately unreliable CRM, so the sync can be tested against the failures real ones have.

- random 500s and 429s with Retry-After;
- "commit then fail": the change is saved but the client gets a 504 - the classic cause of
  duplicate records when a create is retried without an idempotency key;
- optimistic concurrency via If-Match on the record version;
- signed webhooks for *every* change, including the ones the sync itself makes (echoes).
"""

from __future__ import annotations

import hashlib
import hmac
import itertools
import json
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

EDITABLE = ("name", "email", "phone", "credit_limit", "status")


@dataclass
class Faults:
    error_rate: float = 0.0  # 500 before doing anything
    rate_limit_rate: float = 0.0  # 429 with Retry-After
    commit_then_fail_rate: float = 0.0  # change saved, 504 returned
    seed: int = 0


@dataclass
class CrmState:
    contacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    idempotency: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    faults: Faults = field(default_factory=Faults)
    rng: random.Random = field(default_factory=random.Random)
    requests: int = 0
    version_conflicts: int = 0  # 412s: two writers raced on one contact

    def now(self) -> str:
        return datetime.now(UTC).isoformat()

    def emit(self, kind: str, contact: dict[str, Any]) -> None:
        self.events.append({"event_id": str(uuid.uuid4()), "type": kind, "contact": dict(contact)})

    def drain_events(self) -> list[dict[str, Any]]:
        with self.lock:
            out, self.events = self.events, []
        return out


class ContactIn(BaseModel):
    erp_id: str | None = None
    name: str
    email: str | None = None
    phone: str | None = None
    credit_limit: str | None = None
    status: str | None = None


class ContactPatch(BaseModel):
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    credit_limit: str | None = None
    status: str | None = None


def sign(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = timestamp if timestamp is not None else int(time.time())
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def _validate(values: dict[str, Any]) -> None:
    email = values.get("email")
    if email and "@" not in email:
        raise HTTPException(422, {"field": "email", "error": "invalid email address"})


def create_crm_app(state: CrmState | None = None) -> FastAPI:
    st = state or CrmState()
    app = FastAPI(title="mock-crm")
    app.state.crm = st

    def chaos_before() -> Response | None:
        st.requests += 1
        roll = st.rng.random()
        if roll < st.faults.error_rate:
            return JSONResponse({"error": "internal"}, status_code=500)
        if roll < st.faults.error_rate + st.faults.rate_limit_rate:
            return JSONResponse({"error": "slow down"}, 429, headers={"Retry-After": "1"})
        return None

    def chaos_after(body: dict[str, Any], status: int) -> Response:
        if st.rng.random() < st.faults.commit_then_fail_rate:
            return JSONResponse({"error": "gateway timeout"}, status_code=504)
        return JSONResponse(body, status_code=status)

    @app.post("/contacts")
    def create(body: ContactIn, idempotency_key: Annotated[str, Header()]) -> Response:
        if (fail := chaos_before()) is not None:
            return fail
        values = body.model_dump()
        _validate(values)
        with st.lock:
            if idempotency_key in st.idempotency:
                return JSONResponse(st.idempotency[idempotency_key], status_code=200)
            contact = {
                **values,
                "id": f"crm_{next(st.ids):06d}",
                "version": 1,
                "updated_at": st.now(),
            }
            st.contacts[contact["id"]] = contact
            st.idempotency[idempotency_key] = dict(contact)
            st.emit("contact.created", contact)
            snapshot = dict(contact)
        return chaos_after(snapshot, 201)

    @app.get("/contacts/{contact_id}")
    def get(contact_id: str) -> Response:
        if (fail := chaos_before()) is not None:
            return fail
        with st.lock:
            contact = st.contacts.get(contact_id)
            if contact is None:
                raise HTTPException(404, "not found")
            return JSONResponse(dict(contact))

    @app.get("/contacts")
    def list_all() -> list[dict[str, Any]]:
        with st.lock:
            return [dict(c) for c in st.contacts.values()]

    @app.patch("/contacts/{contact_id}")
    def patch(
        contact_id: str,
        body: ContactPatch,
        if_match: Annotated[str | None, Header()] = None,
    ) -> Response:
        if (fail := chaos_before()) is not None:
            return fail
        values = body.model_dump(exclude_unset=True)
        _validate(values)
        with st.lock:
            contact = st.contacts.get(contact_id)
            if contact is None:
                raise HTTPException(404, "not found")
            if if_match is not None and if_match != str(contact["version"]):
                st.version_conflicts += 1
                return JSONResponse(
                    {"error": "version mismatch", "current": contact["version"]}, status_code=412
                )
            contact.update(values)
            contact["version"] += 1
            contact["updated_at"] = st.now()
            st.emit("contact.updated", contact)
            snapshot = dict(contact)
        return chaos_after(snapshot, 200)

    # ---- simulated CRM users (no faults: these are people clicking in the CRM UI) ----

    @app.post("/_ui/contacts")
    def ui_create(body: ContactIn) -> dict[str, Any]:
        with st.lock:
            contact = {
                **body.model_dump(),
                "erp_id": None,
                "id": f"crm_{next(st.ids):06d}",
                "version": 1,
                "updated_at": st.now(),
            }
            st.contacts[contact["id"]] = contact
            st.emit("contact.created", contact)
            return dict(contact)

    @app.post("/_ui/contacts/{contact_id}")
    def ui_edit(contact_id: str, body: ContactPatch) -> dict[str, Any]:
        with st.lock:
            contact = st.contacts[contact_id]
            contact.update(body.model_dump(exclude_unset=True))
            contact["version"] += 1
            contact["updated_at"] = st.now()
            st.emit("contact.updated", contact)
            return dict(contact)

    @app.post("/_faults")
    async def set_faults(request: Request) -> dict[str, Any]:
        data = await request.json()
        st.faults = Faults(**data)
        st.rng.seed(st.faults.seed)
        return dict(data)

    return app


def deliver_forever(state: CrmState, url: str, secret: str, interval: float = 0.5) -> None:
    """At-least-once webhook delivery for the docker-compose demo (tests deliver manually)."""
    pending: list[dict[str, Any]] = []
    with httpx.Client(timeout=5) as client:
        while True:
            pending += state.drain_events()
            still: list[dict[str, Any]] = []
            for event in pending:
                body = json.dumps(event).encode()
                try:
                    r = client.post(
                        url,
                        content=body,
                        headers={
                            "X-CRM-Signature": sign(secret, body),
                            "Content-Type": "application/json",
                        },
                    )
                    if r.status_code >= 300:
                        still.append(event)
                except httpx.HTTPError:
                    still.append(event)
            pending = still
            time.sleep(interval)
