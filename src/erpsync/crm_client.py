"""HTTP client for the CRM. Its only job beyond transport is to classify failures, because the
right reaction depends on the kind of failure, not on the fact that one happened."""

from __future__ import annotations

from typing import Any

import httpx


class CrmError(Exception):
    pass


class Retryable(CrmError):
    """Transient: timeouts, connection errors, 5xx, 429. Try again later."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class Conflict(Retryable):
    """The record changed between our read and our write (412). Re-read and retry soon."""


class Permanent(CrmError):
    """The CRM rejected the data itself (4xx). Retrying will not help: dead-letter it."""


class CrmClient:
    def __init__(self, client: httpx.Client) -> None:
        self.http = client

    def _send(self, method: str, url: str, **kw: Any) -> httpx.Response:
        try:
            resp = self.http.request(method, url, **kw)
        except httpx.TimeoutException as exc:
            raise Retryable(f"timeout: {exc}") from exc
        except httpx.TransportError as exc:
            raise Retryable(f"transport: {exc}") from exc
        if resp.status_code == 412:
            raise Conflict("version mismatch", retry_after=0)
        if resp.status_code == 429:
            raise Retryable("rate limited", _retry_after(resp))
        if resp.status_code >= 500:
            raise Retryable(f"{resp.status_code} from CRM", _retry_after(resp))
        if resp.status_code >= 400 and resp.status_code != 404:
            raise Permanent(f"{resp.status_code}: {resp.text[:300]}")
        return resp

    def create(self, idempotency_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._send(
            "POST", "/contacts", json=payload, headers={"Idempotency-Key": idempotency_key}
        )
        return dict(resp.json())

    def get(self, contact_id: str) -> dict[str, Any] | None:
        resp = self._send("GET", f"/contacts/{contact_id}")
        return None if resp.status_code == 404 else dict(resp.json())

    def patch(self, contact_id: str, version: int, changes: dict[str, Any]) -> dict[str, Any]:
        resp = self._send(
            "PATCH",
            f"/contacts/{contact_id}",
            json=changes,
            headers={"If-Match": str(version)},
        )
        if resp.status_code == 404:
            raise Permanent(f"contact {contact_id} no longer exists in the CRM")
        return dict(resp.json())

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._send("GET", "/contacts").json())


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None
