import json
import time

import pytest

from erpsync.crm_mock import sign
from erpsync.inbox import BadSignature, verify
from tests.conftest import SECRET, Harness


def test_signature_checks() -> None:
    body = b'{"x": 1}'
    verify(SECRET, body, sign(SECRET, body))
    with pytest.raises(BadSignature):
        verify(SECRET, body + b" ", sign(SECRET, body))
    with pytest.raises(BadSignature):
        verify(SECRET, body, sign(SECRET, body, timestamp=int(time.time()) - 3600))
    with pytest.raises(BadSignature):
        verify(SECRET, body, "garbage")


def test_webhook_endpoint_rejects_unsigned(h: Harness) -> None:
    r = h.service.post("/webhooks/crm", content=b"{}", headers={"X-CRM-Signature": "t=1,v1=00"})
    assert r.status_code == 401


def linked(h: Harness, **values: object) -> tuple[str, str]:
    cid = h.erp_create(**{"name": "Acme", **values})
    h.drain()
    crm_id = h.erp(cid)["crm_id"]
    return cid, crm_id


def test_crm_edit_flows_to_erp(h: Harness) -> None:
    cid, crm_id = linked(h)
    h.crm_edit(crm_id, email="new@acme.io", phone="555-0100")
    h.drain()
    erp = h.erp(cid)
    assert (erp["email"], erp["phone"]) == ("new@acme.io", "555-0100")
    # Applying a CRM change must not even queue a round trip back to the CRM:
    # the only outbox row is the original create.
    assert h.sql("select reason from outbox") == [{"reason": "insert"}]
    assert h.crm_state.contacts[crm_id]["version"] == 2


def test_echoes_of_our_own_writes_are_ignored(h: Harness) -> None:
    cid, crm_id = linked(h)
    h.erp_update(cid, credit_limit=900)
    h.drain()
    outcomes = {r["outcome"] for r in h.sql("select outcome from inbox")}
    assert outcomes == {"stale"}  # our create + our patch, both already known
    assert h.crm_state.contacts[crm_id]["version"] == 2


def test_duplicate_and_out_of_order_deliveries(h: Harness) -> None:
    cid, crm_id = linked(h)
    h.crm_edit(crm_id, name="Acme v2")
    h.crm_edit(crm_id, name="Acme v3")
    events = h.crm_state.drain_events()
    for e in [events[1], events[0], events[1], events[0]]:  # newest first, each twice
        body = json.dumps(e).encode()
        h.service.post(
            "/webhooks/crm", content=body, headers={"X-CRM-Signature": sign(SECRET, body)}
        )
    assert len(h.sql("select 1 from inbox where crm_id = %s", crm_id)) == 3  # 2 new + create echo
    h.drain()
    assert h.erp(cid)["name"] == "Acme v3"
    assert {r["outcome"] for r in h.sql("select outcome from inbox where crm_version = 3")} == {
        "applied"
    }
    assert {r["outcome"] for r in h.sql("select outcome from inbox where crm_version = 2")} == {
        "stale"
    }


def test_crm_edit_of_erp_owned_field_is_pushed_back(h: Harness) -> None:
    cid, crm_id = linked(h, credit_limit=500)
    h.crm_edit(crm_id, credit_limit="99999")
    h.drain()
    assert h.erp(cid)["credit_limit"] == 500
    assert h.crm_state.contacts[crm_id]["credit_limit"] == "500.00"
    assert "corrected" in {r["outcome"] for r in h.sql("select outcome from inbox")}


def test_contact_created_in_crm_appears_in_erp(h: Harness) -> None:
    c = h.crm_http.post(
        "/_ui/contacts", json={"name": "Walk-in Ltd", "email": "hi@walkin.io"}
    ).json()
    h.drain()
    [row] = h.sql("select * from customers where crm_id = %s", c["id"])
    assert row["name"] == "Walk-in Ltd" and row["email"] == "hi@walkin.io"
    # Not created a second time in the CRM; the ERP's defaults for the fields it owns are
    # pushed to the existing contact instead.
    assert len(h.crm_state.contacts) == 1
    assert h.crm_state.contacts[c["id"]]["credit_limit"] == "0.00"
    assert h.crm_state.contacts[c["id"]]["status"] == "active"


def test_lost_webhooks_are_recovered_by_the_backstop(h: Harness) -> None:
    from erpsync.backstop import run_backstop

    cid, crm_id = linked(h)
    h.crm_edit(crm_id, phone="555-0199")
    h.crm_state.drain_events()  # the webhook is lost
    h.drain()
    assert h.erp(cid)["phone"] is None
    report = run_backstop(h.pool, h.crm)
    assert report.missed_crm_changes == 1
    h.drain()
    assert h.erp(cid)["phone"] == "555-0199"
