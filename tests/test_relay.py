from decimal import Decimal

from erpsync.crm_mock import Faults
from tests.conftest import Harness


def contacts(h: Harness) -> list[dict]:  # type: ignore[type-arg]
    return list(h.crm_state.contacts.values())


def test_create_links_both_sides(h: Harness) -> None:
    cid = h.erp_create(name="Acme", email="ops@acme.io", credit_limit=500)
    h.drain()
    [c] = contacts(h)
    assert c["erp_id"] == cid and c["credit_limit"] == "500.00"
    assert h.erp(cid)["crm_id"] == c["id"]
    assert h.sql("select status from outbox") == [{"status": "sent"}]


def test_timeout_after_commit_never_duplicates_the_create(h: Harness) -> None:
    h.flaky.rate = 1.0  # every write reaches the CRM, every response is lost
    cid = h.erp_create(name="Acme")
    for _ in range(3):
        h.sql("update outbox set available_at = now()")
        h.relay.run_once()
    h.flaky.rate = 0.0
    h.drain()
    assert len(contacts(h)) == 1
    assert h.erp(cid)["crm_id"] == contacts(h)[0]["id"]


def test_many_edits_coalesce_into_one_patch(h: Harness) -> None:
    cid = h.erp_create(name="Acme")
    h.drain()
    for limit in range(100, 1100, 100):
        h.erp_update(cid, credit_limit=limit)
    assert len(h.sql("select 1 from outbox where status = 'pending'")) == 10
    h.drain()
    [c] = contacts(h)
    assert c["credit_limit"] == "1000.00"
    assert c["version"] == 2  # one create + one patch, not ten


def test_transient_errors_retry_then_succeed(h: Harness) -> None:
    h.crm_state.faults = Faults(error_rate=0.6)
    h.crm_state.rng.seed(3)
    cid = h.erp_create(name="Acme")
    h.drain()
    assert h.erp(cid)["crm_id"] is not None
    [row] = h.sql("select status, attempts from outbox")
    assert row["status"] == "sent"


def test_rate_limit_honours_retry_after(h: Harness) -> None:
    h.crm_state.faults = Faults(rate_limit_rate=1.0)
    h.erp_create(name="Acme")
    h.relay.run_once()
    [row] = h.sql("select attempts, extract(epoch from available_at - now()) as wait from outbox")
    assert row["attempts"] == 1
    assert 0.5 < float(row["wait"]) <= 1.0


def test_rejected_data_is_dead_lettered_and_can_be_replayed(h: Harness) -> None:
    cid = h.erp_create(name="Broken", email="not-an-email")
    h.drain()
    [row] = h.sql("select status, last_error from outbox")
    assert row["status"] == "dead" and "invalid email" in row["last_error"]
    dead = h.service.get("/admin/dead-letters").json()
    assert dead[0]["customer_id"] == cid

    # Someone fixes the data in the ERP. The fix is queued but the customer stays blocked
    # until the dead letter is replayed - no silent skipping past a poison message.
    h.erp_update(cid, email="fixed@acme.io")
    assert h.service.post(f"/admin/dead-letters/{cid}/replay").status_code == 200
    h.drain()
    assert contacts(h)[0]["email"] == "fixed@acme.io"


def test_outages_retry_instead_of_dead_lettering(h: Harness) -> None:
    h.crm_state.faults = Faults(error_rate=1.0)
    h.erp_create(name="Acme")
    for _ in range(20):
        h.sql("update outbox set available_at = now()")
        h.relay.run_once()
    assert h.sql("select status, attempts from outbox") == [{"status": "pending", "attempts": 20}]


def test_gives_up_once_the_retry_window_has_passed(h: Harness) -> None:
    h.crm_state.faults = Faults(error_rate=1.0)
    h.erp_create(name="Acme")
    h.sql("update outbox set created_at = now() - interval '25 hours'")
    h.relay.run_once()
    assert h.sql("select status from outbox") == [{"status": "dead"}]


def test_erp_edit_of_crm_owned_field_is_reverted(h: Harness) -> None:
    cid = h.erp_create(name="Acme", email="sales@acme.io")
    h.drain()
    h.erp_update(cid, email="typo@acme.io", credit_limit=Decimal("10"))
    h.drain()
    assert h.erp(cid)["email"] == "sales@acme.io"
    assert contacts(h)[0]["credit_limit"] == "10.00"


def test_backoff_is_full_jitter_and_capped(h: Harness) -> None:
    from erpsync.relay import Relay

    r = Relay(h.pool, h.crm, base_delay=1, max_delay=30)
    samples = [r._backoff(10) for _ in range(500)]
    assert max(samples) <= 30 and min(samples) >= 0
    assert max(samples) > 20 and min(samples) < 5


def test_edits_made_while_a_lost_create_is_retried_are_not_lost(h: Harness) -> None:
    # Found by running the stack: the create commits in the CRM but the response is lost;
    # the ERP edits the customer; the retry's idempotent replay returns the *original*
    # contact. The edits must still reach the CRM.
    h.flaky.rate = 1.0
    cid = h.erp_create(name="Acme", credit_limit=100)
    h.relay.run_once()
    h.flaky.rate = 0.0
    h.erp_update(cid, credit_limit=8100, phone="555-2343", name="Acme Ltd")
    h.drain()
    [c] = contacts(h)
    assert (c["credit_limit"], c["phone"], c["name"]) == ("8100.00", "555-2343", "Acme Ltd")


def test_crm_rename_survives_an_erp_edit_racing_the_relay(h: Harness) -> None:
    cid = h.erp_create(name="Acme", credit_limit=100)
    h.drain()
    crm_id = h.erp(cid)["crm_id"]
    h.crm_edit(crm_id, name="Acme Holdings")  # webhook not delivered yet
    h.crm_state.drain_events()
    h.erp_update(cid, credit_limit=200)  # queues a relay pass

    real_get = h.crm.get

    def get_then_erp_edits(contact_id: str) -> dict | None:  # type: ignore[type-arg]
        contact = real_get(contact_id)
        h.erp_update(cid, status="on_hold")  # lands between the relay's read and its commit
        h.crm.get = real_get  # type: ignore[method-assign]
        return contact

    h.crm.get = get_then_erp_edits  # type: ignore[method-assign]
    h.drain()
    assert h.erp(cid)["name"] == "Acme Holdings"
    c = h.crm_state.contacts[crm_id]
    assert (c["name"], c["status"], c["credit_limit"]) == ("Acme Holdings", "on_hold", "200.00")
