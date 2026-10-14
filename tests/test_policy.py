from datetime import UTC, datetime, timedelta
from decimal import Decimal

from erpsync.policy import resolve

T = datetime(2026, 10, 1, tzinfo=UTC)
BASE = {
    "name": "Acme",
    "email": "a@acme.io",
    "phone": "1",
    "credit_limit": "100.00",
    "status": "active",
}


def test_not_in_crm_yet_sends_everything() -> None:
    plan = resolve(BASE, T, None, None, None)
    assert plan.crm_changes == {**BASE, "credit_limit": Decimal("100.00")}


def test_owned_fields_follow_their_owner() -> None:
    erp = {**BASE, "email": "erp-edit@acme.io", "credit_limit": "500"}
    crm = {**BASE, "credit_limit": "999"}
    plan = resolve(erp, T, crm, T, BASE)
    assert plan.erp_changes == {"email": "a@acme.io"}  # CRM owns email: ERP edit reverted
    assert plan.crm_changes == {"credit_limit": Decimal("500.00")}  # ERP owns credit


def test_shared_field_changed_on_one_side_wins_even_if_other_row_is_newer() -> None:
    # Renamed in the CRM; later the ERP edited only the credit limit. Row-level
    # "last writer wins" would undo the rename. The three-way merge keeps it.
    crm = {**BASE, "name": "Acme Corp"}
    erp = {**BASE, "credit_limit": "250"}
    plan = resolve(erp, T + timedelta(hours=1), crm, T, BASE)
    assert plan.target["name"] == "Acme Corp"
    assert plan.erp_changes == {"name": "Acme Corp"}
    assert plan.conflicts == ()


def test_true_conflict_goes_to_the_later_writer() -> None:
    erp = {**BASE, "name": "Acme ERP"}
    crm = {**BASE, "name": "Acme CRM"}
    assert resolve(erp, T, crm, T + timedelta(minutes=1), BASE).target["name"] == "Acme CRM"
    later_erp = resolve(erp, T + timedelta(minutes=2), crm, T + timedelta(minutes=1), BASE)
    assert later_erp.target["name"] == "Acme ERP"
    assert later_erp.conflicts == ("name",)


def test_equal_states_are_converged_whatever_the_formatting() -> None:
    crm = {**BASE, "credit_limit": "100", "phone": "1"}
    erp = {**BASE, "credit_limit": Decimal("100.00"), "email": "a@acme.io"}
    assert resolve(erp, T, crm, T, crm).converged
