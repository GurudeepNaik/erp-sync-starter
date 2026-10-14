"""Field ownership and conflict resolution.

Both directions use the same function. Given the ERP row, the current CRM record and the CRM
record as it was when the two systems last agreed (the *base*), `resolve` returns the state both
should converge to, field by field:

    erp   - the ERP owns the field; a CRM edit is pushed back over
    crm   - the CRM owns the field; an ERP edit is reverted to the CRM value
    shared- three-way merge: whichever side changed it since the base wins; if *both* changed
            it, the side whose record was updated last wins

A naive "last writer wins" on row timestamps loses data: rename a contact in the CRM, then edit
the credit limit in the ERP, and the ERP row looks newer - the rename would be overwritten.
Comparing against the base tells us which side actually changed each field.

Because the target is computed from state, not from the event that triggered it, duplicate,
out-of-order and echoed events all resolve to the same answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

Owner = Literal["erp", "crm", "shared"]

POLICY: dict[str, Owner] = {
    "name": "shared",
    "email": "crm",  # sales keep contact details current in the CRM
    "phone": "crm",
    "credit_limit": "erp",  # finance owns credit and account status
    "status": "erp",
}
FIELDS = tuple(POLICY)


def canonical(field: str, value: Any) -> Any:
    """Compare money as Decimal and blanks as None, whatever the transport did to them."""
    if value is None or value == "":
        return None
    if field == "credit_limit":
        return Decimal(str(value)).quantize(Decimal("0.01"))
    return value


def fields_of(row: dict[str, Any] | None) -> dict[str, Any]:
    row = row or {}
    return {f: canonical(f, row.get(f)) for f in FIELDS}


@dataclass(frozen=True, slots=True)
class Plan:
    target: dict[str, Any]
    erp_changes: dict[str, Any]  # apply to the ERP row (as the sync)
    crm_changes: dict[str, Any]  # send to the CRM
    conflicts: tuple[str, ...]  # shared fields changed on both sides

    @property
    def converged(self) -> bool:
        return not self.erp_changes and not self.crm_changes


def resolve(
    erp: dict[str, Any],
    erp_updated_at: datetime,
    crm: dict[str, Any] | None,
    crm_updated_at: datetime | None,
    base: dict[str, Any] | None,
    policy: dict[str, Owner] = POLICY,
) -> Plan:
    e = fields_of(erp)
    if crm is None:
        # Not in the CRM yet: the ERP record is the starting point for every field.
        return Plan(target=e, erp_changes={}, crm_changes=e, conflicts=())
    c, b = fields_of(crm), fields_of(base)
    target: dict[str, Any] = {}
    conflicts: list[str] = []
    for field, owner in policy.items():
        if owner == "erp":
            target[field] = e[field]
        elif owner == "crm":
            target[field] = c[field]
        elif e[field] == c[field] or c[field] == b[field]:
            target[field] = e[field]  # agree, or only the ERP changed it
        elif e[field] == b[field]:
            target[field] = c[field]  # only the CRM changed it
        else:
            conflicts.append(field)
            crm_newer = crm_updated_at is not None and crm_updated_at > erp_updated_at
            target[field] = c[field] if crm_newer else e[field]
    return Plan(
        target=target,
        erp_changes={f: v for f, v in target.items() if v != e[f]},
        crm_changes={f: v for f, v in target.items() if v != c[f]},
        conflicts=tuple(conflicts),
    )


def to_json(values: dict[str, Any]) -> dict[str, Any]:
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in values.items()}
