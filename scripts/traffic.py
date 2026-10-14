"""Generate edits on both sides of a running stack (docker compose up), with CRM faults on.

    uv run python scripts/traffic.py --customers 50 --edits 300
then
    docker compose run --rm worker check
"""

from __future__ import annotations

import argparse
import random

import httpx

ap = argparse.ArgumentParser()
ap.add_argument("--erp", default="http://localhost:8000")
ap.add_argument("--crm", default="http://localhost:8001")
ap.add_argument("--customers", type=int, default=50)
ap.add_argument("--edits", type=int, default=300)
ap.add_argument("--seed", type=int, default=1)
args = ap.parse_args()
rng = random.Random(args.seed)
erp, crm = httpx.Client(base_url=args.erp), httpx.Client(base_url=args.crm)

crm.post("/_faults", json={"error_rate": 0.15, "rate_limit_rate": 0.05,
                            "commit_then_fail_rate": 0.1, "seed": args.seed})  # fmt: skip
ids = [
    erp.post("/erp/customers", json={"name": f"Customer {i}", "email": f"c{i}@example.com",
                                     "credit_limit": rng.randint(1, 50) * 100}).json()["id"]
    for i in range(args.customers)
]  # fmt: skip
ERP_EDITS = [
    lambda: ("name", f"Customer {rng.randint(1, 999)}"),
    lambda: ("credit_limit", rng.randint(1, 99) * 100),
    lambda: ("status", rng.choice(["active", "on_hold"])),
    lambda: ("phone", f"555-{rng.randint(1000, 9999)}"),
]
CRM_EDITS = [
    lambda: ("email", f"crm{rng.randint(1, 999)}@example.com"),
    lambda: ("phone", f"555-{rng.randint(1000, 9999)}"),
    lambda: ("name", f"Renamed {rng.randint(1, 999)}"),
    lambda: ("credit_limit", "1"),  # the ERP owns this: the sync will push it back
]
for _ in range(args.edits):
    if rng.random() < 0.5:
        field, value = rng.choice(ERP_EDITS)()
        erp.patch(f"/erp/customers/{rng.choice(ids)}", json={field: value})
    elif linked := [c["id"] for c in crm.get("/contacts").json()]:
        field, value = rng.choice(CRM_EDITS)()
        crm.post(f"/_ui/contacts/{rng.choice(linked)}", json={field: value})
print(f"created {len(ids)} ERP customers and made {args.edits} edits across both systems")
