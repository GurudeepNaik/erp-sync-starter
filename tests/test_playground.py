"""The playground is the live demo; it must converge like the chaos test does and must count the
chaos it injected, or the page would be showing a green tick that proves nothing."""

from __future__ import annotations

from fastapi.testclient import TestClient

from erpsync.playground import Scenario, create_playground_app, run_scenario
from tests.conftest import URL


def test_rough_scenario_converges_and_counts_its_faults() -> None:
    result = run_scenario(URL, Scenario(seed=7, customers=12, operations=80))
    assert result["converged"], result["proof"]
    assert not result["timed_out"] and not result["stopped_early"]
    crm, hooks = result["crm"], result["webhooks"]
    assert (
        crm["http_500"] + crm["http_429"] + crm["http_504_after_commit"] + crm["lost_responses"] > 0
    )
    assert hooks["duplicated"] > 0 and hooks["dropped"] > 0
    assert hooks["ignored_as_duplicate"] > 0, "duplicate webhooks must be recognised, not applied"
    assert result["outbox"]["failed_attempts"] > 0, "some CRM call must have been retried"
    assert result["outbox"]["pending"] == 0 and result["inbox"].get("pending", 0) == 0
    assert result["final"]["dead_letter_customers"] == result["edits"]["bad_records"]
    assert (
        result["final"]["contacts"]
        == result["final"]["customers"] - result["final"]["dead_letter_customers"]
    )


def test_calm_scenario_has_no_faults_and_still_converges() -> None:
    calm = Scenario(
        seed=2,
        customers=5,
        operations=30,
        error_rate=0,
        rate_limit_rate=0,
        commit_then_fail_rate=0,
        lost_response_rate=0,
        webhook_duplicate=0,
        webhook_drop=0,
        webhook_shuffle=False,
        bad_data_rate=0,
    )
    result = run_scenario(URL, calm)
    assert result["converged"], result["proof"]
    assert result["crm"]["http_500"] == result["crm"]["lost_responses"] == 0
    assert result["webhooks"]["dropped"] == result["webhooks"]["duplicated"] == 0
    assert result["final"]["dead_letter_customers"] == 0


def test_api_stores_and_lists_runs() -> None:
    client = TestClient(create_playground_app(URL))
    assert client.get("/api/health").json() == {"status": "ok", "database": True}
    created = client.post("/api/runs", json={"seed": 3, "customers": 4, "operations": 20}).json()
    assert created["result"]["converged"]
    listed = client.get("/api/runs").json()
    assert any(r["id"] == created["id"] for r in listed)
    assert client.get(f"/api/runs/{created['id']}").json()["scenario"]["seed"] == 3
    assert client.post("/api/runs", json={"operations": 5}).status_code == 422  # below the minimum


def test_api_without_database_says_so() -> None:
    client = TestClient(create_playground_app(None))
    assert client.get("/api/health").json()["database"] is False
    assert client.post("/api/runs", json={}).status_code == 503
