from __future__ import annotations

import logging
import threading
from typing import Annotated

import httpx
import typer
import uvicorn

from erpsync import db
from erpsync.backstop import compare, run_backstop
from erpsync.crm_client import CrmClient
from erpsync.crm_mock import CrmState, create_crm_app, deliver_forever
from erpsync.service import Settings, create_app, run_workers

app = typer.Typer(add_completion=False, no_args_is_help=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


@app.command()
def migrate() -> None:
    """Create or update the sync schema. Run once per deploy, before api and workers start."""
    db.migrate(db.make_pool(Settings().database_url, size=1))
    typer.echo("schema up to date")


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Webhook receiver, admin and metrics endpoints."""
    uvicorn.run(create_app(), host=host, port=port)


@app.command()
def worker(relays: Annotated[int, typer.Option(help="Relay threads")] = 2) -> None:
    """Outbound relay + inbound processor."""
    run_workers(Settings(), relays=relays)


@app.command()
def backstop() -> None:
    """Find CRM changes whose webhooks never arrived, and ERP rows that never reached the CRM."""
    settings = Settings()
    pool = db.make_pool(settings.database_url, size=2)
    report = run_backstop(pool, CrmClient(httpx.Client(base_url=settings.crm_url, timeout=30)))
    typer.echo(report)


@app.command()
def check() -> None:
    """Compare ERP and CRM; exit 1 if they disagree."""
    settings = Settings()
    pool = db.make_pool(settings.database_url, size=2)
    diff = compare(pool, CrmClient(httpx.Client(base_url=settings.crm_url, timeout=30)))
    for kind, ids in diff.items():
        typer.echo(f"{kind:<18} {len(ids)}")
    if any(diff.values()):
        raise typer.Exit(1)


@app.command("crm-mock")
def crm_mock(
    host: str = "0.0.0.0",
    port: int = 8001,
    webhook_url: str = "http://localhost:8000/webhooks/crm",
    secret: str = "change-me",
) -> None:
    """Run the unreliable mock CRM and deliver its webhooks."""
    state = CrmState()
    threading.Thread(target=deliver_forever, args=(state, webhook_url, secret), daemon=True).start()
    uvicorn.run(create_crm_app(state), host=host, port=port)


if __name__ == "__main__":
    app()
