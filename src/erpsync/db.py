from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources

from psycopg import Connection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

Conn = Connection[DictRow]


def make_pool(url: str, size: int = 8) -> ConnectionPool[Conn]:
    pool: ConnectionPool[Conn] = ConnectionPool(
        url, min_size=1, max_size=size, kwargs={"row_factory": dict_row}, open=True
    )
    pool.wait()
    return pool


def migrate(pool: ConnectionPool[Conn]) -> None:
    sql = resources.files("erpsync").joinpath("schema.sql").read_text()
    with pool.connection() as conn:
        # Serialise concurrent startups (several workers booting at once).
        conn.execute("select pg_advisory_xact_lock(424242)")
        conn.execute(sql)


@contextmanager
def as_sync(conn: Conn) -> Iterator[Conn]:
    """Run writes as the sync itself, so the capture trigger does not re-publish them."""
    with conn.transaction():
        conn.execute("select set_config('erpsync.origin', 'sync', true)")
        yield conn
