from collections.abc import Generator
from contextlib import contextmanager

import duckdb


@contextmanager
def get_connection(
    db_path: str = "data/news.db",
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager that opens a DuckDB connection, yields it, and guarantees
    closure on exit regardless of whether an exception is raised.
    """
    conn = duckdb.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()
