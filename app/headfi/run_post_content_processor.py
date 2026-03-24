"""
Processes raw Head-Fi pages stored in headfi.raw_pages and extracts individual
post content into headfi.post_content.

On each run, only pages whose `insert_tstamp` is at or beyond the current watermark
of `post_content` are processed. On first run, all successful raw pages are processed.
"""

from duckdb import DuckDBPyConnection

from src.processors.headfi.post_content import process_new_pages
from src.storage.db import get_connection
from src.storage.schemas.headfi.post_content import SCHEMA_DDL, TBL_DDL


def process_post_content(conn: DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_DDL)
    conn.execute(TBL_DDL)

    process_new_pages(conn)


def main() -> None:
    with get_connection() as conn:
        process_post_content(conn)


if __name__ == "__main__":
    main()
