"""
Incrementally crawls Head-Fi thread pages and stores raw HTML into DuckDB.

On each run, each coordinator queries the database for the highest page
already stored for its thread and advances by `crawl_window` pages.
"""

import logging

from duckdb import DuckDBPyConnection

from app.headfi.threads import THREADS
from src.collectors.url.headfi import HeadFiCrawlCoordinator, HeadFiURLCollector
from src.storage.db import get_connection
from src.storage.schemas.headfi.raw import SCHEMA_DDL, TBL_DDL

logger = logging.getLogger(__name__)

CRAWL_WINDOW = 5


def collect_raw_pages(conn: DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_DDL)
    conn.execute(TBL_DDL)

    threads = []
    for base_url, _ in THREADS:
        coordinator = HeadFiCrawlCoordinator(
            thread_base_url=base_url,
            conn=conn,
        )
        thread = coordinator.get_next_window(crawl_window=CRAWL_WINDOW)
        logger.info(
            f"[{base_url}]: To collect pages {thread.start_i} to {thread.end_i}"
        )
        threads.append(thread)

    collector = HeadFiURLCollector(
        threads=threads,
        conn=conn,
        concurrent_limit=5,
        batch_interval=10.0,
        request_timeout=30.0,
        max_retries=3,
        retry_delay=5.0,
    )
    collector.collect(result_batchsize=50)


def main() -> None:
    with get_connection() as conn:
        collect_raw_pages(conn)


if __name__ == "__main__":
    main()
