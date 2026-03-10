"""
Incrementally crawls Head-Fi thread pages and stores raw HTML into the local DuckDB database.

On each run, each coordinator queries the database for the highest page
already stored for its thread and advances by `crawl_window` pages.
"""

from src.collectors.url.concretes.headfi import HeadFiCrawlCoordinator, HeadFiURLCollector
from src.storage.db import get_connection
from src.storage.schemas.headfi.raw import SCHEMA_DDL, TBL_DDL

THREAD_BASE_URLS = [
    (
        "https://www.head-fi.org/threads/"
        "the-canjam-new-york-2026-impressions-thread-march-7-8-2026.979675/page-{i}"
    ),
]
CRAWL_WINDOW = 5


def main() -> None:
    with get_connection() as conn:
        conn.execute(SCHEMA_DDL)
        conn.execute(TBL_DDL)

        threads = []
        for base_url in THREAD_BASE_URLS:
            coordinator = HeadFiCrawlCoordinator(
                thread_base_url=base_url,
                conn=conn,
            )
            thread = coordinator.get_next_thread(crawl_window=CRAWL_WINDOW)
            print(f"[{base_url}]: To collect pages {thread.start_i} to {thread.end_i}")
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

        collector.run(result_batchsize=50)


if __name__ == "__main__":
    main()
