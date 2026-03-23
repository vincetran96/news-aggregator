"""
Head-Fi forum post assets.

Asset graph:  headfi_raw_pages -> headfi_post_content -> headfi_db_snapshot
"""

import os

from dagster import AssetExecutionContext, asset

from dagster_src.resources import MinIODuckDBResource
from src.collectors.url.headfi import HeadFiCrawlCoordinator, HeadFiURLCollector
from src.processors.headfi.post_content import process_new_pages
from src.storage.schemas.headfi.post_content import SCHEMA_DDL as PC_SCHEMA_DDL
from src.storage.schemas.headfi.post_content import TBL_DDL as PC_TBL_DDL
from src.storage.schemas.headfi.raw import SCHEMA_DDL as RAW_SCHEMA_DDL
from src.storage.schemas.headfi.raw import TBL_DDL as RAW_TBL_DDL

THREAD_BASE_URLS = [
    (
        "https://www.head-fi.org/threads/"
        "the-canjam-new-york-2026-impressions-thread-march-7-8-2026.979675/page-{i}"
    ),
]
CRAWL_WINDOW = 5

GROUP = "news_headfi"


@asset(group_name=GROUP)
def headfi_raw_pages(
    context: AssetExecutionContext, minio_duckdb: MinIODuckDBResource
) -> None:
    minio_duckdb.download()
    context.log.info("Downloaded DB from MinIO (or starting fresh)")

    with minio_duckdb.get_connection() as conn:
        conn.execute(RAW_SCHEMA_DDL)
        conn.execute(RAW_TBL_DDL)

        threads = []
        for base_url in THREAD_BASE_URLS:
            coordinator = HeadFiCrawlCoordinator(
                thread_base_url=base_url,
                conn=conn,
            )
            thread = coordinator.get_next_window(crawl_window=CRAWL_WINDOW)
            context.log.info(
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


@asset(group_name=GROUP, deps=[headfi_raw_pages])
def headfi_post_content(
    context: AssetExecutionContext, minio_duckdb: MinIODuckDBResource
) -> None:
    with minio_duckdb.get_connection() as conn:
        conn.execute(PC_SCHEMA_DDL)
        conn.execute(PC_TBL_DDL)

        process_new_pages(conn)


@asset(group_name=GROUP, deps=[headfi_post_content])
def headfi_db_snapshot(
    context: AssetExecutionContext, minio_duckdb: MinIODuckDBResource
) -> None:
    minio_duckdb.upload()
    file_size = os.path.getsize(minio_duckdb.local_path)
    context.log.info(
        f"Persisted DB ({file_size:,} bytes) "
        f"to {minio_duckdb.bucket}/{minio_duckdb.object_key}"
    )
