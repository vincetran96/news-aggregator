"""
Head-Fi forum post assets.

Asset graph:
    headfi_db_download -> headfi_raw_pages -> headfi_post_content
        -> headfi_post_content_export -> headfi_db_snapshot
"""

import os

from dagster import AssetExecutionContext, asset

from app.headfi.run_collector import collect_raw_pages
from app.headfi.run_post_content_exporter_incr import export_post_content
from app.headfi.run_post_content_processor import process_post_content
from dagster_src.resources import MinIODuckDBResource

GROUP = "news_headfi"
DB_LOCAL_PATH = "/tmp/news.db"
EXPORT_DIR = "/tmp/headfi/export"
EXPORT_OBJECT_PREFIX = "export/"


@asset(group_name=GROUP)
def headfi_db_download(
    context: AssetExecutionContext,
    minio_duckdb: MinIODuckDBResource
) -> None:
    minio_duckdb.download(DB_LOCAL_PATH)
    context.log.info("Downloaded DB from MinIO (or starting fresh)")


@asset(group_name=GROUP, deps=[headfi_db_download])
def headfi_raw_pages(
    context: AssetExecutionContext,
    minio_duckdb: MinIODuckDBResource
) -> None:
    with minio_duckdb.get_connection(DB_LOCAL_PATH) as conn:
        collect_raw_pages(conn)


@asset(group_name=GROUP, deps=[headfi_raw_pages])
def headfi_post_content(
    context: AssetExecutionContext,
    minio_duckdb: MinIODuckDBResource
) -> None:
    with minio_duckdb.get_connection(DB_LOCAL_PATH) as conn:
        process_post_content(conn)


@asset(group_name=GROUP, deps=[headfi_post_content])
def headfi_post_content_export(
    context: AssetExecutionContext,
    minio_duckdb: MinIODuckDBResource
) -> None:
    with minio_duckdb.get_connection(DB_LOCAL_PATH) as conn:
        paths = export_post_content(conn, EXPORT_DIR)

    for path in paths:
        object_key = f"{EXPORT_OBJECT_PREFIX}{os.path.basename(path)}"
        minio_duckdb.upload(path, object_key=object_key)
        context.log.info(f"Uploaded {object_key} ({os.path.getsize(path):,} bytes)")


@asset(group_name=GROUP, deps=[headfi_post_content_export])
def headfi_db_snapshot(
    context: AssetExecutionContext,
    minio_duckdb: MinIODuckDBResource
) -> None:
    minio_duckdb.upload(DB_LOCAL_PATH)
    context.log.info(
        f"Persisted DB ({os.path.getsize(DB_LOCAL_PATH):,} bytes) "
        f"to {minio_duckdb.bucket}/{minio_duckdb.object_key}"
    )
