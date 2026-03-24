from dagster import define_asset_job

from dagster_src.assets.headfi.posts import (
    headfi_db_download,
    headfi_db_snapshot,
    headfi_post_content,
    headfi_post_content_export,
    headfi_raw_pages,
)

headfi_pipeline_job = define_asset_job(
    name="headfi_pipeline",
    selection=[
        headfi_db_download,
        headfi_raw_pages,
        headfi_post_content,
        headfi_post_content_export,
        headfi_db_snapshot,
    ],
)
