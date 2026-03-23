"""Dagster top-level definitions
"""

from dagster import Definitions, EnvVar

from dagster_src.assets.headfi.posts import (
    headfi_db_snapshot,
    headfi_post_content,
    headfi_raw_pages,
)
from dagster_src.jobs import headfi_pipeline_job
from dagster_src.resources import MinIODuckDBResource
from dagster_src.schedules import headfi_pipeline__daily

defs = Definitions(
    assets=[
        headfi_raw_pages,
        headfi_post_content,
        headfi_db_snapshot,
    ],
    jobs=[
        headfi_pipeline_job,
    ],
    schedules=[
        headfi_pipeline__daily,
    ],
    resources={
        "minio_duckdb": MinIODuckDBResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ROOT_USER"),
            secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        ),
    },
)
