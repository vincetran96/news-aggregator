"""
Incrementally exports Head-Fi post content as plain-text files to a local
directory.

Reads the watermark from headfi.post_content_export_state, queries posts
newer than the watermark, writes each thread's export as a text file, and
updates the watermark.  Returns the list of written file paths so the caller
can decide what to do with them (e.g. upload to object storage).
"""

import logging
import os
from datetime import datetime

from duckdb import DuckDBPyConnection

from app.headfi.threads import THREADS
from src.common.constants import DATETIME_FILENAME_FORMAT
from src.exporters.headfi.post_content import export_to_string
from src.storage.schemas.headfi.post_content import FULL_TBL_NAME as PC_FULL_TBL_NAME
from src.storage.schemas.headfi.post_content_export_state import (
    FULL_TBL_NAME as ES_FULL_TBL_NAME,
)
from src.storage.schemas.headfi.post_content_export_state import (
    SCHEMA_DDL as ES_SCHEMA_DDL,
)
from src.storage.schemas.headfi.post_content_export_state import (
    TBL_DDL as ES_TBL_DDL,
)
from src.utils.datetime import now_utc
from src.utils.io import write_plaintext

logger = logging.getLogger(__name__)


def export_post_content(
    conn: DuckDBPyConnection, output_dir: str
) -> list[str]:
    """
    Export new posts as plain-text files to *output_dir*.

    Returns the list of written file paths (empty if no new posts).
    """
    conn.execute(ES_SCHEMA_DDL)
    conn.execute(ES_TBL_DDL)

    since_tstamp: datetime | None = None
    row = conn.execute(
        f"SELECT last_src_tstamp FROM {ES_FULL_TBL_NAME} WHERE id = 1"
    ).fetchone()
    if row:
        since_tstamp = row[0]

    if since_tstamp:
        logger.info(f"Incremental export: posts after {since_tstamp}")
    else:
        logger.info("First export: exporting all posts")

    timestamp = now_utc().strftime(DATETIME_FILENAME_FORMAT)
    written_paths: list[str] = []

    for thread_url, prefix in THREADS:
        result = export_to_string(
            conn, thread_base_url=thread_url, since_tstamp=since_tstamp
        )
        if result is None:
            logger.info(f"No new posts for '{prefix}', skipping")
            continue

        content, post_count = result
        path = write_plaintext(
            content, os.path.join(output_dir, f"{prefix}{timestamp}")
        )
        logger.info(f"Exported {post_count} posts to {path}")
        written_paths.append(path)

    if not written_paths:
        logger.info("No new posts to export across all threads")
        return written_paths

    max_row = conn.execute(
        f"SELECT MAX(insert_tstamp) FROM {PC_FULL_TBL_NAME}"
        + (" WHERE insert_tstamp > ?" if since_tstamp else ""),
        [since_tstamp] if since_tstamp else [],
    ).fetchone()
    new_max = max_row[0] if max_row else None

    if new_max:
        now = now_utc()
        conn.execute(
            f"""
            INSERT INTO {ES_FULL_TBL_NAME} (id, last_src_tstamp, _change_tstamp)
            VALUES (1, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                last_src_tstamp = EXCLUDED.last_src_tstamp,
                _change_tstamp = EXCLUDED._change_tstamp
            """,
            [new_max, now],
        )
        logger.info(f"Export state updated: last_src_tstamp = {new_max}")

    return written_paths


if __name__ == "__main__":
    from src.storage.db import get_connection

    with get_connection() as conn:
        export_post_content(conn, output_dir="output/headfi/export")
