from duckdb import DuckDBPyConnection

from src.collectors.url.base import UrlCollectorResult
from src.collectors.url.headfi.dataclasses import UrlCrawlMeta
from src.storage.schemas.headfi.raw import FULL_TBL_NAME
from src.utils.datetime import now_utc


def upsert_raw_pages(
    conn: DuckDBPyConnection,
    results: list[UrlCollectorResult],
    url_meta: dict[str, UrlCrawlMeta],
) -> None:
    """
    Upserts collector results into raw_pages keyed on `final_url`.

    Conflict resolution:
      - Incoming row is_success=TRUE  -> overwrite the existing row entirely.
      - Incoming row is_success=FALSE -> DO NOTHING (preserve any prior success).

    This allows failed requests to be retried on subsequent runs while ensuring
    a successful re-fetch always overwrites a stale or failed record.
    Results with no `final_url` fall back to the requested URL as the key.
    """
    insert_tstamp = now_utc()
    rows = []
    for r in results:
        meta = url_meta.get(r.url)
        thread_base_url = meta.thread_base_url if meta else ""
        page_num = meta.page_num if meta else 0
        final_url = r.final_url if r.final_url else r.url
        rows.append(
            (
                r.url,
                final_url,
                thread_base_url,
                page_num,
                r.status_code,
                r.is_success,
                r.num_attempts,
                r.request_tstamp,
                r.response_tstamp,
                r.last_error,
                r.content,
                insert_tstamp,
            )
        )

    conn.executemany(
        f"""
        INSERT INTO {FULL_TBL_NAME} (
            requested_url, final_url, thread_base_url, page_num,
            status_code, is_success, num_attempts,
            request_tstamp, response_tstamp, last_error, content,
            insert_tstamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (final_url) DO UPDATE SET
            requested_url   = EXCLUDED.requested_url,
            thread_base_url = EXCLUDED.thread_base_url,
            page_num        = EXCLUDED.page_num,
            status_code     = EXCLUDED.status_code,
            is_success      = EXCLUDED.is_success,
            num_attempts    = EXCLUDED.num_attempts,
            request_tstamp  = EXCLUDED.request_tstamp,
            response_tstamp = EXCLUDED.response_tstamp,
            last_error      = EXCLUDED.last_error,
            content         = EXCLUDED.content,
            insert_tstamp   = EXCLUDED.insert_tstamp
        WHERE EXCLUDED.is_success = TRUE
        """,
        rows,
    )


def get_max_page_num(conn: DuckDBPyConnection, thread_base_url: str) -> int | None:
    """
    Returns the highest page number already stored for the given thread,
    or None if no pages have been crawled yet.
    """
    row = conn.execute(
        f"""
        SELECT MAX(page_num) FROM {FULL_TBL_NAME}
        WHERE thread_base_url = ?
            AND is_success = TRUE
        """,
        [thread_base_url],
    ).fetchone()
    return row[0] if row else None
