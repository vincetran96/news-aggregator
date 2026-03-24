"""
Exports Head-Fi post content from headfi.post_content into a single
plain-text file suitable for LLM ingestion.
"""

from datetime import datetime

from duckdb import DuckDBPyConnection

from src.storage.schemas.headfi.post_content import FULL_TBL_NAME
from src.utils.io import write_plaintext

POST_DELIMITER = "\n\n" + "=" * 60 + "\n\n"


def _fetch_post_texts(
    conn: DuckDBPyConnection,
    thread_base_url: str | None = None,
    since_tstamp: datetime | None = None,
) -> list[str]:
    """
    Fetches content_text for all posts ordered by page then post number.

    Args:
        conn (DuckDBPyConnection): Active DuckDB connection.
        thread_base_url (str | None): If given, only posts from that thread are
            returned. If None, all posts across all threads are returned.
        since_tstamp (datetime | None): If given, only posts with
            insert_tstamp > since_tstamp are returned (incremental export).

    Returns:
        A list of plain-text post bodies, excluding null or empty entries.
    """
    conditions: list[str] = []
    params: list = []
    if thread_base_url:
        conditions.append("thread_base_url = ?")
        params.append(thread_base_url)
    if since_tstamp:
        conditions.append("insert_tstamp > ?")
        params.append(since_tstamp)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT content_text
        FROM {FULL_TBL_NAME}
        {where}
        ORDER BY page_num ASC, post_num ASC NULLS LAST
    """
    rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows if row[0]]


def _format_export(texts: list[str]) -> str:
    """
    Joins post texts into a single string delimited by POST_DELIMITER.

    Args:
        texts (list[str]): Plain-text post bodies as returned by fetch_post_texts.

    Returns:
        The full export as a single string ready to be written to disk.
    """
    return POST_DELIMITER.join(texts) + "\n"


def export_to_string(
    conn: DuckDBPyConnection,
    thread_base_url: str | None = None,
    since_tstamp: datetime | None = None,
) -> tuple[str, int] | None:
    """
    Fetches and formats post content, returning it as a string.

    Args:
        conn (DuckDBPyConnection): Active DuckDB connection.
        thread_base_url (str | None): If given, exports only posts from that
            thread. If None, all threads are exported.
        since_tstamp (datetime | None): If given, only posts inserted after
            this timestamp are included.

    Returns:
        A (content, post_count) tuple, or None if no posts matched.
    """
    texts = _fetch_post_texts(
        conn, thread_base_url=thread_base_url, since_tstamp=since_tstamp
    )
    if not texts:
        return None
    return _format_export(texts), len(texts)


def export_to_file(
    conn: DuckDBPyConnection,
    path: str,
    thread_base_url: str | None = None,
) -> str:
    """
    Fetches, formats, and writes all post content to a plain-text file.

    Args:
        conn (DuckDBPyConnection): Active DuckDB connection.
        path (str): Destination file path (`.txt` extension appended if absent).
        thread_base_url (str | None): If given, exports only posts from that
            thread. If None, all threads are exported.

    Returns:
        The resolved file path that was written.
    """
    texts = _fetch_post_texts(conn, thread_base_url=thread_base_url)
    content = _format_export(texts)
    written_path = write_plaintext(content, path)
    print(f"Exported {len(texts)} post(s) to '{written_path}'.")
    return written_path
