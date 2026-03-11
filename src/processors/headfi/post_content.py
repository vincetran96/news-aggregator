"""
Module storing logic to process post content from Head-Fi threads.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag
from duckdb import DuckDBPyConnection

from src.storage.schemas.headfi.post_content import FULL_TBL_NAME as TGT_TBL_NAME
from src.storage.schemas.headfi.raw import FULL_TBL_NAME as SRC_TBL_NAME
from src.utils.datetime import now_utc


def _parse_post_num(text: str) -> int | None:
    """
    Extracts the sequential post number within a thread.
    'Post #3 of 318' -> 3. Returns None if the pattern is not present.
    """
    match = re.search(r"Post\s+#(\d+)", text)
    return int(match.group(1)) if match else None


def _extract_content(article: Tag) -> tuple[str | None, str | None]:
    """
    Returns
    - (inner_html, plain_text) of the `bbWrapper` div inside an article.
    - (None, None) for deleted, moderated, or media-only posts where the `bbWrapper` element is absent.
    """
    wrapper = article.find(class_="bbWrapper")
    if wrapper is None:
        return None, None
    return wrapper.decode_contents(), wrapper.get_text(separator=" ", strip=True)


def _extract_posts(
    html: str,
    final_url: str,
    thread_base_url: str,
    page_num: int,
) -> list[dict]:
    """
    Parses raw page HTML and returns one dict per post found.

    Each dict contains all target table columns except `insert_tstamp`,
    which is assigned at write time by `upsert_posts`.

    Args:
        html (str): Raw HTML string of a single crawled thread page.
        final_url (str): The resolved URL of the page after redirects, carried
            over from `headfi.raw_pages` and stored on each post for traceability.
        thread_base_url (str): The canonical base URL of the thread (without page
            parameters), used to group posts across pages.
        page_num (int): The page number within the thread this HTML was fetched from.

    Returns:
        A list of dicts, one per `article.message--post` element found in the
        HTML. Articles with no `data-content` attribute are silently skipped.
        Returns an empty list if no posts are found.
    """
    soup = BeautifulSoup(html, "lxml")
    articles = soup.find_all("article", class_="message--post")

    posts = []
    for article in articles:
        post_id = str(article.get("data-content") or "")
        if not post_id:
            continue

        author = article.get("data-author", "")
        header = article.find(class_="message-header")
        header_links = header.find_all("a") if header else []
        posted_at_raw = header_links[0].get_text(strip=True) if header_links else None
        post_num_text = header_links[1].get_text(strip=True) if len(header_links) > 1 else ""
        post_num = _parse_post_num(post_num_text)

        content_html, content_text = _extract_content(article)

        posts.append(
            {
                "post_id": post_id,
                "thread_base_url": thread_base_url,
                "final_url": final_url,
                "page_num": page_num,
                "post_num": post_num,
                "author": author,
                "posted_at_raw": posted_at_raw,
                "content_html": content_html,
                "content_text": content_text,
            }
        )

    return posts


def _upsert_posts(conn: DuckDBPyConnection, posts: list[dict]) -> None:
    """
    Bulk-inserts post dicts into target table.

    Rows whose `post_id` already exists are silently skipped (ON CONFLICT DO NOTHING).
    """
    if not posts:
        print("No posts to insert.")
        return

    insert_tstamp = now_utc()
    rows = [
        (
            p["post_id"],
            p["thread_base_url"],
            p["final_url"],
            p["page_num"],
            p["post_num"],
            p["author"],
            p["posted_at_raw"],
            p["content_html"],
            p["content_text"],
            insert_tstamp,
        )
        for p in posts
    ]

    row = conn.execute(f"SELECT COUNT(*) FROM {TGT_TBL_NAME}").fetchone()
    count_before = row[0] if row else 0
    conn.executemany(
        f"""
        INSERT INTO {TGT_TBL_NAME} (
            post_id, thread_base_url, final_url, page_num, post_num,
            author, posted_at_raw, content_html, content_text, insert_tstamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (post_id) DO NOTHING
        """,
        rows,
    )
    row = conn.execute(f"SELECT COUNT(*) FROM {TGT_TBL_NAME}").fetchone()
    count_after = row[0] if row else 0
    inserted = count_after - count_before
    skipped = len(posts) - inserted
    print(f"Inserted {inserted} post(s) into {TGT_TBL_NAME} ({skipped} skipped as duplicates).")


def process_new_pages(conn: DuckDBPyConnection) -> None:
    """
    Incremental orchestration: reads `raw_pages` rows not yet reflected in
    `post_content` and processes them into Head-Fi Post Content table.

    Watermark logic: fetches raw_pages rows where
        `is_success = TRUE AND insert_tstamp >= MAX(post_content.insert_tstamp)`

    On first run (`post_content` is empty) all successful raw page rows are processed.
    The ">=" boundary means rows at exactly the watermark are re-evaluated;
    `ON CONFLICT DO NOTHING` on post_id ensures this is idempotent.
    """
    watermark_row = conn.execute(f"SELECT MAX(insert_tstamp) FROM {TGT_TBL_NAME}").fetchone()
    watermark = watermark_row[0] if watermark_row else None

    if watermark is None:
        rows = conn.execute(
            f"""
            SELECT final_url, content, thread_base_url, page_num
            FROM {SRC_TBL_NAME}
            WHERE is_success = TRUE
            """
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT final_url, content, thread_base_url, page_num
            FROM {SRC_TBL_NAME}
            WHERE is_success = TRUE
                AND insert_tstamp >= ?
            """,
            [watermark],
        ).fetchall()

    print(f"Processing {len(rows)} raw page(s).")

    all_posts: list[dict] = []
    for final_url, content, thread_base_url, page_num in rows:
        if not content:
            continue
        all_posts.extend(_extract_posts(content, final_url, thread_base_url, page_num))

    _upsert_posts(conn, all_posts)
