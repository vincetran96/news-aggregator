SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS headfi"

FULL_TBL_NAME = "headfi.post_content"

TBL_DDL = f"""
CREATE TABLE IF NOT EXISTS {FULL_TBL_NAME} (
    post_id         VARCHAR        PRIMARY KEY,
    thread_base_url VARCHAR        NOT NULL,
    final_url       VARCHAR        NOT NULL,
    page_num        INTEGER        NOT NULL,
    post_num        INTEGER,
    author          VARCHAR        NOT NULL,
    posted_at_raw   VARCHAR,
    content_html    VARCHAR,
    content_text    VARCHAR,
    insert_tstamp   TIMESTAMPTZ    NOT NULL
)
"""
