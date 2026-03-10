SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS headfi"

FULL_TBL_NAME = "headfi.raw_pages"

TBL_DDL = f"""
CREATE TABLE IF NOT EXISTS {FULL_TBL_NAME} (
    requested_url   VARCHAR        NOT NULL,
    final_url       VARCHAR        NOT NULL,
    thread_base_url VARCHAR        NOT NULL,
    page_num        INTEGER        NOT NULL,
    status_code     INTEGER,
    is_success      BOOLEAN        NOT NULL,
    num_attempts    INTEGER        NOT NULL,
    request_tstamp  TIMESTAMPTZ    NOT NULL,
    response_tstamp TIMESTAMPTZ,
    last_error      VARCHAR,
    content         VARCHAR,
    insert_tstamp   TIMESTAMPTZ    NOT NULL,
    PRIMARY KEY (final_url)
)
"""
