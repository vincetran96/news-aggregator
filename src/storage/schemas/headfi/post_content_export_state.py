SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS headfi"

FULL_TBL_NAME = "headfi.post_content_export_state"

TBL_DDL = f"""
CREATE TABLE IF NOT EXISTS {FULL_TBL_NAME} (
    id               INTEGER       PRIMARY KEY DEFAULT 1,
    last_src_tstamp  TIMESTAMPTZ   NOT NULL,
    _change_tstamp   TIMESTAMPTZ   NOT NULL
)
"""
