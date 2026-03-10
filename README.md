# App
## Common
```bash
uv run -m app.common.preview_db headfi.raw_pages --select-columns page_num final_url insert_tstamp --order-by "insert_tstamp asc" "page_num desc"
```
