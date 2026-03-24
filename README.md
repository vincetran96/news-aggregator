# App
## Common
```bash
uv run -m app.common.preview_db headfi.raw_pages --select-columns page_num final_url insert_tstamp --order-by "insert_tstamp asc" "page_num desc"
```


# K8s Deployment Steps
```bash
./build/build.sh localhost:5000/news-aggregator:latest
```


# Architectural Decisions

A reference document capturing the key design choices made in the `news-aggregator` project, with reasoning and lessons for building similar crawl/pipeline systems.

---

## 1. Separate raw storage from processing

**Decision:** Crawl and store raw HTML first (`headfi.raw_pages`), parse it in a separate stage (`headfi.post_content`).

**Reasoning:** HTML is the source of truth. Parsing logic can change (e.g. the site restructures its markup), and if you've already discarded the raw content you can't reprocess. Keeping the raw layer also means you can add new extraction fields later without re-crawling.

**Alternative rejected:** Parsing inline during the crawl, which couples two concerns that change at different rates — transport/retry logic vs. HTML parsing logic.

---

## 2. Use `final_url` as the primary key on `raw_pages`, not `requested_url`

**Decision:** The PK on `raw_pages` is the URL after redirect resolution, not the URL you asked for.

**Reasoning:** Head-Fi redirects out-of-bound page numbers to the latest actual page. If you key on requested URL, pages 22–26 all insert successfully as distinct rows despite containing identical content. Keying on `final_url` forces the constraint: one row per actual page, and the collision is surfaced rather than silently stored.

**Lesson:** In any crawler that follows redirects, the requested URL and the final URL are semantically different. Design your schema around the one that carries the real identity.

---

## 3. Disable redirect-following at the HTTP client level

**Decision:** `follow_redirects=False` is the default in `HeadFiURLCollector`, explicitly overriding the base class default of `True`.

**Reasoning:** Letting the HTTP client silently follow redirects and marking the request as successful means you can never detect the redirect-to-page-21 problem at the application layer. By raising on redirects instead, the result is correctly marked as failed, and the watermark-based coordinator won't advance past the actual latest page.

**Lesson:** Transparent redirect-following is a convenience that hides information. For crawlers where URL identity matters, disable it and handle redirects explicitly.

---

## 4. Decouple crawl strategy from HTTP transport via `HeadFiCrawlCoordinator`

**Decision:** A separate coordinator object owns the "what pages come next?" logic. The collector only knows how to fetch a given list of URLs.

**Reasoning:** The coordinator queries the database, determines the window, and produces a `HeadFiThread`. The collector is unaware of any of this. This means you can change the incremental strategy (e.g. switch from window-based to gap-detection) without touching the HTTP layer, and you can test the window logic in isolation.

**Lesson:** Crawl strategy and HTTP transport are two distinct concerns even though they're superficially related. Separating them pays off the moment you need to change either independently.

---

## 5. Watermark-based incremental processing using `insert_tstamp`

**Decision:** The processor queries `MAX(insert_tstamp)` from `post_content` and reprocesses any `raw_pages` rows at or after that timestamp.

**Reasoning:** Simple and effective for a pipeline where rows are only ever inserted, not updated (outside of successful re-fetches). The `>=` boundary combined with `ON CONFLICT DO NOTHING` on `post_id` makes the processor fully idempotent — running it multiple times without new data is a no-op.

**Lesson:** Watermark-based processing is the simplest form of incremental state management. It only requires one timestamp column and one `MAX()` query. The idempotency guarantee comes from the conflict handling at write time, not from the watermark logic itself.

---

## 6. Upsert conflict resolution is asymmetric by success state

**Decision:** In `upsert_raw_pages`, `ON CONFLICT DO UPDATE` only fires `WHERE EXCLUDED.is_success = TRUE`. A failed incoming row does nothing; a successful one overwrites everything.

**Reasoning:** On retry, you want a successful result to replace a prior failure. But you never want a transient failure to overwrite a previously successful crawl. The condition makes the semantics explicit: success is always the canonical state, failure is always provisional.

**Lesson:** Upsert semantics in crawl pipelines are rarely symmetric. Think explicitly about which direction of conflict (new vs. existing) should win, and under what conditions.

---

## 7. Use DuckDB as a local embedded store rather than flat files

**Decision:** All intermediate and final data lives in `data/news.db` rather than JSONL files on disk.

**Reasoning:** SQL makes incremental queries trivial (`MAX(insert_tstamp)`, `WHERE is_success = TRUE`), whereas flat files require loading everything into memory or baking page numbers into filenames. The added operability (query from CLI, inspect with `preview_db.py`) outweighs the slight overhead compared to eyeballing JSONL.

**Lesson:** For pipelines with even minimal state management needs, an embedded SQL store pays for itself quickly. Flat files are fine for one-shot outputs but become friction as soon as you need queries.

---

## 8. Three-stage pipeline with independently runnable scripts

**Decision:** Each stage (`run_collector`, `run_post_content_processor`, `run_post_content_exporter`) is a standalone script that can be triggered independently.

**Reasoning:** Stages fail at different rates and for different reasons. The collector fails due to network issues; the processor fails due to HTML parsing changes; the exporter rarely fails. Decoupling them means you can re-run any single stage without re-executing the others, and you can inspect the state between stages.

**Lesson:** Don't wire pipeline stages into a single monolithic script unless you have an orchestrator (e.g. Airflow, Prefect) that handles partial failure and re-runs gracefully. For small projects, independent scripts with a shared database are the simpler equivalent.

---

## 9. Keep `BaseUrlCollector` generic enough to retarget

**Decision:** `BaseUrlCollector` has no Head-Fi-specific logic. URL generation, metadata mapping, and persistence are all in the concrete subclass.

**Reasoning:** The base class owns concurrency, retry, batching, and the async loop. Concrete classes own domain knowledge — URL patterns, HTML structure, storage schema.

**Lesson:** When building a crawler, identify which parts are universal (HTTP mechanics, retry logic, concurrency) and which are domain-specific (URL patterns, HTML structure, storage schema). The split will almost always be worth making even for a single-target project.

---

## 10. Export is a projection, not a transformation

**Decision:** The exporter simply queries `content_text` in order and concatenates with a delimiter. No further transformation.

**Reasoning:** By the time data reaches the export stage, all transformation has already happened in the processor. The exporter is deliberately dumb — its only job is formatting for the downstream consumer (LLM ingestion as plain text).

**Lesson:** Keeping the export stage as a pure projection simplifies it enough that it rarely needs to change. If transformation logic creeps into the exporter, it's a sign the processor stage is incomplete.


# Appearances
This project is featured in [my blog post](https://vinni-tran.notion.site/Instead-of-Scrolling-for-2-Hours-I-Ended-Up-Coding-for-6-Hours-to-Summarize-Audiophile-Content-321abf9a12fb80a3b9cbf30c80fbf23b?source=copy_link).
