from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import override

from duckdb import DuckDBPyConnection
from httpx import AsyncClient

from src.collectors.url.base import BaseUrlCollector, UrlCollectorResult
from src.collectors.url.headfi.dataclasses import HeadFiThread, UrlCrawlMeta
from src.collectors.url.headfi.helpers import get_max_page_num, upsert_raw_pages
from src.utils.datetime import now_utc


class HeadFiCrawlCoordinator:
    """
    Determines the next page range to crawl for a given Head-Fi thread
    by querying the database for the highest page already stored.

    Keeps the incremental crawl logic out of the collector itself.
    """

    def __init__(self, thread_base_url: str, conn: DuckDBPyConnection) -> None:
        self._thread_base_url = thread_base_url
        self._conn = conn

    def get_next_window(self, crawl_window: int) -> HeadFiThread:
        """
        Returns a HeadFiThread starting one page beyond the current maximum,
        spanning `crawl_window` pages.
        """
        max_page = get_max_page_num(self._conn, self._thread_base_url)
        start_i = (max_page + 1) if max_page is not None else 1
        end_i = start_i + crawl_window - 1
        return HeadFiThread(
            base_url=self._thread_base_url,
            start_i=start_i,
            end_i=end_i,
        )


class HeadFiURLCollector(BaseUrlCollector):
    def __init__(
        self,
        threads: list[HeadFiThread],
        conn: DuckDBPyConnection,
        *args,
        follow_redirects: bool = False,
        http_conn_limit: int = 5,
        **kwargs,
    ):
        super().__init__(
            *args, http_conn_limit=http_conn_limit, follow_redirects=follow_redirects, **kwargs
        )
        self._threads = threads
        self._conn = conn
        self._url_meta: dict[str, UrlCrawlMeta] = {}

    @override
    def iter_urls(self) -> Iterator[str]:
        """
        Yields fully constructed page URLs for all forum threads,
        inclusive of both start_i and end_i.
        """
        for url, _ in self._iter_url_with_meta():
            yield url

    @override
    async def crawl_urls(self, result_batchsize: int = 100000) -> None:
        """
        Crawls all URLs from iter_urls, accumulate results,
        and persists to the database in batches of `result_batchsize`.
        """
        self._url_meta = self._build_url_meta()
        urls = list(self._url_meta.keys())

        accumulated: list[UrlCollectorResult] = []
        persist_idx = 0

        async for batch in self._crawl_with_limit(urls):
            accumulated.extend(batch)
            if len(accumulated) >= result_batchsize:
                self._persist_results(accumulated, batch_id=str(persist_idx))
                persist_idx += 1
                accumulated = []

        if accumulated:
            self._persist_results(accumulated, batch_id=str(persist_idx))

    @override
    async def _get_url_result(self, url: str, client: AsyncClient) -> UrlCollectorResult:
        result = UrlCollectorResult(
            url=url,
            request_tstamp=now_utc(),
            num_attempts=0,
        )
        error = None
        for retry in range(self.max_retries):
            result.num_attempts += 1
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                if resp.is_redirect:
                    raise ValueError(f"Unexpected redirect to: {resp.headers.get('location', '?')}")
                result.final_url = str(resp.url)
                result.response_tstamp = now_utc()
                result.status_code = resp.status_code
                result.content = resp.text
                result.is_success = True
                break
            except Exception as exc:
                error = exc
            if retry < self.max_retries - 1:
                await asyncio.sleep(self._rand_retry_delay() * (retry + 1))
        else:
            result.is_success = False
            result.last_error = repr(error)

        return result

    def _iter_url_with_meta(self) -> Iterator[tuple[str, UrlCrawlMeta]]:
        """
        Single source of truth for URL generation.

        Yields (url, UrlCrawlMeta) pairs for every page across all forum threads.
        """
        for thread in self._threads:
            for i in range(thread.start_i, thread.end_i + 1):
                # Special case for i == 1.
                if i == 1:
                    url = thread.base_url.replace("page-{i}", "")
                else:
                    url = thread.base_url.format(i=i)
                yield url, UrlCrawlMeta(thread_base_url=thread.base_url, page_num=i)

    def _build_url_meta(self) -> dict[str, UrlCrawlMeta]:
        """
        Builds a mapping of requested_url -> UrlCrawlMeta.

        Used when persisting results so each URL can be associated with its
        thread and page number.
        """
        return {url: meta for url, meta in self._iter_url_with_meta()}

    def _persist_results(
        self,
        results: list[UrlCollectorResult],
        batch_id: str,
    ) -> None:
        upsert_raw_pages(self._conn, results, self._url_meta)
        print(f"[Batch {batch_id}] Upserted {len(results)} results to raw_pages")
