import asyncio
from dataclasses import dataclass
from typing import Iterator, override

from httpx import AsyncClient

from src.collectors.url.base import BaseUrlCollector, UrlCollectorResult
from src.common.constants import DATETIME_FILENAME_FORMAT
from src.utils.datetime import now_utc
from src.utils.io import write_jsonl


@dataclass
class HeadFiThread:
    """
    Class representing a thread on HeadFi.
    The pages can be iterated from start_i to end_i.
    """
    base_url: str
    start_i: int
    end_i: int


class HeadFiURLCollector(BaseUrlCollector):
    def __init__(self, threads: list[HeadFiThread], output_dir: str = "output", *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._threads = threads
        self._output_dir = output_dir

    @override
    def iter_urls(self) -> Iterator[str]:
        """
        Concrete implementation of the base class method.

        Yields fully constructed URLs for each page in each thread,
        inclusive of both start_i and end_i.
        """
        for thread in self._threads:
            for i in range(thread.start_i, thread.end_i + 1):
                yield thread.base_url.format(i=i)

    @override
    async def get_urls(self, result_batchsize: int = 100000) -> None:
        """
        Concrete implementation of the base class method.

        Iterates over batches from _get_with_limit, accumulates results,
        and flushes to plaintext whenever the accumulated count reaches result_batchsize.
        Any remaining results are flushed at the end.
        """
        urls = list(self.iter_urls())
        accumulated: list[UrlCollectorResult] = []
        persist_idx = 0

        async for batch in self._get_with_limit(urls):
            accumulated.extend(batch)
            if len(accumulated) >= result_batchsize:
                self._persist_results(accumulated, batch_id=str(persist_idx))
                persist_idx += 1
                accumulated = []

        if accumulated:
            self._persist_results(accumulated, batch_id=str(persist_idx))

    @override
    async def _get_url_result(self, url: str, client: AsyncClient) -> UrlCollectorResult:
        """
        Concrete implementation of the base class method.
        """
        result = UrlCollectorResult(
            url=url,
            request_tstamp=now_utc(),
            num_attempts=0
        )
        error = None
        for retry in range(self.max_retries):
            result.num_attempts += 1
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                result.response_tstamp = now_utc()
                result.status_code = resp.status_code
                result.content = resp.text
                result.is_success = True
                break
            except Exception as exc:
                error = exc
            if retry == self.max_retries - 1:
                await asyncio.sleep(self._rand_retry_delay() * (retry + 1))
        else:
            result.is_success = False
            result.last_error = repr(error)

        return result

    def _persist_results(
        self,
        results: list[UrlCollectorResult],
        batch_id: str,
    ) -> None:
        """
        Persists the results to a JSONL file (one JSON object per line).
        Each record is the full UrlCollectorResult including raw HTML content.
        """
        records = [r.to_dict() for r in results]
        ts = now_utc().strftime(DATETIME_FILENAME_FORMAT)
        path = f"{self._output_dir}/raw_{ts}_{batch_id}"
        written = write_jsonl(records=records, path=path)
        print(f"Persisted {len(results)} results to {written}")
