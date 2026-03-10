import asyncio
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncGenerator, Iterator

from httpx import AsyncClient, Limits


@dataclass
class UrlCollectorResult:
    url: str
    request_tstamp: datetime
    response_tstamp: datetime | None = None
    num_attempts: int = 0
    status_code: int | None = None
    content: Any | None = None
    is_success: bool = False
    last_error: str | None = None

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "request_tstamp": self.request_tstamp.isoformat(),
            "response_tstamp": self.response_tstamp.isoformat() if self.response_tstamp else None,
            "num_attempts": self.num_attempts,
            "status_code": self.status_code,
            "content": self.content,
            "is_success": self.is_success,
            "last_error": self.last_error,
        }


class BaseUrlCollector(ABC):
    def __init__(
        self,
        concurrent_limit: int = 100,
        batch_interval: float = 30.0,
        request_timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        follow_redirects: bool = True,
    ):
        self.concurrent_limit: int = concurrent_limit
        self.batch_interval: float = batch_interval
        self.request_timeout: float = request_timeout
        self.max_retries: int = max_retries
        self.retry_delay: float = retry_delay
        self.follow_redirects: bool = follow_redirects
        self.http_conn_limit: int = 5                   # This number should be conservative.

    @abstractmethod
    def iter_urls(self) -> Iterator[str]:
        """
        Returns an iterator of URLs to collect.
        The signature must be implemented in concrete classes.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_urls(self, result_batchsize: int = 100000) -> None:
        """
        Gets the results from the self's list of URLs.
        The signature must be implemented in concrete classes.
        """
        raise NotImplementedError

    @abstractmethod
    async def _get_url_result(self, url: str, client: AsyncClient) -> UrlCollectorResult:
        """
        Gets the result for a single URL.
        The signature must be implemented in concrete classes.
        """
        raise NotImplementedError

    def run(self, *, runner=asyncio.run, **kwargs) -> None:
        """
        Synchronous entry point that wraps `get_urls` for callers that should not
        need to manage an event loop directly.

        `runner` can be replaced with a custom async runner (e.g. one that spawns
        a new event loop per thread when using concurrent.futures.ThreadPoolExecutor).
        """
        runner(self.get_urls(**kwargs))

    def _rand_batch_interval(self) -> float:
        return random.uniform(1, self.batch_interval)

    def _rand_retry_delay(self) -> float:
        return random.uniform(1, self.retry_delay)

    async def _get_batch(self, url_batch: list[str], client: AsyncClient) -> list[UrlCollectorResult]:
        """
        Gets all URLs in this batch. The number of URLs in this batch should
        conform to the concurrent limit.
        """
        tasks = [self._get_url_result(url=url, client=client) for url in url_batch]
        return await asyncio.gather(*tasks)

    async def _get_with_limit(self, urls: list[str]) -> AsyncGenerator[list[UrlCollectorResult], None]:
        """
        Batches the provided URLs as per the concurrent limit.
        We use one single Async client for all the batches.
        """
        start_time = time.monotonic()
        print(f"Length of url queue: {len(urls)}")

        async with AsyncClient(
            limits=Limits(max_connections=self.http_conn_limit),
            timeout=self.request_timeout,
            follow_redirects=self.follow_redirects,
        ) as client:
            while urls:
                batch_start = time.monotonic()
                batch: list[str] = []
                while urls and len(batch) < self.concurrent_limit:
                    batch.append(urls.pop())

                if not batch:
                    break
                results = await self._get_batch(url_batch=batch, client=client)
                now_time = time.monotonic()
                print(
                    f"Elapsed since start: {now_time - start_time:.2f}s, "
                    f"Average speed: {len(results) / (now_time - start_time):.2f} results/s"
                )
                yield results

                # Wait if needed, at least 0.5s
                if urls:
                    batch_duration = time.monotonic() - batch_start
                    wait_time = max(0.5, self._rand_batch_interval() - batch_duration)
                    print(f"Waiting {wait_time:.2f}s before next batch...")
                    await asyncio.sleep(wait_time)
