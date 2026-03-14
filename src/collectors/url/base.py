import asyncio
import random
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from httpx import AsyncClient, Limits


@dataclass
class UrlCollectorResult:
    url: str
    request_tstamp: datetime
    response_tstamp: datetime | None = None
    final_url: str | None = None
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
            "final_url": self.final_url,
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
        http_conn_limit: int = 10,
    ):
        """
        Args:
            concurrent_limit (int): Maximum number of URLs fetched in a single
                batch before persisting results and waiting for the next interval.
            batch_interval (float): Upper bound in seconds for the random wait
                between batches. Actual wait is randomized in [0.5, batch_interval].
            request_timeout (float): Per-request timeout in seconds passed to
                the underlying httpx AsyncClient.
            max_retries (int): Number of attempts per URL before marking it as
                failed.
            retry_delay (float): Upper bound in seconds for the randomized
                exponential back-off between retries.
            follow_redirects (bool): Whether the httpx client should follow HTTP
                301/302 redirects automatically. Enables transparent handling of
                out-of-range page redirects.
            http_conn_limit (int): Maximum number of simultaneous open TCP
                connections in the shared httpx connection pool. Keep this
                conservative to avoid overwhelming the target server.
        """
        self.concurrent_limit: int = concurrent_limit
        self.batch_interval: float = batch_interval
        self.request_timeout: float = request_timeout
        self.max_retries: int = max_retries
        self.retry_delay: float = retry_delay
        self.follow_redirects: bool = follow_redirects
        self.http_conn_limit: int = http_conn_limit

    @abstractmethod
    def iter_urls(self) -> Iterator[str]:
        """
        Returns an iterator of URLs to collect.
        The signature must be implemented in concrete classes.
        """
        raise NotImplementedError

    @abstractmethod
    async def crawl_urls(self, result_batchsize: int = 100000) -> None:
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

    def collect(self, *, runner=asyncio.run, **kwargs) -> None:
        """
        Synchronous entry point that wraps `crawl_urls` for callers that should not
        need to manage an event loop directly.

        `runner` can be replaced with a custom async runner (e.g. one that spawns
        a new event loop per thread when using concurrent.futures.ThreadPoolExecutor).
        """
        runner(self.crawl_urls(**kwargs))

    def _rand_batch_interval(self) -> float:
        return random.uniform(1, self.batch_interval)

    def _rand_retry_delay(self) -> float:
        return random.uniform(1, self.retry_delay)

    async def _crawl_batch(
        self, url_batch: list[str], client: AsyncClient
    ) -> list[UrlCollectorResult]:
        """
        Crawls all URLs in this batch. The number of URLs in this batch should
        conform to the concurrent limit.
        """
        tasks = [self._get_url_result(url=url, client=client) for url in url_batch]
        return await asyncio.gather(*tasks)

    async def _crawl_with_limit(
        self, urls: list[str]
    ) -> AsyncGenerator[list[UrlCollectorResult], None]:
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
                results = await self._crawl_batch(url_batch=batch, client=client)
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
