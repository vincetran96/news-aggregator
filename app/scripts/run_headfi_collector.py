"""
Script that runs the HeadFiURLCollector against a set of Head-Fi threads.
"""
from src.collectors.url.base import BaseUrlCollector
from src.collectors.url.concretes.headfi import HeadFiThread, HeadFiURLCollector


def main() -> None:
    threads = [
        HeadFiThread(
            base_url="https://www.head-fi.org/threads/the-canjam-new-york-2026-impressions-thread-march-7-8-2026.979675/page-{i}",
            start_i=1,
            end_i=20,
        ),
    ]

    collector: BaseUrlCollector = HeadFiURLCollector(
        threads=threads,
        output_dir="output/headfi",
        concurrent_limit=5,
        batch_interval=10.0,
        request_timeout=30.0,
        max_retries=3,
        retry_delay=5.0,
    )

    collector.run(result_batchsize=10)


if __name__ == "__main__":
    main()
