"""
Exports Head-Fi post content from headfi.post_content to a plain-text file.
"""

from src.common.constants import DATETIME_FILENAME_FORMAT
from src.exporters.headfi.post_content import export_to_file
from src.storage.db import get_connection
from src.utils.datetime import now_utc

OUTPUT_DIR = "output/headfi"

THREADS: list[tuple[str, str]] = [
    (
        "https://www.head-fi.org/threads/"
        "the-canjam-new-york-2026-impressions-thread-march-7-8-2026.979675/page-{i}",
        "canjam_ny_2026_",
    ),
]


def main() -> None:
    timestamp = now_utc().strftime(DATETIME_FILENAME_FORMAT)

    with get_connection() as conn:
        for thread_base_url, prefix in THREADS:
            export_to_file(
                conn,
                path=f"{OUTPUT_DIR}/{prefix}{timestamp}",
                thread_base_url=thread_base_url,
            )


if __name__ == "__main__":
    main()
