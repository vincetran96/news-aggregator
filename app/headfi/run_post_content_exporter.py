"""
Exports Head-Fi post content from headfi.post_content to a plain-text file.
"""

from app.headfi.threads import THREADS
from src.common.constants import DATETIME_FILENAME_FORMAT
from src.exporters.headfi.post_content import export_to_file
from src.storage.db import get_connection
from src.utils.datetime import now_utc

OUTPUT_DIR = "output/headfi"


def main() -> None:
    timestamp = now_utc().strftime(DATETIME_FILENAME_FORMAT)

    with get_connection() as conn:
        for thread in THREADS:
            export_to_file(
                conn,
                path=f"{OUTPUT_DIR}/{thread.export_prefix}{timestamp}",
                thread_base_url=thread.base_url,
            )


if __name__ == "__main__":
    main()
