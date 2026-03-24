from typing import NamedTuple


class HeadFiThread(NamedTuple):
    base_url: str
    export_prefix: str


THREADS: list[HeadFiThread] = [
    HeadFiThread(
        base_url=(
            "https://www.head-fi.org/threads/"
            "the-canjam-new-york-2026-impressions-thread-march-7-8-2026.979675/page-{i}"
        ),
        export_prefix="canjam_ny_2026_",
    ),
]
