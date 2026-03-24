from dataclasses import dataclass


@dataclass
class HeadFiCrawlThread:
    """
    Represents a Head-Fi forum thread to be crawled.
    Pages are queried inclusively from start_i to end_i.

    Used by: HeadFiURLCollector, HeadFiCrawlCoordinator
    """

    base_url: str
    start_i: int
    end_i: int


@dataclass
class UrlCrawlMeta:
    """
    Metadata associated with a single requested URL, recorded at crawl time.

    Attributes:
      thread_base_url: the URL template for the thread this page belongs to (e.g. "https://www.head-fi.org/.../page-{i}")
      page_num: the page index that was requested

    Used by: HeadFiURLCollector (to build the mapping), upsert_raw_pages (to persist)
    """

    thread_base_url: str
    page_num: int
