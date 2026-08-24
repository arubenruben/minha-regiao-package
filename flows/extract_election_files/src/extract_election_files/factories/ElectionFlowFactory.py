from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import StealthyFetcher

from extract_election_files.schema.Election import Election
from extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher


class ElectionFlowFactory:
    """Builds the standard fetch-page -> extract-links -> build-elections Prefect flow for a single election source."""

    def __init__(self, domain: str, matcher: ElectionFileMatcher):
        self._domain = domain
        self._matcher = matcher

    def build(self):
        domain = self._domain
        matcher = self._matcher

        @task(name=f"fetch_{domain}_page")
        async def fetch_page(url: str):
            logger = get_run_logger()
            logger.info(f"Fetching {domain} elections page from {url}")

            page = await StealthyFetcher.async_fetch(url, headless=True, network_idle=True)

            logger.info(f"Fetched {domain} elections page")
            return page

        @task(name=f"extract_{domain}_file_links", cache_policy=NO_CACHE)
        def extract_file_links(page) -> list[str]:
            logger = get_run_logger()

            links = page.css("a[href$='.xlsx'], a[href$='.xls']")
            hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
            matched = matcher.filter_hrefs(hrefs)

            logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to {domain} election files")
            return matched

        @task(name=f"build_{domain}_elections")
        def build_elections(hrefs: list[str], base_url: str) -> list[Election]:
            logger = get_run_logger()

            elections = [matcher.build_election(href, base_url) for href in hrefs]

            logger.info(f"Built {len(elections)} {domain} election records")
            return elections

        @flow(name=f"{domain}_elections")
        async def election_flow(url: str) -> list[Election]:
            logger = get_run_logger()

            page = await fetch_page(url)
            hrefs = extract_file_links(page)
            elections = build_elections(hrefs, url)

            logger.info(f"Found {len(elections)} {domain} election files at {url}")

            return elections

        return election_flow
