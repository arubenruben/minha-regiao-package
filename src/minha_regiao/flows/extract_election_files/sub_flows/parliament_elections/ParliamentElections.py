from prefect.cache_policies import NO_CACHE
from prefect import flow, task, get_run_logger
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.sub_flows.parliament_elections.services.parliament_election_service import (
    build_parliament_election,
    filter_parliament_file_hrefs,
)
from minha_regiao.flows.extract_election_files.sub_flows.parliament_elections.Settings import (
    settings,
)


@task(name="fetch_parliament_page")
def fetch_parliament_page(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching parliament elections page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched parliament elections page")
    return page


@task(name="extract_parliament_file_links", cache_policy=NO_CACHE)
def extract_parliament_file_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("a[href$='.xlsx'], a[href$='.xls']")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
    matched = filter_parliament_file_hrefs(hrefs)

    logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to parliament election files")
    return matched


@task(name="build_parliament_elections")
def build_parliament_elections(hrefs: list[str], base_url: str) -> list[Election]:
    logger = get_run_logger()

    elections = [build_parliament_election(href, base_url) for href in hrefs]

    logger.info(f"Built {len(elections)} parliament election records")
    return elections


@flow(name="parliament_elections")
def parliament_elections(url: str) -> list[Election]:
    logger = get_run_logger()

    page = fetch_parliament_page(url)
    hrefs = extract_parliament_file_links(page)
    elections = build_parliament_elections(hrefs, url)

    logger.info(f"Found {len(elections)} parliament election files at {url}")

    return elections


if __name__ == "__main__":
    parliament_elections(settings.base_url)
