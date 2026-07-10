from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.services.european_election_service import (
    build_european_election,
    filter_european_file_hrefs,
)
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.Settings import (
    settings,
)


@task(name="fetch_european_page")
def fetch_european_page(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching european elections page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched european elections page")
    return page


@task(name="extract_european_file_links", cache_policy=NO_CACHE)
def extract_european_file_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("a[href$='.xlsx'], a[href$='.xls']")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
    matched = filter_european_file_hrefs(hrefs)

    logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to european election files")
    return matched


@task(name="build_european_elections")
def build_european_elections(hrefs: list[str], base_url: str) -> list[Election]:
    logger = get_run_logger()

    elections = [build_european_election(href, base_url) for href in hrefs]

    logger.info(f"Built {len(elections)} european election records")
    return elections


@flow(name="european_elections")
def european_elections(url: str) -> list[Election]:
    logger = get_run_logger()

    page = fetch_european_page(url)
    hrefs = extract_european_file_links(page)
    elections = build_european_elections(hrefs, url)

    logger.info(f"Found {len(elections)} european election files at {url}")

    return elections


if __name__ == "__main__":
    european_elections(settings.base_url)
