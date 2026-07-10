from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.sub_flows.presidential_elections.services.presidential_election_service import (
    build_presidential_election,
    filter_presidential_file_hrefs,
)
from minha_regiao.flows.extract_election_files.sub_flows.presidential_elections.Settings import (
    settings,
)


@task(name="fetch_presidential_page")
def fetch_presidential_page(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching presidential elections page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched presidential elections page")
    return page


@task(name="extract_presidential_file_links", cache_policy=NO_CACHE)
def extract_presidential_file_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("a[href$='.xlsx'], a[href$='.xls']")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
    matched = filter_presidential_file_hrefs(hrefs)

    logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to presidential election files")
    return matched


@task(name="build_presidential_elections")
def build_presidential_elections(hrefs: list[str], base_url: str) -> list[Election]:
    logger = get_run_logger()

    elections = [build_presidential_election(href, base_url) for href in hrefs]

    logger.info(f"Built {len(elections)} presidential election records")
    return elections


@flow(name="presidential_elections")
def presidential_elections(url: str) -> list[Election]:
    logger = get_run_logger()

    page = fetch_presidential_page(url)
    hrefs = extract_presidential_file_links(page)
    elections = build_presidential_elections(hrefs, url)

    logger.info(f"Found {len(elections)} presidential election files at {url}")

    return elections


if __name__ == "__main__":
    presidential_elections(settings.base_url)
