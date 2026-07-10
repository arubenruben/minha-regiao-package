from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.sub_flows.regional_elections.services.regional_election_service import (
    build_regional_election,
    filter_regional_file_hrefs,
)
from minha_regiao.flows.extract_election_files.sub_flows.regional_elections.Settings import (
    settings,
)


@task(name="fetch_regional_page")
def fetch_regional_page(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching regional elections page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched regional elections page")
    return page


@task(name="extract_regional_file_links", cache_policy=NO_CACHE)
def extract_regional_file_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("a[href$='.xlsx'], a[href$='.xls']")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
    matched = filter_regional_file_hrefs(hrefs)

    logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to regional election files")
    return matched


@task(name="build_regional_elections")
def build_regional_elections(hrefs: list[str], base_url: str) -> list[Election]:
    logger = get_run_logger()

    elections = [build_regional_election(href, base_url) for href in hrefs]

    logger.info(f"Built {len(elections)} regional election records")
    return elections


@flow(name="regional_elections")
def regional_elections(url: str) -> list[Election]:
    logger = get_run_logger()

    page = fetch_regional_page(url)
    hrefs = extract_regional_file_links(page)
    elections = build_regional_elections(hrefs, url)

    logger.info(f"Found {len(elections)} regional election files at {url}")

    return elections


if __name__ == "__main__":
    regional_elections(settings.base_url)
