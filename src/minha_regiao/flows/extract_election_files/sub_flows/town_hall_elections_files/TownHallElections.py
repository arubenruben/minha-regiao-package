from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.sub_flows.town_hall_elections_files.services.town_hall_election_service import (
    build_town_hall_election,
    filter_town_hall_file_hrefs,
)
from minha_regiao.flows.extract_election_files.sub_flows.town_hall_elections_files.Settings import (
    settings,
)


@task(name="fetch_town_hall_page")
def fetch_town_hall_page(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching town hall elections page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched town hall elections page")
    return page


@task(name="extract_town_hall_file_links", cache_policy=NO_CACHE)
def extract_town_hall_file_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("a[href$='.xlsx'], a[href$='.xls']")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]
    matched = filter_town_hall_file_hrefs(hrefs)

    logger.info(f"Matched {len(matched)}/{len(hrefs)} spreadsheet links to town hall election files")
    return matched


@task(name="build_town_hall_elections")
def build_town_hall_elections(hrefs: list[str], base_url: str) -> list[Election]:
    logger = get_run_logger()

    elections = [build_town_hall_election(href, base_url) for href in hrefs]

    logger.info(f"Built {len(elections)} town hall election records")
    return elections


@flow(name="town_hall_elections")
def town_hall_elections(url: str) -> list[Election]:
    logger = get_run_logger()

    page = fetch_town_hall_page(url)
    hrefs = extract_town_hall_file_links(page)
    elections = build_town_hall_elections(hrefs, url)

    logger.info(f"Found {len(elections)} town hall election files at {url}")

    return elections


if __name__ == "__main__":
    town_hall_elections(settings.base_url)
