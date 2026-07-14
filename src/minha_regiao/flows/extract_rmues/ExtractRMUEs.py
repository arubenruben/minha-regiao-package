import asyncio

from prefect import flow, task, get_run_logger
from scrapling.fetchers import StealthyFetcher
from scrapling.parser import Selector

from minha_regiao.flows.extract_rmues.Settings import settings
from minha_regiao.flows.extract_rmues.schema.RMUERegulation import RMUERegulation
from minha_regiao.flows.extract_rmues.services.RMUEPageParser import parse_rmue_regulations
from minha_regiao.flows.extract_rmues.services.RMURepository import persist_rmue_regulations


@task(name="fetch_rmue_page")
def fetch_rmue_page(url: str) -> Selector:
    logger = get_run_logger()
    logger.info(f"Fetching RMUE page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched RMUE page")
    return page


@task(name="parse_rmue_page")
def parse_rmue_page(page: Selector) -> list[RMUERegulation]:
    logger = get_run_logger()

    entries = parse_rmue_regulations(page)

    logger.info(f"Parsed {len(entries)} municipalities from the RMUE page")
    return entries


@task(name="persist_rmue_regulations")
def persist_rmue_page(entries: list[RMUERegulation]) -> int:
    logger = get_run_logger()

    persisted, unmatched_cities, skipped_documents = asyncio.run(
        persist_rmue_regulations(settings.database_url, entries)
    )

    logger.info(f"Persisted {persisted} regulation documents")
    if unmatched_cities:
        logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")
    if skipped_documents:
        logger.warning(f"Skipped documents (no parseable year): {sorted(skipped_documents)}")

    return persisted


@flow(
    name="extract_rmues",
    description="Extract and index RMUE and municipal fee regulations from the Diário da República website by city.",
)
def extract_rmues(base_url: str) -> int:
    page = fetch_rmue_page(base_url)
    entries = parse_rmue_page(page)
    return persist_rmue_page(entries)


if __name__ == "__main__":
    extract_rmues(base_url=settings.rmue_url)
