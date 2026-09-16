from prefect import get_run_logger, task
from scrapling.fetchers import StealthyFetcher
from scrapling.parser import Selector


@task(name="fetch_rmue_page")
async def fetch_rmue_page_task(url: str) -> Selector:
    logger = get_run_logger()
    logger.info(f"Fetching RMUE page from {url}")

    page = await StealthyFetcher.async_fetch(url, headless=True, network_idle=True)

    logger.info("Fetched RMUE page")
    return page
