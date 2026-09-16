from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from scrapling.parser import Selector

from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.services.RMUEPageParser import parse_rmue_regulations


@task(name="parse_rmue_page", cache_policy=NO_CACHE)
async def parse_rmue_page_task(page: Selector) -> list[RMUERegulation]:
    logger = get_run_logger()

    entries = parse_rmue_regulations(page)

    logger.info(f"Parsed {len(entries)} municipalities from the RMUE page")
    return entries
