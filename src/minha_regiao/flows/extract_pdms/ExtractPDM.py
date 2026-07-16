import asyncio

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import AsyncStealthySession

from minha_regiao.flows.extract_pdms.Settings import settings
from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult
from minha_regiao.flows.extract_pdms.services.PDMRepository import find_cities_missing_pdm, persist_pdm_results
from minha_regiao.flows.extract_pdms.services.SiteCrawler import find_pdm

# How many town hall sites to crawl concurrently, and concurrently within
# that batch, through a single shared browser (as tabs in its page pool).
# Each batch is persisted before moving on to the next one, so a failure
# partway through only loses the in-flight batch, not everything resolved
# so far.
CITY_BATCH_SIZE = 8


@task(name="find_cities_missing_pdm")
async def find_pending_cities() -> list[CityWebsite]:
    logger = get_run_logger()

    cities = await find_cities_missing_pdm(settings.database_url)

    logger.info(f"Found {len(cities)} cities without a resolved PDM")
    return cities


async def _crawl_city(session: AsyncStealthySession, city: CityWebsite, logger) -> PDMResult | None:
    try:
        result = await find_pdm(session, city, settings.max_pages_per_site, settings.max_depth)
    except Exception:
        logger.warning(f"Failed to crawl {city.website}", exc_info=True)
        return None

    if result is None:
        logger.warning(f"No PDM found on {city.website}")

    return result


@task(name="crawl_cities_for_pdm", cache_policy=NO_CACHE)
async def crawl_cities_for_pdm(cities: list[CityWebsite]) -> int:
    logger = get_run_logger()

    persisted = 0
    async with AsyncStealthySession(headless=True, network_idle=True, max_pages=CITY_BATCH_SIZE) as session:
        for start in range(0, len(cities), CITY_BATCH_SIZE):
            batch = cities[start : start + CITY_BATCH_SIZE]

            resolved = await asyncio.gather(*(_crawl_city(session, city, logger) for city in batch))
            found = [result for result in resolved if result is not None]

            updated = await persist_pdm_results(settings.database_url, found)
            persisted += updated

            batch_number = start // CITY_BATCH_SIZE + 1
            logger.info(f"Batch {batch_number}: persisted {updated}/{len(batch)} PDMs")

    logger.info(f"Resolved and persisted {persisted}/{len(cities)} PDMs in total")
    return persisted


@flow(
    name="extract_pdms",
    description="Crawl each city's town hall website in parallel to find and resolve its PDM (Plano Diretor Municipal) regulation PDF.",
)
async def extract_pdms() -> int:
    cities = await find_pending_cities()
    return await crawl_cities_for_pdm(cities)


if __name__ == "__main__":
    asyncio.run(extract_pdms())
