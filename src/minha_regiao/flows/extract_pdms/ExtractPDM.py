import asyncio

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import AsyncStealthySession

from minha_regiao.flows.extract_pdms.Settings import settings
from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult
from minha_regiao.flows.extract_pdms.services.PDMRepository import find_cities_missing_pdm, persist_pdm_results
from minha_regiao.flows.extract_pdms.services.SiteCrawler import SiteBlockedError, find_pdm


@task(name="find_cities_missing_pdm")
async def find_pending_cities() -> list[CityWebsite]:
    logger = get_run_logger()

    cities = await find_cities_missing_pdm(settings.database_url)

    logger.info(f"Found {len(cities)} cities without a resolved PDM")
    return cities


async def _crawl_city(session: AsyncStealthySession, city: CityWebsite, logger) -> PDMResult | None:
    try:
        result = await find_pdm(
            session,
            city,
            settings.max_pages_per_site,
            settings.max_depth,
            site_concurrency=settings.site_concurrency,
            request_delay_seconds=settings.site_request_delay_seconds,
            max_retries_on_403=settings.max_retries_on_403,
            retry_backoff_seconds=settings.retry_backoff_seconds,
        )
    except SiteBlockedError as error:
        logger.error(f"Giving up on {city.website}: {error}")
        return None
    except Exception:
        logger.warning(f"Failed to crawl {city.website}", exc_info=True)
        return None

    if result is None:
        logger.warning(f"No PDM found on {city.website}")

    return result


@task(name="crawl_cities_for_pdm", cache_policy=NO_CACHE)
async def crawl_cities_for_pdm(cities: list[CityWebsite]) -> int:
    logger = get_run_logger()

    city_concurrency = settings.city_concurrency
    # The browser's tab pool needs a slot for every page that can be in
    # flight at once: one city batch, each city fetching up to
    # site_concurrency pages of its own.
    max_pages = city_concurrency * settings.site_concurrency

    persisted = 0
    async with AsyncStealthySession(headless=True, network_idle=True, max_pages=max_pages) as session:
        for start in range(0, len(cities), city_concurrency):
            batch = cities[start : start + city_concurrency]

            resolved = await asyncio.gather(*(_crawl_city(session, city, logger) for city in batch))
            found = [result for result in resolved if result is not None]

            updated = await persist_pdm_results(settings.database_url, found)
            persisted += updated

            batch_number = start // city_concurrency + 1
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
