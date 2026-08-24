import asyncio

import httpx
from prefect import flow, task, get_run_logger
from scrapling.fetchers import AsyncStealthySession
from tqdm import tqdm

from minha_regiao.flows.extract_pdms.Settings import settings
from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult
from minha_regiao.flows.extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from minha_regiao.flows.extract_pdms.services.PDMRepository import find_cities_missing_pdm, persist_pdm_results
from minha_regiao.flows.extract_pdms.services.SiteCrawler import SiteBlockedError, find_pdm

# Tag applied to every city-crawl task run. Paired with a Prefect tag-based
# concurrency limit (see `ensure_concurrency_limit` below) so that "how many
# town hall sites we hit at once" is enforced by the Prefect server itself,
# not just by the local semaphore in `crawl_cities_for_pdm` -- the two
# mechanisms are deliberately kept in lockstep (see comment there) so a
# 403-triggering burst of crawls can't slip past the server-side limit.
CITY_CRAWL_TAG = "pdm-city-crawl"


@task(name="find_cities_missing_pdm")
async def find_pending_cities() -> list[CityWebsite]:
    logger = get_run_logger()

    cities = await find_cities_missing_pdm(settings.database_url)

    logger.info(f"Found {len(cities)} cities without a resolved PDM")
    return cities


@flow(name="crawl_cities_for_pdm")
async def crawl_cities_for_pdm(cities: list[CityWebsite]) -> int:
    logger = get_run_logger()

    city_concurrency = settings.city_concurrency
    # The browser's tab pool needs a slot for every page that can be in
    # flight at once: one city batch, each city fetching up to
    # site_concurrency pages of its own.
    max_pages = city_concurrency * settings.site_concurrency

    # Registers (upserts) the server-side Prefect concurrency limit that
    # caps CITY_CRAWL_TAG task runs. This alone would only make Prefect
    # *queue* excess task runs rather than reject them, but submitting all
    # `len(cities)` task runs at once (e.g. via asyncio.gather) still means
    # every one of them races to acquire a concurrency-slot lease
    # simultaneously, which can flood Prefect's lease-acquisition service
    # under a large batch. The asyncio.Semaphore below is sized to the same
    # limit so at most `city_concurrency` task runs are ever created at
    # once, keeping lease acquisition prompt instead of queued.
    await ensure_concurrency_limit(CITY_CRAWL_TAG, city_concurrency)
    semaphore = asyncio.Semaphore(city_concurrency)

    persisted = 0
    pending_results: list[PDMResult] = []

    async def flush() -> None:
        nonlocal persisted
        updated = await persist_pdm_results(settings.database_url, pending_results)
        persisted += updated
        logger.info(f"Persisted {updated}/{len(pending_results)} PDMs (running total: {persisted})")
        pending_results.clear()

    # One browser is shared across every city (its tab pool is sized to
    # max_pages above) since launching a stealth browser per city would be
    # far more expensive than a tab in an already-running one. `crawl_city`
    # closes over it instead of taking it as a task argument so Prefect
    # never has to hash a live session into a cache key. httpx clients are
    # cheap and pool connections per-host, which buys nothing shared across
    # cities that are all different hosts anyway, so each city just opens
    # its own.
    async with AsyncStealthySession(headless=True, network_idle=True, max_pages=max_pages) as session:

        @task(
            name="crawl_city_for_pdm",
            task_run_name="crawl-city-{city.website}",
            persist_result=False,
            tags=[CITY_CRAWL_TAG],
        )
        async def crawl_city(city: CityWebsite) -> PDMResult | None:
            logger = get_run_logger()

            try:
                async with httpx.AsyncClient() as http_client:
                    result = await find_pdm(
                        session,
                        city,
                        settings.max_pages_per_site,
                        settings.max_depth,
                        http_client,
                        settings.pdm_output_dir,
                        settings.min_pdf_pages,
                        settings.min_pdm_keyword_hits,
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

        async def crawl_with_limit(city: CityWebsite) -> PDMResult | None:
            async with semaphore:
                return await crawl_city(city)

        with tqdm(total=len(cities), desc="Crawling cities for PDM", unit="city") as progress:
            for coro in asyncio.as_completed([crawl_with_limit(city) for city in cities]):
                result = await coro
                if result is not None:
                    pending_results.append(result)

                if len(pending_results) >= city_concurrency:
                    await flush()

                progress.set_postfix(persisted=persisted)
                progress.update(1)

            if pending_results:
                await flush()
                progress.set_postfix(persisted=persisted)

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
