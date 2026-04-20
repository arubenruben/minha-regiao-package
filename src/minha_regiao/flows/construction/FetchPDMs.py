import asyncio
from prefect import flow, task, get_run_logger
from minha_regiao.flows.construction.Settings import Settings
from minha_regiao.scrapping.SmartProxyStrategy import SmartProxyStrategy
from minha_regiao.flows.construction.subflows.FetchTownHallWebsites import get_cities
from minha_regiao.flows.construction.subflows.CrawlCandidateFiles import (
    fetch_candidate_files,
)

settings = Settings()


@flow(name="Fetch PDMs")
async def fetch_pdms():
    cities = get_cities()

    crawler_strategy = SmartProxyStrategy(api_key=settings.smart_proxy_api_key)

    pdm_files = await fetch_candidate_files(cities=cities, scraper=crawler_strategy)


if __name__ == "__main__":
    asyncio.run(fetch_pdms())
