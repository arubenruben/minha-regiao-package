
import asyncio
from prefect import flow, task, get_run_logger
from minha_regiao.flows.construction.subflows.FetchTownHallWebsites import get_cities
from minha_regiao.flows.construction.subflows.CrawlCandidateFiles import fetch_candidate_files


@flow(name="Fetch PDMs")
async def fetch_pdms():
    cities = get_cities()
    pdm_files = await fetch_candidate_files(cities)

if __name__ == "__main__":
    asyncio.run(fetch_pdms())