from tqdm import tqdm
from typing import Sequence
from minha_regiao.schema.City import City
from prefect import flow, task, get_run_logger

@task(name="Connect to VPN")
def connect_to_vpn():
    pass

@task(name="Disconnect from VPN")
def disconnect_from_vpn():
    pass

@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(town_hall_url: str) -> Sequence[str]:
    # Placeholder for actual crawling logic
    raise NotImplementedError("Crawling logic not implemented yet")


@flow(name="Fetch Candidate Files")
async def fetch_candidate_files(
    cities: Sequence[City]
):
    logger = get_run_logger()
    logger.info("Fetching candidate files...")

    for city in tqdm(cities, desc="Crawling Town Hall Websites"):
        websites = await crawl_town_hall_website(city.town_hall.website)