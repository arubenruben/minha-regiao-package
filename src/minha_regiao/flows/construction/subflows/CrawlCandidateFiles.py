import asyncio
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from minha_regiao.schema.City import City
from typing import Sequence, Optional, Set
from urllib.parse import urlparse, urljoin
from prefect import flow, task, get_run_logger
from minha_regiao.scrapping.SmartProxy import SmartProxy
from minha_regiao.flows.construction.Settings import Settings

settings = Settings()

@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(
    town_hall_url: str,
    scraper: Optional[SmartProxy] = None,
    max_concurrency: int = 5
) -> Sequence[str]:
    if not scraper:
        scraper = SmartProxy(
            api_key=settings.smart_proxy_api_key,
        )

    domain = urlparse(town_hall_url).netloc
    visited: Set[str] = set()
    queue = asyncio.Queue()
    await queue.put(town_hall_url)
    found_files: Set[str] = set()
    
    # Limit concurrency per domain to avoid getting blocked
    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_url(url: str):
        nonlocal found_files
        async with semaphore:
            try:
                soup = await scraper.query(url)
                
                links = [a.get("href") for a in soup.find_all("a", href=True)]
                
                for link in links:
                    if not isinstance(link, str):
                        continue
                    full_url = urljoin(url, link)
                    parsed_link = urlparse(full_url)
                    
                    if parsed_link.netloc == domain and full_url not in visited:
                        await queue.put(full_url)
                    
                    # PDM File Filtering
                    if not full_url.lower().endswith(".pdf"):
                        continue
                        
                    filename = parsed_link.path.lower()
                    pdm_keywords = [
                        "pdm", 
                        "plano diretor municipal", 
                        "plano_diretor", 
                        "pdm_municipal", 
                        "plano-diretor",
                        "estratégia municipal",
                        "ordenamento do território"
                    ]
                    if any(kw in filename for kw in pdm_keywords):
                        found_files.add(full_url)
            except Exception as e:
                get_run_logger().error(f"Error crawling {url}: {e}")

    # Process the queue until empty
    while not queue.empty():
        current_url = await queue.get()
        if current_url in visited:
            queue.task_done()
            continue
        
        visited.add(current_url)
        await process_url(current_url)
        queue.task_done()

    return list(found_files)


@flow(name="Fetch Candidate Files")
async def fetch_candidate_files(
    cities: Sequence[City]
):
    logger = get_run_logger()
    logger.info("Fetching candidate files...")

    for city in tqdm(cities, desc="Crawling Town Hall Websites"):
        websites = await crawl_town_hall_website(city.town_hall.website)
        
        if websites:
            print(websites)
            logger.info(f"Found {len(websites)} candidate files for {city.name}")
            break
        else:
            logger.info(f"No candidate files found for {city.name}")

