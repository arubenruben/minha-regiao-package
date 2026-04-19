import asyncio
from tqdm import tqdm
from minha_regiao.schema.City import City
from typing import Sequence, Optional, Set, List, Tuple
from urllib.parse import urlparse, urljoin
from prefect import flow, task, get_run_logger
from minha_regiao.scrapping.SmartProxy import SmartProxy
from minha_regiao.flows.construction.Settings import Settings

settings = Settings()

PDM_KEYWORDS = [
    "pdm", 
    "plano diretor municipal", 
    "plano_diretor", 
    "pdm_municipal", 
    "plano-diretor",
    "estratégia municipal",
    "ordenamento do território"
]


@task(name="Check if PDM File")
def is_pdm_file(url: str) -> bool:
    """Check if a URL points to a potential PDM file."""
    if not url.lower().endswith(".pdf"):
        return False
    
    filename = urlparse(url).path.lower()
    return any(keyword in filename for keyword in PDM_KEYWORDS)


@task(name="Crawl Single Page")
async def crawl_single_page(url: str, domain: str, scraper: SmartProxy) -> Tuple[List[str], List[str]]:
    """Crawl a single page and return lists of (same_domain_links, pdm_files)."""
    logger = get_run_logger()
    
    try:
        soup = await scraper.query(url)
        links = [a.get("href") for a in soup.find_all("a", href=True)]
        
        same_domain_links = []
        pdm_files = []
        
        for link in links:
            if not isinstance(link, str):
                continue
                
            full_url = urljoin(url, link)
            parsed_link = urlparse(full_url)
            
            # Collect links from same domain for further crawling
            if parsed_link.netloc == domain:
                same_domain_links.append(full_url)
            
            # Check if this is a PDM file
            if is_pdm_file(full_url):
                pdm_files.append(full_url)
        
        logger.debug(f"Crawled {url}: found {len(same_domain_links)} links, {len(pdm_files)} PDM files")
        return same_domain_links, pdm_files
        
    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return [], []


async def crawl_with_limit(url: str, domain: str, scraper: SmartProxy, semaphore: asyncio.Semaphore):
    async with semaphore:
        return await crawl_single_page(url, domain, scraper)


@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(
    town_hall_url: str,
    scraper: Optional[SmartProxy] = None,
    max_concurrency: int = 5
) -> Sequence[str]:
    
    if not scraper:
        scraper = SmartProxy(api_key=settings.smart_proxy_api_key)

    domain = urlparse(town_hall_url).netloc
    visited: Set[str] = set()
    to_visit: Set[str] = {town_hall_url}
    found_files: Set[str] = set()
    semaphore = asyncio.Semaphore(max_concurrency)
    
    # Crawl pages until no new pages to visit
    while to_visit:
        # Get batch of URLs to process
        current_batch = list(to_visit)
        to_visit.clear()
        visited.update(current_batch)
        
        # Crawl all URLs in batch concurrently
        results = await asyncio.gather(*[crawl_with_limit(url, domain, scraper, semaphore) for url in current_batch])
        
        # Process results
        for new_links, pdm_files in results:
            # Add new PDM files
            found_files.update(pdm_files)
            
            # Queue unvisited links for next batch
            for link in new_links:
                if link not in visited:
                    to_visit.add(link)

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

