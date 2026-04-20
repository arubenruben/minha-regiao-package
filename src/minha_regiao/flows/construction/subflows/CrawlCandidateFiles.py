import asyncio
from tqdm import tqdm
from minha_regiao.schema.City import City
from urllib.parse import urlparse, urljoin
from typing import Sequence, Set, List, Tuple
from prefect import flow, task, get_run_logger
from minha_regiao.flows.construction.Settings import Settings
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy

settings = Settings()

PDM_KEYWORDS = [
    "pdm",
    "plano diretor municipal",
    "plano_diretor",
    "pdm_municipal",
    "plano-diretor",
    "estratégia municipal",
    "ordenamento do território",
]


@task(name="Check if PDM File")
def is_pdm_file(url: str) -> bool:
    """Check if a URL points to a potential PDM file."""
    if not url.lower().endswith(".pdf"):
        return False

    filename = urlparse(url).path.lower().strip()

    return any(keyword.strip().lower() in filename for keyword in PDM_KEYWORDS)


@task(name="Check if URL is File")
def is_file_url(url: str) -> bool:
    """Check if a URL points to a file (has a non-HTML extension)."""
    path = urlparse(url).path.lower()

    # Check if path has an extension
    if "." not in path.split("/")[-1]:
        return False

    # HTML-like extensions that should be crawled
    html_extensions = (".html", ".htm", ".php", ".asp", ".aspx", ".jsp")

    # If it has an extension that's not HTML-like, it's a file
    return not any(path.endswith(ext) for ext in html_extensions)


@task(name="Crawl Single Page")
async def crawl_single_page(
    url: str, domain: str, scraper: ScraperStrategy
) -> Tuple[List[str], List[str]]:
    """Crawl a single page and return lists of (same_domain_links, pdm_files)."""
    logger = get_run_logger()

    try:
        soup = await scraper.query(url)
        links = [a.get("href") for a in soup.find_all("a", href=True)]

        same_domain_links = []
        pdm_files = []

        for link in tqdm(links, desc=f"Crawling {url}", leave=False):
            if not isinstance(link, str):
                continue

            logger.debug(f"Processing link: {link} from {url}")

            full_url = urljoin(url, link)
            parsed_link = urlparse(full_url)

            # Check if this is a PDM file
            if is_pdm_file(full_url):
                logger.info(f"Found candidate PDM file: {full_url}")
                pdm_files.append(full_url)

            # Collect links from same domain for further crawling (but not files)
            elif parsed_link.netloc == domain and not is_file_url(full_url):
                same_domain_links.append(full_url)

        logger.debug(
            f"Crawled {url}: found {len(same_domain_links)} links, {len(pdm_files)} PDM files"
        )
        return same_domain_links, pdm_files

    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return [], []


@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(
    town_hall_url: str, scraper: ScraperStrategy
) -> Sequence[str]:
    domain = urlparse(town_hall_url).netloc
    visited: Set[str] = set()
    to_visit: Set[str] = {town_hall_url}
    found_files: Set[str] = set()

    # Crawl pages until no new pages to visit
    while to_visit:
        # Get batch of URLs to process
        current_batch = list(to_visit)
        to_visit.clear()
        visited.update(current_batch)

        # Crawl all URLs in batch concurrently
        results = await asyncio.gather(
            *[crawl_single_page(url, domain, scraper) for url in current_batch]
        )

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
    cities: Sequence[City],
    scraper: ScraperStrategy,
):
    logger = get_run_logger()
    logger.info("Fetching candidate files...")

    for city in tqdm(cities, desc="Crawling Town Hall Websites"):
        websites = await crawl_town_hall_website(
            town_hall_url=city.town_hall.website, scraper=scraper
        )

        if websites:
            logger.info(f"Found {len(websites)} candidate files for {city.name}")
            break
        else:
            logger.info(f"No candidate files found for {city.name}")
