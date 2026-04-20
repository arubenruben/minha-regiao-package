import os
import json
import asyncio
from tqdm import tqdm
from dataclasses import dataclass, field
from minha_regiao.schema.City import City
from prefect import flow, task, get_run_logger
from urllib.parse import urlparse, urljoin, urlunparse
from typing import Sequence, Set, List, Tuple, Optional, Any
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = CURRENT_DIR.split("src")[0]
CACHE_DIR = os.path.join(ROOT_DIR, "cache")

PDM_KEYWORDS = [
    "pdm",
    "plano diretor municipal",
    "plano_diretor",
    "pdm_municipal",
    "plano-diretor",
    "estratégia municipal",
    "ordenamento do território",
]


def normalize_url(url: str) -> str:
    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    if path != "/":
        path = path.rstrip("/")

    # Remove fragment, keep query
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def is_pdm_file(url: str) -> bool:
    path = urlparse(url).path.lower()

    if not path.endswith(".pdf"):
        return False

    return any(keyword in path for keyword in PDM_KEYWORDS)


def is_file_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    last = path.split("/")[-1]

    if "." not in last:
        return False

    html_extensions = (".html", ".htm", ".php", ".asp", ".aspx", ".jsp")
    return not any(path.endswith(ext) for ext in html_extensions)


async def crawl_single_page(
    url: str,
    domain: str,
    scraper: ScraperStrategy,
    logger: Any,
) -> Tuple[List[str], List[str]]:
    try:
        logger.info(f"Crawling {url} for candidate files...")

        soup = await scraper.query(url)

        if soup is None:
            logger.warning(f"Received no content for {url}")
            return [], []

        same_domain_links: Set[str] = set()
        pdm_files: Set[str] = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not isinstance(href, str):
                continue

            full_url = normalize_url(urljoin(url, href))
            parsed = urlparse(full_url)

            if parsed.netloc != domain:
                continue

            if is_pdm_file(full_url):
                logger.info(f"Found candidate PDM file: {full_url}")
                pdm_files.add(full_url)
            elif not is_file_url(full_url):
                same_domain_links.add(full_url)

        return list(same_domain_links), list(pdm_files)

    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return [], []


@dataclass
class CrawlState:
    domain: str
    scraper: ScraperStrategy
    logger: Any
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    seen: Set[str] = field(default_factory=set)
    found_files: Set[str] = field(default_factory=set)


async def crawl_worker(worker_id: int, state: CrawlState) -> None:
    while True:
        url: Optional[str] = await state.queue.get()

        if url is None:
            state.queue.task_done()
            return

        try:
            new_links, pdm_files = await crawl_single_page(
                url=url,
                domain=state.domain,
                scraper=state.scraper,
                logger=state.logger,
            )

            state.found_files.update(pdm_files)

            for link in new_links:
                if link not in state.seen:
                    state.seen.add(link)
                    await state.queue.put(link)

        except Exception as e:
            state.logger.error(f"Worker {worker_id} failed on {url}: {e}")
        finally:
            state.queue.task_done()


@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(
    town_hall_url: str,
    scraper: ScraperStrategy,
    max_concurrency: int = 128,
) -> Sequence[str]:
    logger = get_run_logger()

    start_url = normalize_url(town_hall_url)
    domain = urlparse(start_url).netloc.lower()

    state = CrawlState(
        domain=domain,
        scraper=scraper,
        logger=logger,
    )

    state.seen.add(start_url)
    await state.queue.put(start_url)

    workers = [
        asyncio.create_task(crawl_worker(i, state)) for i in range(max_concurrency)
    ]

    await state.queue.join()

    for _ in workers:
        await state.queue.put(None)

    await asyncio.gather(*workers)

    return sorted(state.found_files)


@task(name="Read Cache File")
async def read_cache_file(cache_path: str) -> Sequence[City]:
    if not os.path.exists(cache_path):
        return []

    with open(cache_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return [City.model_validate(item) for item in data]


@task(name="Write Cache File")
async def write_cache_file(cache_path: str, city: City, lock: asyncio.Lock) -> None:
    async with lock:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        data = []

        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError):
                data = []

        data.append(city.model_dump())

        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


async def process_single_city(
    city: City,
    scraper: ScraperStrategy,
    cache_lock: asyncio.Lock,
    logger: Any,
) -> City:
    """Process a single city: crawl website and update cache."""
    websites = await crawl_town_hall_website(
        town_hall_url=city.town_hall.website,
        scraper=scraper,
        max_concurrency=128,
    )

    if websites:
        logger.info(f"Found {len(websites)} candidate files for {city.name}")
    else:
        logger.info(f"No candidate files found for {city.name}")

    city.town_hall.pdm_candidate_files = websites

    # Update cache after each city to ensure progress is saved
    # Protected by lock to prevent race conditions
    await write_cache_file(
        os.path.join(CACHE_DIR, "candidate_files.json"),
        city,
        cache_lock,
    )

    return city


@flow(name="Fetch Candidate Files")
async def fetch_candidate_files(
    cities: Sequence[City],
    scraper: ScraperStrategy,
) -> Sequence[City]:
    logger = get_run_logger()
    logger.info("Fetching candidate files...")

    cached_cities = await read_cache_file(
        os.path.join(CACHE_DIR, "candidate_files.json")
    )
    # Filter out cities that are already cached
    cached_city_names = {city.name for city in cached_cities}
    cities_to_process = [city for city in cities if city.name not in cached_city_names]

    logger.info(
        f"Filtered out {len(cached_city_names)} cached cities, {len(cities_to_process)} remaining to process"
    )

    # Create lock for cache writing protection
    cache_lock = asyncio.Lock()

    # Process cities in parallel
    await asyncio.gather(
        *[
            process_single_city(city, scraper, cache_lock, logger)
            for city in cities_to_process
        ]
    )

    return cities
