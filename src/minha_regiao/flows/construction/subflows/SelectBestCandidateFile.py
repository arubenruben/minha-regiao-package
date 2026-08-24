import io
import json
import asyncio
import aiohttp
from tqdm import tqdm
from pathlib import Path
from pypdf import PdfReader
from typing import Sequence, Optional
from minha_regiao.schema.City import City
from prefect import flow, task, get_run_logger
from minha_regiao.scrapping.LocalScraperStrategy import LocalScraperStrategy


# Cache file configuration
CACHE_DIR = Path("/mnt/docker/minha-regiao-package/cache")
CACHE_FILE = CACHE_DIR / "best_candidates_cache.json"

# Global lock for file I/O operations
cache_lock = asyncio.Lock()


# Extended list of PDM-related keywords in Portuguese
PDM_KEYWORDS = [
    # Core PDM terms
    "pdm",
    "plano diretor municipal",
    "plano diretor",
    # Land use and zoning
    "solos",
    "ordenamento do território",
    "ordenamento territorial",
    "uso do solo",
    "ocupação do solo",
    "zonamento",
    "planta de ordenamento",
    "planta de condicionantes",
    # Municipal strategy
    "estratégia municipal",
    "gestão territorial",
    "desenvolvimento sustentável",
    "modelo territorial",
    # Regulations and legal framework
    "regulamento",
    "regulamento municipal",
    "rjigt",
    "regime jurídico dos instrumentos de gestão territorial",
    # Protected areas
    "reserva ecológica nacional",
    "ren",
    "reserva agrícola nacional",
    "ran",
    "estrutura ecológica",
    # Urban planning elements
    "urbanização",
    "edificação",
    "perímetro urbano",
    "áreas urbanas",
    "áreas de edificação consolidada",
    "servidão",
    "servidões administrativas",
    # Infrastructure and facilities
    "infraestruturas urbanas",
    "espaços verdes",
    "equipamentos coletivos",
    "equipamentos públicos",
    "rede viária",
    "mobilidade",
    # Economic and housing
    "habitação",
    "atividades económicas",
    "actividades económicas",
    # Planning units
    "unidades operativas de planeamento",
    "uop",
    "unidades de execução",
    # Related plans
    "prot",
    "plano regional de ordenamento do território",
]


async def load_cache() -> dict:
    """
    Load the cache file with lock protection.

    Returns:
        Dictionary mapping city names to their cached data
    """
    async with cache_lock:
        if not CACHE_FILE.exists():
            return {}

        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading cache: {e}")
            return {}


async def save_to_cache(city: City) -> None:
    """
    Save a single city result to the cache with lock protection.

    Args:
        city: City object to save to cache
    """
    async with cache_lock:
        # Load existing cache
        cache = {}
        if CACHE_FILE.exists():
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    cache = json.load(f)
            except Exception:
                cache = {}

        # Update cache with current city
        cache[city.name] = city.model_dump()

        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        # Save updated cache
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)


async def validate_pdf(url: str) -> tuple[bool, Optional[str]]:
    """
    Validates if a URL points to a valid PDF file with more than 5 pages and contains text.

    Args:
        url: URL to validate

    Returns:
        Tuple of (is_valid, reason) where is_valid is True if the PDF meets all criteria,
        and reason contains the failure reason if invalid
    """
    try:
        # Check if URL looks like a PDF
        if not url.lower().endswith(".pdf"):
            return False, "URL does not end with .pdf"

        # Download the PDF
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    return False, f"HTTP {response.status}"

                # Check Content-Type header
                content_type = response.headers.get("Content-Type", "").lower()
                if "pdf" not in content_type and content_type:
                    return False, f"Content-Type is {content_type}, not PDF"

                # Read PDF content
                pdf_content = await response.read()

        # Parse PDF
        pdf_file = io.BytesIO(pdf_content)
        reader = PdfReader(pdf_file)

        # Check number of pages
        num_pages = len(reader.pages)
        if num_pages <= 5:
            return False, f"Only {num_pages} pages (need >5)"

        # Check if PDF contains extractable text
        text_found = False
        for page in reader.pages[: min(3, num_pages)]:  # Check first 3 pages
            text = page.extract_text().strip()
            if len(text) > 100:  # Require at least 100 characters of text
                text_found = True
                break

        if not text_found:
            return False, "No extractable text found in PDF"

        return True, None

    except Exception as e:
        return False, f"Error: {str(e)}"


@task(name="Select Best Candidate File Task")
async def select_best_candidate_file(city: City) -> City:
    """
    Selects the best PDM candidate file based on keyword frequency.

    The best candidate is the one that has the most mentions of PDM-related keywords.
    Updates the city.town_hall.pdm_file with the selected URL.

    Args:
        city: City object with candidate files

    Returns:
        Updated City object with pdm_file set to the best candidate
    """
    logger = get_run_logger()

    if not city.town_hall.pdm_candidate_files:
        logger.warning(f"No PDM candidate files found for {city.name}")
        return city

    scraper = LocalScraperStrategy()
    best_url = None
    max_score = 0

    logger.info(
        f"Analyzing {len(city.town_hall.pdm_candidate_files)} candidate files for {city.name}"
    )

    for candidate_url in city.town_hall.pdm_candidate_files:
        try:
            # First, validate if it's a proper PDF file
            is_valid, reason = await validate_pdf(candidate_url)

            if not is_valid:
                logger.info(f"Skipping {candidate_url}: {reason}")
                continue

            logger.info(f"Valid PDF found: {candidate_url}")

            # Fetch the page content for keyword analysis
            soup = await scraper.query(candidate_url)

            if soup is None:
                logger.warning(f"Failed to fetch {candidate_url}")
                continue

            # Extract text content
            text_content = soup.get_text().lower()

            # Count keyword occurrences
            score = sum(text_content.count(keyword.lower()) for keyword in PDM_KEYWORDS)

            logger.info(f"URL: {candidate_url} - Score: {score}")

            if score > max_score:
                max_score = score
                best_url = candidate_url

        except Exception as e:
            logger.error(f"Error processing {candidate_url}: {str(e)}")
            continue

    if best_url:
        city.town_hall.pdm_file = best_url
        logger.info(
            f"Selected best candidate for {city.name}: {best_url} (score: {max_score})"
        )
    else:
        logger.warning(f"Could not select a best candidate for {city.name}")

    # Save to cache after processing
    await save_to_cache(city)

    return city


@flow(name="Select Best Candidate File")
async def find_best_candidates(cities: Sequence[City]) -> Sequence[City]:
    logger = get_run_logger()
    logger.info(f"Finding best candidates for {len(cities)} cities")

    # Load existing cache
    cache = await load_cache()
    logger.info(f"Loaded cache with {len(cache)} entries")

    updated_cities = []

    for city in tqdm(cities, desc="Finding best candidates"):
        # Check if city is already in cache
        if city.name in cache:
            logger.info(f"Loading {city.name} from cache")
            cached_city = City.model_validate(cache[city.name])
            updated_cities.append(cached_city)
        else:
            logger.info(f"Processing {city.name}")
            updated_city = await select_best_candidate_file(city)
            updated_cities.append(updated_city)

    logger.info(f"Completed processing. Total cities: {len(updated_cities)}")
    return updated_cities


if __name__ == "__main__":
    # /mnt/docker/minha-regiao-package/cache/candidate_files.json
    # Example usage
    cities = []  # This should be populated with actual City instances

    with open(
        "/mnt/docker/minha-regiao-package/cache/candidate_files.json",
        "r",
        encoding="utf-8",
    ) as f:
        cities_data = json.load(f)

        for city_data in cities_data:
            city = City.model_validate(city_data)  # Assuming a from_dict method exists
            cities.append(city)

    print(f"Loaded {len(cities)} cities from candidate_files.json")

    updated_cities = asyncio.run(find_best_candidates(cities))

    print(f"Updated {len(updated_cities)} cities with best candidate files")
