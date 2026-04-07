import os
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from prefect import flow, task

from minha_regiao.flows.file_fetching.construction.schema.TownHallDTO import TownHallDTO


@task(name="Load HTML Content")
def load_html_content(filepath: str) -> str:
    """Load HTML content from file."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Loading town hall list from: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        html_content = f.read()

    return html_content


@task(name="Extract CM-Prefixed URLs")
def extract_cm_prefixed_urls(html_content: str) -> list[TownHallDTO]:
    """Extract town hall URLs with cm- prefix."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Extracting town hall URLs (cm- prefixed)...")

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href", ""))
        if href.startswith("http://www.cm-") or href.startswith("https://www.cm-"):
            town_halls.append(TownHallDTO(url=href, domain=urlparse(href).netloc))

    logger.debug(f"Found {len(town_halls)} town halls with cm- prefix")
    return town_halls


@task(name="Extract Consultar Website URLs")
def extract_consultar_website_urls(html_content: str) -> list[TownHallDTO]:
    """Extract town hall URLs from 'consultar website' links."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Extracting town hall URLs (consultar website)...")

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = str(a_tag["href"]).replace("\t", "").replace("\n", "")
        if "consultar website" in text:
            town_halls.append(TownHallDTO(url=href, domain=urlparse(href).netloc))

    logger.debug(f"Found {len(town_halls)} URLs from 'consultar website' links")
    return town_halls


@task(name="Deduplicate Town Halls")
def deduplicate_town_halls(town_halls: list[TownHallDTO]) -> list[TownHallDTO]:
    """Remove duplicate town halls by URL."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Deduplicating URLs...")
    logger.debug(f"Total URLs before deduplication: {len(town_halls)}")

    seen_urls = set()
    unique_town_halls = []
    duplicate_count = 0

    for th in town_halls:
        if th.url not in seen_urls:
            seen_urls.add(th.url)
            unique_town_halls.append(th)
        else:
            duplicate_count += 1

    logger.debug(f"Removed {duplicate_count} duplicate URLs")
    return unique_town_halls


@task(name="Normalize URLs to HTTPS")
def normalize_urls_to_https(town_halls: list[TownHallDTO]) -> list[TownHallDTO]:
    """Normalize all URLs to use HTTPS protocol."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Normalizing URLs to HTTPS...")

    for th in town_halls:
        th.url = th.url.replace("http://", "https://")

    return town_halls


@task(name="Add Known Missing Town Halls")
def add_known_missing_town_halls(town_halls: list[TownHallDTO]) -> list[TownHallDTO]:
    """Add known missing town halls to the list."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Adding known missing town halls...")

    known_town_halls = [
        "https://www.mogadouro.pt/",
        "https://www.cm-maia.pt/",
        "https://www.ourem.pt/",
        "https://www.sines.pt/",
        "https://www.cmav.pt/",
        "https://www.cmpb.pt/",
        "https://www.chaves.pt/",
        "https://valpacos.pt/",
        "https://angradoheroismo.pt/",
        "https://www.cmpv.pt/",
        "https://cmvfc.pt/",
    ]

    seen_urls = {th.url for th in town_halls}
    added_count = 0

    for url in known_town_halls:
        if url not in seen_urls:
            town_halls.append(TownHallDTO(url=url, domain=urlparse(url).netloc))
            seen_urls.add(url)
            added_count += 1

    logger.debug(f"Added {added_count} known missing town halls")
    return town_halls


@task(name="Fix Malformed URLs")
def fix_malformed_urls(town_halls: list[TownHallDTO]) -> list[TownHallDTO]:
    """Fix specific known malformed URLs."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info("Fixing malformed URLs...")

    malformed_url = "https://Http://www.cm-campo-maior.pt"
    seen_urls = {th.url for th in town_halls}

    if malformed_url in seen_urls:
        logger.warning(f"Found malformed URL, correcting: {malformed_url}")
        town_halls = [th for th in town_halls if th.url != malformed_url]
        town_halls.append(
            TownHallDTO(
                url="https://www.cm-campo-maior.pt",
                domain="www.cm-campo-maior.pt",
            )
        )
        logger.info(f"Malformed URL corrected to: https://www.cm-campo-maior.pt")

    return town_halls


@task(name="Validate Town Hall Count")
def validate_town_hall_count(
    town_halls: list[TownHallDTO], expected_count: int = 308
) -> list[TownHallDTO]:
    """Validate that the final count matches expectations."""
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Total unique town halls collected: {len(town_halls)}")

    assert (
        len(town_halls) == expected_count
    ), f"Expected {expected_count} town hall URLs, but found {len(town_halls)}"

    return town_halls


@task(name="Fetch Town Hall List")
def fetch_town_hall_list(filepath: str) -> list[TownHallDTO]:
    """Orchestrate the extraction and processing of town hall URLs."""
    # Load HTML content
    html_content = load_html_content(filepath)

    # Extract URLs from different sources
    cm_prefixed_urls = extract_cm_prefixed_urls(html_content)
    consultar_website_urls = extract_consultar_website_urls(html_content)

    # Combine all URLs
    all_town_halls = cm_prefixed_urls + consultar_website_urls

    # Process and clean the data
    unique_town_halls = deduplicate_town_halls(all_town_halls)
    normalized_town_halls = normalize_urls_to_https(unique_town_halls)
    complete_town_halls = add_known_missing_town_halls(normalized_town_halls)
    fixed_town_halls = fix_malformed_urls(complete_town_halls)
    validated_town_halls = validate_town_hall_count(fixed_town_halls)

    return validated_town_halls


@flow(name="Fetch Town Halls")
def fetch_town_halls(data_dir: Optional[str] = None) -> list[TownHallDTO]:

    if data_dir is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        construction_dir = os.path.dirname(current_dir)
        data_dir = os.path.join(construction_dir, "data")

    filepath = os.path.join(data_dir, "link.html")

    return fetch_town_hall_list(filepath=filepath)
