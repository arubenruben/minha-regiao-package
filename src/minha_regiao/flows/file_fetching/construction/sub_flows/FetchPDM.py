import os
import json
import httpx
import asyncio
import pandas as pd
from tqdm import tqdm
from pypdf import PdfReader
from threading import Lock
from datetime import datetime
from typing import Optional, Any
from tempfile import NamedTemporaryFile
from prefect import flow, task, get_run_logger
from pypdf.errors import PdfStreamError, PdfReadError
from minha_regiao.flows.file_fetching.construction.sub_flows.pdms import (
    crawl_for_pdm_candidates,
)
from minha_regiao.flows.file_fetching.construction.schema.TownHallDTO import TownHallDTO
from minha_regiao.flows.file_fetching.construction.schema.PDMCandidateDTO import (
    PDMCandidateDTO,
)
from minha_regiao.flows.file_fetching.construction.schema.PDMCrawlResultDTO import (
    PDMCrawlResultDTO,
)
from minha_regiao.flows.file_fetching.construction.schema.PDMFetchResultDTO import (
    PDMFetchResultDTO,
)
from minha_regiao.flows.file_fetching.construction.schema.PDFContentDTO import (
    PDFContentDTO,
)
from minha_regiao.flows.file_fetching.construction.schema.PDMCacheEntryDTO import (
    PDMCacheEntryDTO,
)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONSTRUCTION_DIR = os.path.dirname(CURRENT_DIR)
CACHE_DIR = os.path.join(CONSTRUCTION_DIR, "cache")
DATA_DIR = os.path.join(CONSTRUCTION_DIR, "data")
PDM_CACHE_FILEPATH = os.path.join(CACHE_DIR, "pdm_results.json")
PDM_KEY_TERMS = [
    # Termos fundamentais de ordenamento
    "ordenamento do território",
    "plano diretor municipal",
    "zonamento",
    "usos do solo",
    "ocupação do solo",
    
    # Classificação e qualificação do solo
    "solo urbano",
    "solo urbanizável",
    "solo rural",
    "perímetro urbano",
    
    # Áreas e zonas específicas
    "áreas urbanas",
    "zonas residenciais",
    "zonas industriais",
    "zona agrícola",
    "espaços verdes",
    
    # Parâmetros urbanísticos
    "edificabilidade",
    "índice de construção",
    "densidade populacional",
    "cércea",
    
    # Equipamentos e infraestruturas
    "equipamentos coletivos",
    "infraestruturas urbanas",
    "rede viária",
    
    # Condicionantes e servidões
    "servidões administrativas",
    "condicionantes",
    "reserva ecológica nacional",
    "reserva agrícola nacional",
    "domínio público",
    
    # Peças desenhadas
    "planta de ordenamento",
    "planta de condicionantes",
    
    # Regulamentação
    "regulamento municipal",
    "revisão do plano",
    "alteração por adaptação",
]

# Global cache lock for thread-safe operations
_cache_lock = Lock()


@task(name="Load PDM Cache")
def load_pdm_cache(cache_filepath: str) -> dict[str, PDMCacheEntryDTO]:
    """Load cache from file in a thread-safe manner."""
    with _cache_lock:
        if not os.path.exists(cache_filepath):
            return {}

        with open(cache_filepath, "r", encoding="utf-8") as cache_file:
            cached_entries = json.load(cache_file)

        if not isinstance(cached_entries, dict):
            raise ValueError(f"Expected a JSON object in {cache_filepath}")

        result = {}

        for url, entry_data in cached_entries.items():
            if not isinstance(url, str):
                continue

            if isinstance(entry_data, list):
                result[url] = PDMCacheEntryDTO(
                    town_hall_url=url,
                    candidate_urls=entry_data,
                )
            elif isinstance(entry_data, dict):
                result[url] = PDMCacheEntryDTO(
                    town_hall_url=url,
                    candidate_urls=entry_data.get("candidate_urls", []),
                    best_candidate=entry_data.get("best_candidate"),
                    crawl_timestamp=entry_data.get("crawl_timestamp"),
                    filter_timestamp=entry_data.get("filter_timestamp"),
                )

        return result


@task(name="Persist PDM Cache")
def persist_pdm_cache(
    cache_filepath: str, cached_entries: dict[str, PDMCacheEntryDTO]
) -> None:
    """Persist cache to file in a thread-safe manner using atomic write."""
    with _cache_lock:
        os.makedirs(os.path.dirname(cache_filepath), exist_ok=True)
        temp_filepath = f"{cache_filepath}.tmp"

        # Convert DTOs to dict for JSON serialization
        serializable_entries = {
            entry.town_hall_url: entry.to_dict() for entry in cached_entries.values()
        }

        with open(temp_filepath, "w", encoding="utf-8") as cache_file:
            json.dump(
                serializable_entries,
                cache_file,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

        os.replace(temp_filepath, cache_filepath)


@task(name="Save Crawl Result to Cache")
def save_crawl_result(
    cache_filepath: str,
    url: str,
    crawl_result: PDMCrawlResultDTO,
) -> None:
    """Save a single crawl result to cache incrementally."""
    cached_entries = load_pdm_cache(cache_filepath)

    # Update or create cache entry with crawl data
    if url in cached_entries:
        cached_entries[url].candidate_urls = crawl_result.candidate_pdf_urls
        cached_entries[url].crawl_timestamp = datetime.now().isoformat()
    else:
        cached_entries[url] = PDMCacheEntryDTO(
            town_hall_url=url,
            candidate_urls=crawl_result.candidate_pdf_urls,
            crawl_timestamp=datetime.now().isoformat(),
        )

    persist_pdm_cache(cache_filepath, cached_entries)


@task(name="Save Filter Result to Cache")
def save_filter_result(
    cache_filepath: str,
    url: str,
    best_candidate: Optional[PDFContentDTO],
) -> None:
    """Save a single filter result to cache incrementally."""
    cached_entries = load_pdm_cache(cache_filepath)

    if url in cached_entries:
        cached_entries[url].best_candidate = (
            best_candidate.model_dump() if best_candidate else None
        )
        cached_entries[url].filter_timestamp = datetime.now().isoformat()
        persist_pdm_cache(cache_filepath, cached_entries)


@task(name="Navigate to PDM URL")
def navigate_to_pdm_url(
    town_hall: TownHallDTO, max_concurrency: int, max_pages: int = 400
) -> PDMCrawlResultDTO:
    result = asyncio.run(
        crawl_for_pdm_candidates(
            town_hall.url, max_concurrency, max_pages, get_run_logger()
        )
    )
    return result


@task(name="Crawl and Cache PDM")
def crawl_and_cache_pdm(
    town_hall: TownHallDTO,
    cache_filepath: str,
    max_concurrency: int,
    max_pages: int = 400,
) -> PDMCrawlResultDTO:
    """Crawl PDM and save result to cache immediately."""
    result = navigate_to_pdm_url(town_hall, max_concurrency, max_pages)
    save_crawl_result(cache_filepath, town_hall.url, result)
    return result


@task(name="Filter and Cache PDM Candidate")
def filter_and_cache_pdm_candidate(
    url: str,
    candidate_urls: list[str],
    cache_filepath: str,
    max_concurrency: int,
) -> Optional[PDFContentDTO]:
    """Filter PDM candidates and save best candidate to cache immediately."""
    logger = get_run_logger()
    if not candidate_urls:
        logger.debug(f"No candidate URLs for {url}")
        return None

    logger.info(f"Filtering {len(candidate_urls)} PDM candidates for: {url}")
    best_candidate = filter_pdm_candidates(candidate_urls, max_concurrency)
    save_filter_result(cache_filepath, url, best_candidate)

    if best_candidate:
        logger.info(
            f"Selected best PDM candidate: {best_candidate.url} ({best_candidate.number_of_pages} pages, {best_candidate.file_size_bytes} bytes)"
        )
    else:
        logger.warning(f"No valid PDM candidate found for {url}")

    return best_candidate


async def fetch_pdf_content_async(
    url: str, client: httpx.AsyncClient, logger
) -> Optional[PDFContentDTO]:
    """Fetch a PDF from URL and extract its content and page count."""
    try:
        response = await client.get(url, timeout=600.0)
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None

    if response.status_code != 200:
        logger.debug(f"Failed to fetch {url}: {response.status_code}")
        return None

    temp_file_path = None

    try:
        with NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name

        try:
            reader = PdfReader(temp_file_path)
            logger.debug(
                f"Successfully fetched PDF: {url} ({len(reader.pages)} pages, {len(response.content)} bytes)"
            )
            return PDFContentDTO(
                url=url,
                content="".join(page.extract_text() for page in reader.pages),
                number_of_pages=len(reader.pages),
                file_size_bytes=len(response.content),
            )
        except (PdfStreamError, PdfReadError) as pdf_error:
            logger.debug(f"Failed to parse PDF from {url}: {pdf_error}")
            return None
        except Exception as parse_error:
            logger.debug(f"Unexpected error parsing PDF from {url}: {parse_error}")
            return None
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


@task(name="Fetch All PDFs Concurrently")
async def fetch_all_pdfs_concurrently(
    urls: list[str], max_concurrency: int
) -> list[Optional[PDFContentDTO]]:

    limits = httpx.Limits(max_connections=max_concurrency)

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [fetch_pdf_content_async(url, client, get_run_logger()) for url in urls]
        return await asyncio.gather(*tasks)


@task(name="Select Best PDM Candidate")
def select_best_candidate(
    filtered_df: pd.DataFrame, key_terms: list[str]
) -> Optional[str]:
    """Select the URL with the highest key term count from filtered candidates."""
    best_url = None
    max_key_terms_count = 0

    for _, row in filtered_df.iterrows():
        content_lower = row["content"].lower()
        key_terms_count = sum(content_lower.count(term) for term in key_terms)
        if key_terms_count > max_key_terms_count:
            max_key_terms_count = key_terms_count
            best_url = row["url"]

    return best_url


@task(name="Filter PDM Candidates")
def filter_pdm_candidates(
    pdm_results: list[str], max_concurrency: int
) -> Optional[PDFContentDTO]:
    pdf_contents = asyncio.run(
        fetch_all_pdfs_concurrently(pdm_results, max_concurrency)
    )

    valid_contents = [content for content in pdf_contents if content is not None]

    if not valid_contents:
        return None

    df = pd.DataFrame([content.model_dump() for content in valid_contents])

    # Filter URLs with page count above the 75th percentile
    threshold = df["number_of_pages"].quantile(0.75)
    filtered_df = df[df["number_of_pages"] > threshold]

    if filtered_df.empty:
        return None

    best_url = select_best_candidate(filtered_df, PDM_KEY_TERMS)
    if best_url:
        return next((c for c in valid_contents if c.url == best_url), None)
    return None


@task(name="Categorize Town Halls by Cache Status")
def categorize_town_halls_by_cache(
    town_halls: list[TownHallDTO],
    cached_entries: dict[str, PDMCacheEntryDTO],
) -> tuple[list[TownHallDTO], list[str], list[str]]:
    logger = get_run_logger()
    to_crawl = []
    to_filter = []
    already_complete = []

    for th in town_halls:
        if th.url not in cached_entries:
            to_crawl.append(th)
        elif cached_entries[th.url].filter_timestamp is None:
            to_filter.append(th.url)
            logger.info(
                f"Skipping PDM candidate crawl (already in cache): {th.name} ({th.url})"
            )
        else:
            already_complete.append(th.url)
            logger.info(
                f"Skipping PDM processing (best candidate already determined): {th.name} ({th.url})"
            )

    logger.info(
        f"Cache status: {len(to_crawl)} to crawl, {len(to_filter)} to filter, {len(already_complete)} already complete"
    )
    return to_crawl, to_filter, already_complete


@task(name="Reconstruct Result from Cache")
def reconstruct_result_from_cache(
    url: str,
    cache_entry: PDMCacheEntryDTO,
) -> PDMCrawlResultDTO:

    best_candidate = None

    if cache_entry.best_candidate:
        best_candidate = PDFContentDTO(**cache_entry.best_candidate)

    return PDMCrawlResultDTO(
        town_hall_url=url,
        candidate_pdf_urls=cache_entry.candidate_urls,
        pages_visited=0,
        best_candidate=best_candidate,
    )


@task(name="Build Final Candidates List")
def build_final_candidates(
    results: dict[str, PDMCrawlResultDTO],
) -> list[PDMCandidateDTO]:
    """Build the final list of PDM candidates from crawl results."""
    return [
        PDMCandidateDTO(
            url=r.best_candidate.url,
            source_town_hall=r.town_hall_url,
            metadata={
                "number_of_pages": r.best_candidate.number_of_pages,
                "file_size_bytes": r.best_candidate.file_size_bytes,
            },
        )
        for r in results.values()
        if r.best_candidate is not None
    ]


@task(name="Process Cached Complete Entries")
def process_cached_complete_entries(
    already_complete: list[str],
    cached_entries: dict[str, PDMCacheEntryDTO],
    progress: tqdm,
) -> dict[str, PDMCrawlResultDTO]:
    """Process already complete entries from cache."""
    results = {}

    for url in already_complete:
        cache_entry = cached_entries[url]
        results[url] = reconstruct_result_from_cache(url, cache_entry)
        progress.update(1)
        progress.set_postfix_str(f"cached (complete): {url[:50]}...")

    return results


def wait_for_crawl_results(
    crawl_futures: dict[str, Any],
    progress: tqdm,
    max_concurrency: int,
) -> tuple[dict[str, PDMCrawlResultDTO], dict[str, Any]]:
    """Wait for all crawling tasks to complete and return results."""
    crawl_results = {}
    new_filter_futures = {}

    for url, future in crawl_futures.items():
        crawl_result = future.result()
        crawl_results[url] = crawl_result

        # Immediately submit filtering task for this crawl result
        new_filter_futures[url] = filter_and_cache_pdm_candidate.submit(
            url, crawl_result.candidate_pdf_urls, PDM_CACHE_FILEPATH, max_concurrency
        )

        progress.update(1)
        progress.set_postfix_str(f"crawled: {url[:50]}...")

    return crawl_results, new_filter_futures


def wait_for_filter_results(
    filter_futures: dict[str, Any],
    existing_results: dict[str, PDMCrawlResultDTO],
    cached_entries: dict[str, PDMCacheEntryDTO],
    progress: tqdm,
) -> dict[str, PDMCrawlResultDTO]:
    """Wait for all filtering tasks to complete and update results."""
    results = existing_results.copy()

    for url, future in filter_futures.items():
        best_candidate = future.result()

        # Update the result with the best candidate
        if url in results:
            results[url].best_candidate = best_candidate
        else:
            # This was a to_filter item, reconstruct the result
            cache_entry = cached_entries[url]
            results[url] = PDMCrawlResultDTO(
                town_hall_url=url,
                candidate_pdf_urls=cache_entry.candidate_urls,
                pages_visited=0,
                best_candidate=best_candidate,
            )
            progress.update(1)

        status = "filtered" if best_candidate else "no candidate"
        progress.set_postfix_str(f"{status}: {url[:50]}...")

    return results


@flow(name="Fetch PDM")
def fetch_pdm(town_halls: list[TownHallDTO], max_concurrency: int) -> PDMFetchResultDTO:
    logger = get_run_logger()
    logger.info(f"Starting PDM fetch flow for {len(town_halls)} town halls")
    logger.info(f"Max concurrency set to: {max_concurrency}")

    # Load cache and categorize work
    cached_entries = load_pdm_cache(PDM_CACHE_FILEPATH)
    logger.info(f"Loaded cache with {len(cached_entries)} entries")
    to_crawl, to_filter, already_complete = categorize_town_halls_by_cache(
        town_halls, cached_entries
    )

    # Initialize result tracking
    crawl_futures = {}
    filter_futures = {}

    # Submit crawling tasks for town halls not in cache
    for th in to_crawl:
        crawl_futures[th.url] = crawl_and_cache_pdm.submit(
            th, PDM_CACHE_FILEPATH, max_concurrency
        )

    # Submit filtering tasks for already-crawled entries
    for url in to_filter:
        cache_entry = cached_entries[url]
        filter_futures[url] = filter_and_cache_pdm_candidate.submit(
            url, cache_entry.candidate_urls, PDM_CACHE_FILEPATH, max_concurrency
        )

    # Process results with progress tracking
    with tqdm(total=len(town_halls), desc="Processing PDMs") as progress:
        # Handle already complete entries
        results = process_cached_complete_entries(
            already_complete, cached_entries, progress
        )

        # Wait for crawling tasks and get new filtering futures
        crawl_results, new_filter_futures = wait_for_crawl_results(
            crawl_futures, progress, max_concurrency
        )
        results.update(crawl_results)
        filter_futures.update(new_filter_futures)

        # Wait for all filtering tasks to complete
        results = wait_for_filter_results(
            filter_futures, results, cached_entries, progress
        )

    # Build final output
    all_candidates = build_final_candidates(results)

    logger.info(f"\nPDM Fetch Summary:")
    logger.info(f"  Town halls processed: {len(results)}")
    logger.info(f"  Total candidates found: {len(all_candidates)}")
    logger.info(
        f"  Candidates with best PDM: {len([c for c in all_candidates if c is not None])}"
    )

    return PDMFetchResultDTO(
        town_halls_processed=len(results),
        total_candidates_found=len(all_candidates),
        candidates=all_candidates,
        crawl_results=list(results.values()),
    )
