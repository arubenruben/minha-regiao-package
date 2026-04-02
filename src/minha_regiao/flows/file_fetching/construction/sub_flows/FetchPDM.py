import os
import json
import httpx
import asyncio
import pandas as pd
from tqdm import tqdm
from pypdf import PdfReader
from typing import Optional
from huggingface_hub import login
from tempfile import NamedTemporaryFile
from prefect import flow, task, get_run_logger, cache_policies
from minha_regiao.flows.file_fetching.construction.sub_flows._pdm_crawler import (
    PDMCrawler,
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


def load_pdm_cache(cache_filepath: str) -> dict[str, PDMCacheEntryDTO]:
    if not os.path.exists(cache_filepath):
        return {}

    with open(cache_filepath, "r", encoding="utf-8") as cache_file:
        cached_entries = json.load(cache_file)

    if not isinstance(cached_entries, dict):
        raise ValueError(f"Expected a JSON object in {cache_filepath}")

    return {
        str(url): PDMCacheEntryDTO(
            town_hall_url=str(url),
            candidate_urls=result,
        )
        for url, result in cached_entries.items()
        if isinstance(url, str) and isinstance(result, list)
    }


def persist_pdm_cache(
    cache_filepath: str, cached_entries: dict[str, PDMCacheEntryDTO]
) -> None:
    os.makedirs(os.path.dirname(cache_filepath), exist_ok=True)
    temp_filepath = f"{cache_filepath}.tmp"

    # Convert DTOs to dict for JSON serialization
    serializable_entries = {
        entry.town_hall_url: entry.candidate_urls for entry in cached_entries.values()
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


@task(name="Login to Hugging Face")
def login_to_hf(api_key: str):
    login(token=api_key)


@task(name="Navigate to PDM URL")
def navigate_to_pdm_url(
    town_hall: TownHallDTO, max_concurrency: int = 12, max_pages: int = 400
) -> PDMCrawlResultDTO:
    logger = get_run_logger()
    return asyncio.run(
        PDMCrawler(town_hall.url, max_concurrency, max_pages, logger).crawl()
    )


PDM_KEY_TERMS = ["zonamento", "usos do solo", "revisão do plano"]


@task(name="Fetch PDF Content", cache_policy=cache_policies.NO_CACHE)
async def fetch_pdf_content_async(
    url: str, client: httpx.AsyncClient
) -> Optional[PDFContentDTO]:
    """Fetch a PDF from URL and extract its content and page count."""
    try:
        response = await client.get(url)
    except Exception as e:
        get_run_logger().warning(f"Failed to fetch {url}: {e}")
        return None

    if response.status_code != 200:
        get_run_logger().warning(f"Failed to fetch {url}: {response.status_code}")
        return None

    temp_file_path = None

    try:
        with NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name

        reader = PdfReader(temp_file_path)
        return PDFContentDTO(
            url=url,
            content="".join(page.extract_text() for page in reader.pages),
            number_of_pages=len(reader.pages),
            file_size_bytes=len(response.content),
        )
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


async def fetch_all_pdfs_concurrently(
    urls: list[str], max_concurrency: int = 10
) -> list[Optional[PDFContentDTO]]:
    """Fetch multiple PDFs concurrently using httpx."""
    limits = httpx.Limits(max_connections=max_concurrency)

    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        tasks = [fetch_pdf_content_async(url, client) for url in urls]
        return await asyncio.gather(*tasks)


def filter_by_page_count(df: pd.DataFrame, quantile: float = 0.75) -> pd.DataFrame:
    """Filter URLs with page count above the specified quantile."""
    threshold = df["number_of_pages"].quantile(quantile)
    return df[df["number_of_pages"] > threshold]


def score_content_by_key_terms(content: str, key_terms: list[str]) -> int:
    """Count occurrences of key terms in the content."""
    content_lower = content.lower()
    return sum(content_lower.count(term) for term in key_terms)


def select_best_candidate(
    filtered_df: pd.DataFrame, key_terms: list[str]
) -> Optional[str]:
    """Select the URL with the highest key term count from filtered candidates."""
    best_url = None
    max_key_terms_count = 0

    for _, row in filtered_df.iterrows():
        key_terms_count = score_content_by_key_terms(row["content"], key_terms)
        if key_terms_count > max_key_terms_count:
            max_key_terms_count = key_terms_count
            best_url = row["url"]

    return best_url


@task(name="Filter PDM Candidates")
def filter_pdm_candidates(pdm_results: list[str]) -> Optional[PDFContentDTO]:
    pdf_contents = asyncio.run(fetch_all_pdfs_concurrently(pdm_results))

    valid_contents = [content for content in pdf_contents if content is not None]

    if not valid_contents:
        return None

    df = pd.DataFrame([content.model_dump() for content in valid_contents])
    filtered_df = filter_by_page_count(df)

    if filtered_df.empty:
        return None

    best_url = select_best_candidate(filtered_df, PDM_KEY_TERMS)
    if best_url:
        return next((c for c in valid_contents if c.url == best_url), None)
    return None


@flow(name="Fetch PDM")
def fetch_pdm(town_halls: list[TownHallDTO]) -> PDMFetchResultDTO:
    cached_results = load_pdm_cache(PDM_CACHE_FILEPATH)

    pending_town_halls = [th for th in town_halls if th.url not in cached_results]

    submitted_tasks = {
        th.url: navigate_to_pdm_url.submit(th) for th in pending_town_halls
    }

    results: dict[str, PDMCrawlResultDTO] = {}

    with tqdm(total=len(town_halls), desc="Processing town halls") as progress:
        for cached_url in [th.url for th in town_halls if th.url in cached_results]:
            cache_entry = cached_results[cached_url]
            results[cached_url] = PDMCrawlResultDTO(
                town_hall_url=cache_entry.town_hall_url,
                candidate_pdf_urls=cache_entry.candidate_urls,
                pages_visited=0,
            )
            progress.update(1)
            progress.set_postfix_str(f"cached: {cached_url}")

        for url, future in submitted_tasks.items():
            results[url] = future.result()
            progress.update(1)
            progress.set_postfix_str(f"crawled: {url}")

    for url, crawl_result in tqdm(results.items(), desc="Filtering PDM candidates"):
        candidate = filter_pdm_candidates(crawl_result.candidate_pdf_urls)

        if not candidate:
            get_run_logger().warning(f"No valid PDM candidate found for {url}")
        else:
            crawl_result.best_candidate = candidate
            cached_results[url] = PDMCacheEntryDTO(
                town_hall_url=url,
                candidate_urls=crawl_result.candidate_pdf_urls,
            )
            persist_pdm_cache(PDM_CACHE_FILEPATH, cached_results)
            progress.set_postfix_str(f"cached: {url}")

    all_candidates = [
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

    return PDMFetchResultDTO(
        town_halls_processed=len(results),
        total_candidates_found=len(all_candidates),
        candidates=all_candidates,
        crawl_results=list(results.values()),
    )
