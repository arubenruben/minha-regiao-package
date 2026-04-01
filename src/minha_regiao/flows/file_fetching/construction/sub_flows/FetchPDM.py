import os
import json
import asyncio
import requests
import pandas as pd
from tqdm import tqdm
from pypdf import PdfReader
from typing import Optional
from huggingface_hub import login
from tempfile import NamedTemporaryFile
from prefect import flow, task, get_run_logger
from minha_regiao.flows.file_fetching.construction.sub_flows._pdm_crawler import (
    PDMCrawler,
)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONSTRUCTION_DIR = os.path.dirname(CURRENT_DIR)
CACHE_DIR = os.path.join(CONSTRUCTION_DIR, "cache")
DATA_DIR = os.path.join(CONSTRUCTION_DIR, "data")
PDM_CACHE_FILEPATH = os.path.join(CACHE_DIR, "pdm_results.json")


def load_pdm_cache(cache_filepath: str) -> dict[str, list[str]]:
    if not os.path.exists(cache_filepath):
        return {}

    with open(cache_filepath, "r", encoding="utf-8") as cache_file:
        cached_entries = json.load(cache_file)

    if not isinstance(cached_entries, dict):
        raise ValueError(f"Expected a JSON object in {cache_filepath}")

    return {
        str(url): result
        for url, result in cached_entries.items()
        if isinstance(url, str) and isinstance(result, list)
    }


def persist_pdm_cache(
    cache_filepath: str, cached_entries: dict[str, list[str]]
) -> None:
    os.makedirs(os.path.dirname(cache_filepath), exist_ok=True)
    temp_filepath = f"{cache_filepath}.tmp"

    with open(temp_filepath, "w", encoding="utf-8") as cache_file:
        json.dump(
            cached_entries, cache_file, ensure_ascii=False, indent=2, sort_keys=True
        )

    os.replace(temp_filepath, cache_filepath)


@task(name="Login to Hugging Face")
def login_to_hf(api_key: str):
    login(token=api_key)


@task(name="Navigate to PDM URL")
def navigate_to_pdm_url(root_url: str, max_concurrency: int = 12, max_pages: int = 400):
    logger = get_run_logger()
    return asyncio.run(PDMCrawler(root_url, max_concurrency, max_pages, logger).crawl())


PDM_KEY_TERMS = ["zonamento", "usos do solo", "revisão do plano"]


@task(name="Fetch PDF Content")
def fetch_pdf_content(url: str) -> Optional[dict]:
    """Fetch a PDF from URL and extract its content and page count."""
    response = requests.get(url)

    if response.status_code != 200:
        get_run_logger().warning(f"Failed to fetch {url}: {response.status_code}")
        return None

    temp_file_path = None

    try:
        with NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name

        reader = PdfReader(temp_file_path)
        return {
            "url": url,
            "content": "".join(page.extract_text() for page in reader.pages),
            "number_of_pages": len(reader.pages),
        }
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


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
def filter_pdm_candidates(pdm_results: list[str]) -> Optional[str]:
    pdf_contents = [
        fetch_pdf_content(url)
        for url in tqdm(pdm_results, desc="Filtering PDM candidates")
    ]

    valid_contents = [content for content in pdf_contents if content is not None]

    if not valid_contents:
        return None

    df = pd.DataFrame(valid_contents)
    filtered_df = filter_by_page_count(df)

    if filtered_df.empty:
        return None

    return select_best_candidate(filtered_df, PDM_KEY_TERMS)


@flow(name="Fetch PDM")
def fetch_pdm(url_town_halls: list[str]) -> dict[str, list[str]]:
    cached_results = load_pdm_cache(PDM_CACHE_FILEPATH)
    selected_town_halls = url_town_halls[:2]
    pending_town_halls = [
        url for url in selected_town_halls if url not in cached_results
    ]

    submitted_tasks = {
        url: navigate_to_pdm_url.submit(url) for url in pending_town_halls
    }

    results = {
        url: cached_results[url] for url in selected_town_halls if url in cached_results
    }

    with tqdm(total=len(selected_town_halls), desc="Processing town halls") as progress:
        for cached_url in results:
            progress.update(1)
            progress.set_postfix_str(f"cached: {cached_url}")

        for url, future in submitted_tasks.items():
            results[url] = future.result()
            cached_results[url] = results[url]
            persist_pdm_cache(PDM_CACHE_FILEPATH, cached_results)
            progress.update(1)
            progress.set_postfix_str(f"done: {url}")

    final_results = {}

    for url, pdm_urls in tqdm(results.items(), desc="Logging PDM URLs"):
        candidate = filter_pdm_candidates(pdm_urls)

        if not candidate:
            get_run_logger().warning(f"No valid PDM candidate found for {url}")

        final_results[url] = candidate

    return final_results
