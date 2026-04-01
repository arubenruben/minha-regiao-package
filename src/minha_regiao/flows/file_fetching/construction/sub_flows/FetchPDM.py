import os
import asyncio
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
from huggingface_hub import login
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


@task(name="Fetch Town Hall List")
def fetch_town_hall_list(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href", ""))
        if href.startswith("http://www.cm-") or href.startswith("https://www.cm-"):
            town_halls.append(href)

    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = str(a_tag["href"]).replace("\t", "").replace("\n", "")
        if "consultar website" in text:
            town_halls.append(href)

    unique_town_halls = sorted(set(town_halls))
    unique_town_halls = [
        url.replace("http://", "https://") for url in unique_town_halls
    ]

    for url in [
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
    ]:
        if url not in unique_town_halls:
            unique_town_halls.append(url)

    if "https://Http://www.cm-campo-maior.pt" in unique_town_halls:
        unique_town_halls.remove("https://Http://www.cm-campo-maior.pt")
        unique_town_halls.append("https://www.cm-campo-maior.pt")

    assert (
        len(unique_town_halls) == 308
    ), f"Expected 308 town hall URLs, but found {len(unique_town_halls)}"

    return unique_town_halls


@task(name="Navigate to PDM URL")
def navigate_to_pdm_url(root_url: str, max_concurrency: int = 12, max_pages: int = 400):
    logger = get_run_logger()
    return asyncio.run(PDMCrawler(root_url, max_concurrency, max_pages, logger).crawl())


@flow(name="Fetch PDM")
def fetch_pdm():
    url_town_halls = fetch_town_hall_list(filepath=os.path.join(DATA_DIR, "link.html"))
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

    return url_town_halls


if __name__ == "__main__":
    fetch_pdm()
