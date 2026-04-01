import asyncio
import requests
from bs4 import BeautifulSoup
from prefect import flow, task, get_run_logger
from huggingface_hub import login
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit


@task(name="Login to Hugging Face")
def login_to_hf(api_key: str):
    login(token=api_key)


@task(name="Fetch Town Hall List")
def fetch_town_hall_list(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    # Find <a> tags with href containing "http://www.cm-"
    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href", ""))
        if href.startswith("http://www.cm-") or href.startswith("https://www.cm-"):
            town_halls.append(href)

    # Find <a> tags with text containing "consultar website"
    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = str(a_tag["href"]).replace("\t", "").replace("\n", "")

        if "consultar website" in text:
            town_halls.append(href)

    # Find unique town hall URLs
    unique_town_halls = list(set(town_halls))

    unique_town_halls.sort()  # Sort the list of URLs for consistency

    # replace http with https
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

    # Replace specific incorrect URL
    if "https://Http://www.cm-campo-maior.pt" in unique_town_halls:
        unique_town_halls.remove("https://Http://www.cm-campo-maior.pt")
        unique_town_halls.append("https://www.cm-campo-maior.pt")

    # Assert 308 entries
    assert (
        len(unique_town_halls) == 308
    ), f"Expected 308 town hall URLs, but found {len(unique_town_halls)}"

    return unique_town_halls


@task(name="Navigate to PDM URL")
def navigate_to_pdm_url(root_url: str, max_concurrency: int = 12, max_pages: int = 400):
    logger = get_run_logger()

    def normalize_url(url: str) -> str:
        parts = urlsplit(url)

        scheme = parts.scheme.lower()
        hostname = (parts.hostname or "").lower()
        port = parts.port

        if port and (
            (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
        ):
            netloc = hostname
        elif port:
            netloc = f"{hostname}:{port}"
        else:
            netloc = hostname

        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        # Sort query params to treat same URLs with different query order as identical.
        query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))

        return urlunsplit((scheme, netloc, path, query, ""))

    async def _crawl_site() -> list[str]:
        # Crawl pages concurrently with worker tasks while keeping domain boundaries.
        normalized_root_url = normalize_url(root_url)
        root_parts = urlparse(normalized_root_url)
        root_domain = root_parts.netloc
        root_path = root_parts.path or "/"
        if root_path != "/":
            root_path = root_path.rstrip("/")

        def is_within_root_scope(candidate_url: str) -> bool:
            parsed_candidate = urlparse(candidate_url)
            if parsed_candidate.scheme not in {"http", "https"}:
                return False
            if parsed_candidate.netloc != root_domain:
                return False

            candidate_path = parsed_candidate.path or "/"
            if candidate_path != "/":
                candidate_path = candidate_path.rstrip("/")

            if root_path == "/":
                return True

            return candidate_path == root_path or candidate_path.startswith(
                f"{root_path}/"
            )

        visited = set()
        queued = {normalized_root_url}
        candidate_pdf_urls = set()
        queue: asyncio.Queue[str] = asyncio.Queue()
        queue.put_nowait(normalized_root_url)
        lock = asyncio.Lock()

        async def worker() -> None:
            while True:
                current_url = await queue.get()
                try:
                    async with lock:
                        if current_url in visited or len(visited) >= max_pages:
                            continue
                        visited.add(current_url)

                    logger.info(f"[{root_domain}] Visiting page: {current_url}")

                    try:
                        response = await asyncio.to_thread(
                            requests.get, current_url, timeout=30
                        )
                        response.raise_for_status()
                    except requests.RequestException as e:
                        logger.warning(
                            f"[{root_domain}] Error fetching {current_url}: {e}"
                        )
                        continue

                    soup = BeautifulSoup(response.text, "html.parser")

                    for a_tag in soup.find_all("a", href=True):
                        href = str(a_tag["href"]).strip()
                        full_url = normalize_url(urljoin(current_url, href))
                        href_lower = href.lower()
                        full_url_lower = full_url.lower()
                        pdm_keywords = ("pdm", "plano diretor municipal")

                        if full_url_lower.endswith(".pdf") and any(
                            keyword in href_lower or keyword in full_url_lower
                            for keyword in pdm_keywords
                        ):
                            candidate_pdf_urls.add(full_url)

                        if is_within_root_scope(full_url):
                            async with lock:
                                if (
                                    full_url not in visited
                                    and full_url not in queued
                                    and len(visited) + queue.qsize() < max_pages
                                ):
                                    queued.add(full_url)
                                    queue.put_nowait(full_url)
                finally:
                    queue.task_done()

        workers = [
            asyncio.create_task(worker()) for _ in range(max(1, max_concurrency))
        ]

        await queue.join()

        for task_ref in workers:
            task_ref.cancel()

        await asyncio.gather(*workers, return_exceptions=True)
        return sorted(candidate_pdf_urls)

    return asyncio.run(_crawl_site())


@flow(name="Fetch PDM")
def fetch_pdm():
    url_town_halls = fetch_town_hall_list(
        filepath="C:\\Users\\dst7095\\Desktop\\Nova pasta\\minha-regiao-package\\src\\minha_regiao\\flows\\file_fetching\\construction\\data\\link.html"
    )

    submitted_tasks = {
        url: navigate_to_pdm_url.submit(url) for url in url_town_halls[:2]
    }

    results = {url: future.result() for url, future in submitted_tasks.items()}

    return url_town_halls


if __name__ == "__main__":
    fetch_pdm()
