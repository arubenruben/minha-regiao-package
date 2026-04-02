import asyncio
import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit
from minha_regiao.flows.file_fetching.construction.schema.PDMCrawlResultDTO import (
    PDMCrawlResultDTO,
)


def normalize_for_keyword_match(value: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "", value).lower()


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


def is_within_root_scope(candidate_url: str, root_domain: str, root_path: str) -> bool:
    parsed = urlparse(candidate_url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc != root_domain:
        return False

    candidate_path = parsed.path or "/"
    if candidate_path != "/":
        candidate_path = candidate_path.rstrip("/")

    if root_path == "/":
        return True

    return candidate_path == root_path or candidate_path.startswith(f"{root_path}/")


PDM_KEYWORDS = tuple(
    normalize_for_keyword_match(kw) for kw in ("PDM", "plano diretor municipal")
)

FILE_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".zip",
        ".rar",
        ".7z",
        ".tar",
        ".gz",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".svg",
        ".mp3",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
        ".txt",
        ".csv",
        ".xml",
        ".json",
    }
)


def is_file_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return any(path.endswith(ext) for ext in FILE_EXTENSIONS)


class PDMCrawler:
    def __init__(self, root_url: str, max_concurrency: int, max_pages: int, logger):
        normalized = normalize_url(root_url)
        root_parts = urlparse(normalized)
        self.root_parts = root_parts
        self.root_domain = root_parts.netloc
        self.root_path = root_parts.path or "/"
        if self.root_path != "/":
            self.root_path = self.root_path.rstrip("/")

        self.max_concurrency = max_concurrency
        self.max_pages = max_pages
        self.logger = logger

        self.visited: set[str] = set()
        self.queued: set[str] = {normalized}
        self.candidate_pdf_urls: set[str] = set()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue.put_nowait(normalized)
        self.lock = asyncio.Lock()

    async def _process_links(self, soup: BeautifulSoup, current_url: str) -> None:
        for a_tag in soup.find_all("a", href=True):
            href = str(a_tag["href"]).strip()
            full_url = normalize_url(urljoin(current_url, href))
            href_normalized = normalize_for_keyword_match(href)
            full_url_normalized = normalize_for_keyword_match(full_url)

            if full_url.lower().endswith(".pdf") and any(
                keyword in href_normalized or keyword in full_url_normalized
                for keyword in PDM_KEYWORDS
            ):
                self.candidate_pdf_urls.add(full_url)

            if is_within_root_scope(full_url, self.root_domain, self.root_path):
                if is_file_url(full_url):
                    continue

                async with self.lock:
                    if (
                        full_url not in self.visited
                        and full_url not in self.queued
                        and len(self.visited) + self.queue.qsize() < self.max_pages
                    ):
                        self.queued.add(full_url)
                        self.queue.put_nowait(full_url)

    async def _worker(self) -> None:
        while True:
            current_url = await self.queue.get()
            try:
                async with self.lock:
                    if (
                        current_url in self.visited
                        or len(self.visited) >= self.max_pages
                    ):
                        continue
                    self.visited.add(current_url)

                self.logger.info(f"[{self.root_domain}] Visiting page: {current_url}")

                try:
                    response = await asyncio.to_thread(
                        requests.get, current_url, timeout=30
                    )
                    response.raise_for_status()
                except requests.RequestException as e:
                    self.logger.warning(
                        f"[{self.root_domain}] Error fetching {current_url}: {e}"
                    )
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                await self._process_links(soup, current_url)
            finally:
                self.queue.task_done()

    async def crawl(self) -> PDMCrawlResultDTO:
        workers = [
            asyncio.create_task(self._worker())
            for _ in range(max(1, self.max_concurrency))
        ]

        await self.queue.join()

        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        return PDMCrawlResultDTO(
            town_hall_url=f"{self.root_parts.scheme}://{self.root_domain}{self.root_path}",
            candidate_pdf_urls=sorted(self.candidate_pdf_urls),
            pages_visited=len(self.visited),
            crawl_timestamp=datetime.now().isoformat(),
        )
