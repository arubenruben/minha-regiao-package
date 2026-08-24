"""Web crawler for discovering PDM PDF documents."""

import asyncio
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse

from .url_normalizer import URLNormalizer
from .pdm_detector import PDMDetector
from minha_regiao.flows.file_fetching.construction.schema.PDMCrawlResultDTO import (
    PDMCrawlResultDTO,
)


class WebCrawler:
    """
    Asynchronous web crawler for discovering PDM PDF documents on town hall websites.

    The crawler starts from a root URL and follows links within the same domain and path,
    looking for PDF files that match PDM-related keywords.
    """

    def __init__(
        self,
        root_url: str,
        max_concurrency: int = 12,
        max_pages: int = 400,
        logger=None,
    ):
        """
        Initialize the web crawler.

        Args:
            root_url: Starting URL for the crawl
            max_concurrency: Maximum number of concurrent workers
            max_pages: Maximum number of pages to visit
            logger: Logger instance for tracking progress
        """
        self.url_normalizer = URLNormalizer()
        self.pdm_detector = PDMDetector()

        # Normalize and parse the root URL
        normalized = self.url_normalizer.normalize_url(root_url)
        root_parts = urlparse(normalized)

        self.root_parts = root_parts
        self.root_domain = root_parts.netloc
        self.root_path = root_parts.path or "/"
        if self.root_path != "/":
            self.root_path = self.root_path.rstrip("/")

        self.max_concurrency = max_concurrency
        self.max_pages = max_pages
        self.logger = logger

        # State tracking
        self.visited: set[str] = set()
        self.queued: set[str] = {normalized}
        self.candidate_pdf_urls: set[str] = set()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue.put_nowait(normalized)
        self.lock = asyncio.Lock()
        self.client: httpx.AsyncClient | None = None

    async def _process_links(self, soup: BeautifulSoup, current_url: str) -> None:
        for a_tag in soup.find_all("a", href=True):
            href = str(a_tag["href"]).strip()
            href_text = a_tag.get_text(strip=True)

            # Resolve relative URLs and normalize
            full_url = self.url_normalizer.normalize_url(urljoin(current_url, href))

            # Check if this is a PDM PDF candidate
            if self.pdm_detector.is_pdm_pdf_url(full_url, href_text):
                if full_url not in self.candidate_pdf_urls:
                    self.candidate_pdf_urls.add(full_url)
                    if self.logger:
                        self.logger.info(
                            f"[{self.root_domain}] Found PDM candidate: {full_url}"
                        )

            # Check if we should crawl this link
            if self.url_normalizer.is_within_root_scope(
                full_url, self.root_domain, self.root_path
            ):
                # Skip file URLs (we only crawl HTML pages)
                if self.url_normalizer.is_file_url(full_url):
                    continue

                # Add to queue if not already visited or queued
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
                # Check if we should skip this URL
                async with self.lock:
                    if (
                        current_url in self.visited
                        or len(self.visited) >= self.max_pages
                    ):
                        continue
                    self.visited.add(current_url)

                if self.logger:
                    self.logger.info(
                        f"[{self.root_domain}] Visiting page: {current_url}"
                    )

                # Fetch the page
                try:
                    if self.client is None:
                        raise RuntimeError("HTTP client not initialized")
                    response = await self.client.get(current_url, timeout=60.0)
                    response.raise_for_status()
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    if self.logger:
                        self.logger.warning(
                            f"[{self.root_domain}] Error fetching {current_url}: {e}"
                        )
                    continue

                # Parse and process links
                soup = BeautifulSoup(response.text, "html.parser")
                await self._process_links(soup, current_url)

            finally:
                self.queue.task_done()

    async def crawl(self) -> PDMCrawlResultDTO:
        if self.logger:
            self.logger.info(
                f"[{self.root_domain}] Starting crawl "
                f"(max_concurrency={self.max_concurrency}, max_pages={self.max_pages})"
            )

        # Create HTTP client with limits
        async with httpx.AsyncClient(
            limits=httpx.Limits(max_connections=self.max_concurrency), timeout=60.0
        ) as client:
            self.client = client

            # Spawn workers
            workers = [
                asyncio.create_task(self._worker())
                for _ in range(max(1, self.max_concurrency))
            ]

            # Wait for all work to complete
            await self.queue.join()

            # Cancel workers
            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            self.client = None

        if self.logger:
            self.logger.info(
                f"[{self.root_domain}] Crawl completed: "
                f"visited {len(self.visited)} pages, "
                f"found {len(self.candidate_pdf_urls)} PDM candidates"
            )

        return PDMCrawlResultDTO(
            town_hall_url=f"{self.root_parts.scheme}://{self.root_domain}{self.root_path}",
            candidate_pdf_urls=sorted(self.candidate_pdf_urls),
            pages_visited=len(self.visited),
            crawl_timestamp=datetime.now().isoformat(),
        )
