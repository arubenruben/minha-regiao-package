import asyncio
import logging
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from prefect.tasks import exponential_backoff
from scrapling.fetchers import AsyncStealthySession
from scrapling.engines.toolbelt.custom import Response

from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult
from minha_regiao.flows.extract_pdms.services.PDMDocumentValidator import download_and_validate_pdm
from minha_regiao.flows.extract_pdms.services.PDMLinkMatcher import (
    is_high_priority,
    is_pdm_regulation_pdf,
    is_worth_following,
)

# Scrapling logs every failed/retried request straight to its own console
# handler (see scrapling.core.utils._utils.setup_logger), independent of
# Prefect's logging. _fetch_page already retries and gracefully skips every
# failure it can hit (redirect loops, download-triggering URLs, timeouts,
# ...) and the outcome is reported at the appropriate level through Prefect's
# own logger, so Scrapling's per-attempt console spam is silenced here.
logging.getLogger("scrapling").setLevel(logging.CRITICAL)

# 401/403 are assumed to mean the *site* is blocking/rate-limiting us, not
# just that one page, so they're retried with backoff rather than skipped
# like other failures; if they persist, the whole crawl gives up (SiteBlockedError)
# instead of ploughing through the rest of the page budget against a site
# that's just going to keep rejecting us.
AUTH_ERROR_STATUSES = (401, 403)


class SiteBlockedError(Exception):
    """Raised when a page keeps returning 401/403 after all retries."""


def _strip_fragment(url: str) -> str:
    return url.split("#", 1)[0].rstrip("/")


async def _fetch_page(
    session: AsyncStealthySession,
    url: str,
    request_delay_seconds: float,
    max_retries_on_403: int,
    retry_backoff_seconds: float,
) -> Response | None:
    """Fetches `url`, waiting `request_delay_seconds` beforehand to throttle
    requests to the site. A 401/403 is assumed to be rate limiting rather
    than a hard failure, so it's retried with exponential backoff (via
    Prefect's own `exponential_backoff` helper) instead of being given up on
    immediately; raises `SiteBlockedError` if it's still failing after
    `max_retries_on_403` retries.
    """
    backoff_delays = exponential_backoff(retry_backoff_seconds)(max_retries_on_403)
    attempt = 0
    while True:
        if request_delay_seconds > 0:
            await asyncio.sleep(request_delay_seconds)

        try:
            page = await session.fetch(url)
        except Exception:
            return None

        if page.status not in AUTH_ERROR_STATUSES:
            return page

        if attempt >= max_retries_on_403:
            raise SiteBlockedError(f"{url} kept returning {page.status} after {max_retries_on_403} retries")

        await asyncio.sleep(backoff_delays[attempt])
        attempt += 1


async def find_pdm(
    session: AsyncStealthySession,
    city: CityWebsite,
    max_pages_per_site: int,
    max_depth: int,
    http_client: httpx.AsyncClient,
    out_dir: Path,
    min_pdf_pages: int,
    min_keyword_hits: int,
    site_concurrency: int = 1,
    request_delay_seconds: float = 1.0,
    max_retries_on_403: int = 3,
    retry_backoff_seconds: float = 5.0,
) -> PDMResult | None:
    """Crawls `city.website` breadth-first, following only links that look
    like municipal-planning navigation, until it finds a link that names
    both the PDM and "regulamento" whose downloaded document also looks like
    one (see `download_and_validate_pdm`) — a link that matches but fails
    validation is treated as a false positive and the search continues.
    Links that already name the PDM are explored before merely plausible
    ones, so the limited page budget is spent on the most promising path
    first.

    Pages are fetched in rounds of up to `site_concurrency` at a time (1, i.e.
    fully sequential, by default), each request throttled and retried on
    401/403 per `_fetch_page`, to avoid tripping a given site's rate
    limiting. Raises `SiteBlockedError` (see `_fetch_page`) if a page keeps
    returning 401/403 after retries, aborting the crawl for this city.
    """
    base_netloc = urlparse(city.website).netloc

    priority_frontier: deque[tuple[str, int]] = deque()
    frontier: deque[tuple[str, int]] = deque([(city.website, 0)])
    visited: set[str] = set()
    attempted_documents: set[str] = set()

    while priority_frontier or frontier:
        if len(visited) >= max_pages_per_site:
            break

        batch: list[tuple[str, int]] = []
        while (priority_frontier or frontier) and len(batch) < site_concurrency and len(visited) < max_pages_per_site:
            url, depth = priority_frontier.popleft() if priority_frontier else frontier.popleft()

            normalized = _strip_fragment(url)
            if normalized in visited:
                continue
            visited.add(normalized)
            batch.append((url, depth))

        if not batch:
            break

        pages = await asyncio.gather(
            *(
                _fetch_page(session, url, request_delay_seconds, max_retries_on_403, retry_backoff_seconds)
                for url, _ in batch
            )
        )

        for (url, depth), page in zip(batch, pages):
            if page is None or page.status != 200:
                continue

            for anchor in page.css("a"):
                href = anchor.attrib.get("href")
                if not href:
                    continue

                absolute = urljoin(url, href)
                parsed = urlparse(absolute)
                if parsed.scheme not in ("http", "https") or parsed.netloc != base_netloc:
                    continue

                text = anchor.get_all_text(strip=True)

                if is_pdm_regulation_pdf(absolute, text):
                    normalized_doc = _strip_fragment(absolute)
                    if normalized_doc not in attempted_documents:
                        attempted_documents.add(normalized_doc)
                        if await download_and_validate_pdm(
                            http_client, out_dir, city.name, absolute, min_pdf_pages, min_keyword_hits
                        ):
                            return PDMResult(city_id=city.id, source_url=url, pdf_url=absolute)
                    continue

                if (
                    depth >= max_depth
                    or _strip_fragment(absolute) in visited
                    or not is_worth_following(absolute, text)
                ):
                    continue

                queue = priority_frontier if is_high_priority(absolute, text) else frontier
                queue.append((absolute, depth + 1))

    return None
