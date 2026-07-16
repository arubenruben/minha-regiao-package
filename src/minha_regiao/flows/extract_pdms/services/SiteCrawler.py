from collections import deque
from urllib.parse import urljoin, urlparse

from scrapling.fetchers import AsyncStealthySession

from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult
from minha_regiao.flows.extract_pdms.services.PDMLinkMatcher import (
    is_high_priority,
    is_pdm_regulation_pdf,
    is_worth_following,
)


def _strip_fragment(url: str) -> str:
    return url.split("#", 1)[0].rstrip("/")


async def find_pdm(
    session: AsyncStealthySession,
    city: CityWebsite,
    max_pages_per_site: int,
    max_depth: int,
) -> PDMResult | None:
    """Crawls `city.website` breadth-first, following only links that look
    like municipal-planning navigation, until it finds a PDF that names both
    the PDM and "regulamento". Links that already name the PDM are explored
    before merely plausible ones, so the limited page budget is spent on the
    most promising path first.
    """
    base_netloc = urlparse(city.website).netloc

    priority_frontier: deque[tuple[str, int]] = deque()
    frontier: deque[tuple[str, int]] = deque([(city.website, 0)])
    visited: set[str] = set()

    while priority_frontier or frontier:
        if len(visited) >= max_pages_per_site:
            break

        url, depth = priority_frontier.popleft() if priority_frontier else frontier.popleft()

        normalized = _strip_fragment(url)
        if normalized in visited:
            continue
        visited.add(normalized)

        try:
            page = await session.fetch(url)
        except Exception:
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
                return PDMResult(city_id=city.id, source_url=url, pdf_url=absolute)

            if depth >= max_depth or _strip_fragment(absolute) in visited or not is_worth_following(absolute, text):
                continue

            queue = priority_frontier if is_high_priority(absolute, text) else frontier
            queue.append((absolute, depth + 1))

    return None
