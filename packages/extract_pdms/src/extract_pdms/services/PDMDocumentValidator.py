import asyncio
import logging
import re
import unicodedata
from io import BytesIO
from pathlib import Path

import httpx
from pypdf import PdfReader
from pypdf.errors import PdfReadError

logger = logging.getLogger(__name__)

# Portuguese planning/zoning terms expected to recur throughout a genuine PDM
# regulation. Used as a content-level check on top of the URL/link-text
# rules in PDMLinkMatcher, which can be fooled by an unrelated regulation
# that happens to also mention "plano" or "regulamento" in passing.
_PDM_CONTENT_KEYWORDS = (
    "solo",
    "plano",
    "ordenamento",
    "territorio",
    "urbanistico",
    "edificabilidade",
    "servidao",
    "reserva ecologica",
    "reserva agricola",
)

_DOWNLOAD_TIMEOUT_SECONDS = 180.0
_MAX_DOWNLOAD_RETRIES = 3
_RETRY_DELAY_SECONDS = 5.0

_FILENAME_PATTERN = re.compile(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)\"?")


def _strip_accents(text: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char))


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", _strip_accents(name).lower()).strip("-")
    return slug or "city"


def _filename_from(url: str, response: httpx.Response) -> str:
    match = _FILENAME_PATTERN.search(response.headers.get("content-disposition", ""))
    filename = match.group(1) if match else (Path(httpx.URL(url).path).name or "document")
    return filename if filename.lower().endswith(".pdf") else f"{filename}.pdf"


async def _download(client: httpx.AsyncClient, url: str) -> tuple[bytes, str] | None:
    for attempt in range(1, _MAX_DOWNLOAD_RETRIES + 1):
        try:
            response = await client.get(url, timeout=_DOWNLOAD_TIMEOUT_SECONDS, follow_redirects=True)
            response.raise_for_status()
            return response.content, _filename_from(url, response)
        except httpx.HTTPError as error:
            if attempt == _MAX_DOWNLOAD_RETRIES:
                logger.warning(f"Failed to download {url}: {error}")
                return None
            await asyncio.sleep(_RETRY_DELAY_SECONDS)

    return None


def _looks_like_pdm(content: bytes, min_pages: int, min_keyword_hits: int) -> bool:
    try:
        reader = PdfReader(BytesIO(content))
        page_count = len(reader.pages)
    except PdfReadError:
        logger.info("Rejected candidate: not a readable PDF")
        return False

    if page_count < min_pages:
        logger.info(f"Rejected candidate: {page_count} pages (< {min_pages})")
        return False

    text = " ".join(page.extract_text() or "" for page in reader.pages)
    normalized = _strip_accents(text).lower()
    hits = sum(normalized.count(keyword) for keyword in _PDM_CONTENT_KEYWORDS)
    if hits < min_keyword_hits:
        logger.info(f"Rejected candidate: {page_count} pages but only {hits} keyword hits (< {min_keyword_hits})")
        return False

    logger.info(f"Accepted candidate: {page_count} pages, {hits} keyword hits")
    return True


async def download_and_validate_pdm(
    client: httpx.AsyncClient,
    out_dir: Path,
    city_name: str,
    url: str,
    min_pages: int,
    min_keyword_hits: int,
) -> bool:
    """Downloads `url` into `out_dir/<slugified city_name>/`, keeping the
    file on disk regardless of outcome so rejected candidates can be
    inspected, then checks it actually looks like a PDM regulation: at least
    `min_pages` pages and at least `min_keyword_hits` occurrences of
    Portuguese planning/zoning terms in its text.
    """
    downloaded = await _download(client, url)
    if downloaded is None:
        return False
    content, filename = downloaded

    city_dir = out_dir / _slugify(city_name)
    city_dir.mkdir(parents=True, exist_ok=True)
    (city_dir / filename).write_bytes(content)

    return _looks_like_pdm(content, min_pages, min_keyword_hits)
