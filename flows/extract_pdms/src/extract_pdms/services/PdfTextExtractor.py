from pathlib import Path
from uuid import uuid4

import httpx
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from extract_pdms.exception.PdfDownloadError import PdfDownloadError
from extract_pdms.exception.PdfTextExtractionError import PdfTextExtractionError


async def download_pdf(client: httpx.AsyncClient, url: str, dest_dir: Path, timeout_seconds: float) -> Path:
    try:
        response = await client.get(url, timeout=timeout_seconds, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as error:
        raise PdfDownloadError(f"Failed to download {url}: {error}") from error

    dest_path = dest_dir / f"{uuid4().hex}.pdf"
    dest_path.write_bytes(response.content)
    return dest_path


def extract_text(pdf_path: Path) -> str:
    """Extracts and concatenates the text of every page. Returns an empty
    string (rather than raising) when the PDF parses fine but yields no text
    at all -- the caller is expected to treat that as "needs OCR", since
    that's how older, scanned-image regulation PDFs behave.
    """
    try:
        reader = PdfReader(pdf_path)
    except PdfReadError as error:
        raise PdfTextExtractionError(f"{pdf_path.name} is not a readable PDF: {error}") from error

    return "".join(page.extract_text() or "" for page in reader.pages).strip()
