from pathlib import Path

import httpx
from prefect import get_run_logger, task

from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services.PdfTextExtractor import (
    PdfDownloadError,
    PdfTextExtractionError,
    download_pdf,
    extract_text,
)

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM).
EXTRACT_PDF_TEXT_TAG = "pdm-pdf-extract"


@task(name="extract_pdf_text", tags=[EXTRACT_PDF_TEXT_TAG], persist_result=False)
async def extract_pdf_text_task(
    tmp_dir: Path, document: RegulationDocument, timeout_seconds: float
) -> RegulationDocument:
    """Downloads `document`'s PDF into `tmp_dir` and extracts its text,
    returning an updated copy. A PDF that downloads and parses fine but
    yields no text at all is an old, scanned regulation that needs OCR --
    until OCR support exists, that's only logged as an error; the document
    is returned with `needs_ocr=True` and no text rather than being dropped.
    A download or parse failure is likewise only logged, returning the
    document unchanged.

    Opens its own httpx.AsyncClient rather than taking a shared one: when
    this task is fanned out via `.map()`, Prefect's default task runner
    executes each call in its own thread with its own fresh event loop (see
    ThreadPoolTaskRunner.submit), so a client shared across calls via
    unmapped() gets used from a different loop than the one it was created
    on and breaks (confirmed: raises "RuntimeError: Event loop is closed").
    """
    logger = get_run_logger()
    url = str(document.url)

    try:
        async with httpx.AsyncClient() as client:
            pdf_path = await download_pdf(client, url, tmp_dir, timeout_seconds)
    except PdfDownloadError as error:
        logger.error(str(error))
        return document

    try:
        text = extract_text(pdf_path)
    except PdfTextExtractionError as error:
        logger.error(str(error))
        return document
    finally:
        pdf_path.unlink(missing_ok=True)

    if not text:
        # TODO: once OCR support exists, route through it here instead of
        # just logging.
        logger.error(
            f"No extractable text in {url} ({document.doc_type} "
            f"{document.number}/{document.year}) -- likely a scanned/old PDF that needs OCR"
        )
        return document.model_copy(update={"needs_ocr": True})

    return document.model_copy(update={"text": text})
