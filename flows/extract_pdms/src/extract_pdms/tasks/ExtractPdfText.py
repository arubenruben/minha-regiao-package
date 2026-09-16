from pathlib import Path

import httpx
from prefect import get_run_logger, task

from extract_pdms.schema.RegulationDocument import DocumentStatus, RegulationDocument
from extract_pdms.services.StructureParser import parse_structure
from minha_regiao.gazette.exception.GazetteNoticeNotFoundError import GazetteNoticeNotFoundError
from minha_regiao.gazette.exception.PdfDownloadError import PdfDownloadError
from minha_regiao.gazette.exception.PdfTextExtractionError import PdfTextExtractionError
from minha_regiao.gazette.GazetteSegmenter import find_notice_text
from minha_regiao.gazette.PdfTextExtractor import download_pdf, extract_text

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM).
EXTRACT_PDF_TEXT_TAG = "pdm-pdf-extract"


@task(name="extract_pdf_text", tags=[EXTRACT_PDF_TEXT_TAG], persist_result=False)
async def extract_pdf_text_task(
    tmp_dir: Path, document: RegulationDocument, timeout_seconds: float
) -> RegulationDocument:
    """Downloads `document`'s PDF into `tmp_dir` and extracts its text,
    returning an updated copy with `status` set to record the outcome --
    `text` and `structure` are left null whenever `status` isn't OK, since
    there's nothing reliable to report for a file that couldn't be read or
    couldn't be narrowed to this document's own notice.

    The downloaded PDF is a raw Diário da República page range, not a file
    scoped to this one regulation -- it can bundle unrelated notices from
    other municipalities/entities published on the same page(s) (see
    minha_regiao.gazette.GazetteSegmenter). So the page's full extracted
    text is immediately narrowed, via `find_notice_text`, to just the
    notice matching `document`'s own doc_type/number/year; `text` is that
    narrowed notice, not the whole page. `structure` is that same narrowed
    text broken down into its Parte/Título/Capítulo/Secção/Subsecção/Artigo
    hierarchy (see extract_pdms.services.StructureParser).

    A PDF that downloads and parses fine but yields no text at all is an
    old, scanned regulation that needs OCR -- until OCR support exists,
    that's recorded as NEEDS_OCR rather than attempted. A download or parse
    failure is recorded as DOWNLOAD_FAILED / EXTRACTION_FAILED respectively.
    Failure to locate this document's own notice inside the page (an
    unmapped doc_type, or no matching header line -- the matching mechanism
    is heuristic, not proven exhaustive) is recorded as NOTICE_NOT_FOUND
    rather than silently falling back to the whole, possibly-unrelated,
    page text.

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
        return document.model_copy(update={"status": DocumentStatus.DOWNLOAD_FAILED, "text": None})

    try:
        text = extract_text(pdf_path)
    except PdfTextExtractionError as error:
        logger.error(str(error))
        return document.model_copy(update={"status": DocumentStatus.EXTRACTION_FAILED, "text": None})
    finally:
        pdf_path.unlink(missing_ok=True)

    if not text:
        # TODO: once OCR support exists, route through it here instead of
        # just recording NEEDS_OCR.
        logger.error(
            f"No extractable text in {url} ({document.doc_type} "
            f"{document.number}/{document.year}) -- likely a scanned/old PDF that needs OCR"
        )
        return document.model_copy(update={"status": DocumentStatus.NEEDS_OCR, "text": None})

    try:
        notice_text = find_notice_text(text, document.doc_type, document.number, document.year, document.suffix)
    except GazetteNoticeNotFoundError as error:
        logger.error(
            f"{error} ({url}, {document.doc_type} {document.number}/{document.year}) -- "
            "matching mechanism needs to be extended for this document"
        )
        return document.model_copy(update={"status": DocumentStatus.NOTICE_NOT_FOUND, "text": None})

    structure = parse_structure(notice_text)
    return document.model_copy(update={"status": DocumentStatus.OK, "text": notice_text, "structure": structure})
