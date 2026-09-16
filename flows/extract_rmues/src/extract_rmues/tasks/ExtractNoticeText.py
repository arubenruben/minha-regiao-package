import tempfile
from pathlib import Path

import httpx
from minha_regiao.gazette.exception.GazetteNoticeNotFoundError import (
    GazetteNoticeNotFoundError,
)
from minha_regiao.gazette.exception.PdfDownloadError import PdfDownloadError
from minha_regiao.gazette.exception.PdfTextExtractionError import PdfTextExtractionError
from minha_regiao.gazette.GazetteSegmenter import find_notice_text_by_heading
from minha_regiao.gazette.PdfTextExtractor import download_pdf, extract_text
from minha_regiao.gazette.StructureParser import parse_structure
from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.schema.RMUERegulation import DocumentStatus, RegulationDocument
from extract_rmues.services.RegulationMetadata import parse_notice_metadata
from extract_rmues.Settings import settings

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_rmues.ExtractRMUEs), sized from settings.pdf_extract_concurrency.
EXTRACT_NOTICE_TEXT_TAG = "rmue-notice-extract"


@task(name="extract_notice_text", tags=[EXTRACT_NOTICE_TEXT_TAG], persist_result=False)
async def extract_notice_text_task(document: PendingDocument) -> RegulationDocument:
    """Downloads `document`'s PDF (already resolved -- `document.pdf_url`
    must not be None) and narrows it down to its own notice's text and
    legal structure, via the same shared minha_regiao.gazette logic
    extract_pdms uses, since a DR-hosted regulation PDF is a raw gazette
    page range that bundles unrelated notices from other
    municipalities/entities published on the same page(s), not a file
    scoped to this one document.

    Always returns a RegulationDocument -- `status` records the outcome
    (see DocumentStatus) rather than raising, so one document's failure
    doesn't abort a batch fanned out via `.map()`. `raw_text`/`structure`
    are left null whenever `status` isn't OK.
    """
    logger = get_run_logger()
    assert document.pdf_url is not None

    base = RegulationDocument(name=document.name, dre_url=document.dre_url, pdf_url=document.pdf_url)

    metadata = parse_notice_metadata(document.name)
    if metadata is None:
        logger.warning(f"Could not parse notice metadata from name {document.name!r}")
        return base.model_copy(update={"status": DocumentStatus.METADATA_UNPARSEABLE})
    heading_phrase, number, year, suffix = metadata

    with tempfile.TemporaryDirectory(prefix="extract_rmues_") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)

        try:
            async with httpx.AsyncClient() as client:
                pdf_path = await download_pdf(client, document.pdf_url, tmp_dir, settings.pdf_extract_timeout_seconds)
        except PdfDownloadError as error:
            logger.error(str(error))
            return base.model_copy(update={"status": DocumentStatus.DOWNLOAD_FAILED})

        try:
            text = extract_text(pdf_path)
        except PdfTextExtractionError as error:
            logger.error(str(error))
            return base.model_copy(update={"status": DocumentStatus.EXTRACTION_FAILED})

    if not text:
        logger.error(f"No extractable text in {document.pdf_url} ({document.name}) -- likely a scanned/old PDF")
        return base.model_copy(update={"status": DocumentStatus.NEEDS_OCR})

    try:
        notice_text = find_notice_text_by_heading(text, heading_phrase, number, year, suffix)
    except GazetteNoticeNotFoundError as error:
        logger.error(f"{error} ({document.pdf_url}, {document.name})")
        return base.model_copy(update={"status": DocumentStatus.NOTICE_NOT_FOUND})

    structure = parse_structure(notice_text)
    return base.model_copy(update={"status": DocumentStatus.OK, "raw_text": notice_text, "structure": structure})
