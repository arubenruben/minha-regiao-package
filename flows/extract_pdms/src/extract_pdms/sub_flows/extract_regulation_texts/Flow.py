import asyncio
import tempfile
from pathlib import Path

import httpx
from prefect import flow, get_run_logger

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_pdms.Settings import settings
from extract_pdms.sub_flows.extract_regulation_texts.tasks.ExtractPdfText import (
    EXTRACT_PDF_TEXT_TAG,
    extract_pdf_text_task,
)


@flow(name="extract_regulation_texts")
async def extract_regulation_texts(pdm_records: list[PDMRecord]) -> list[PDMRecord]:
    """Downloads every regulation PDF referenced by `pdm_records` into a
    temp folder (removed once this step finishes, success or not) and
    extracts its text via `extract_pdf_text_task`.
    """
    logger = get_run_logger()

    concurrency = settings.pdf_download_concurrency
    await ensure_concurrency_limit(EXTRACT_PDF_TEXT_TAG, concurrency)

    documents = [document for pdm_record in pdm_records for document in pdm_record.documents]

    with tempfile.TemporaryDirectory(prefix="extract_pdms_") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)

        async with httpx.AsyncClient() as client:
            # asyncio.gather (unlike as_completed) preserves input order, so
            # `extracted` lines up 1:1 with `documents` with no extra bookkeeping.
            extracted = await asyncio.gather(
                *(
                    extract_pdf_text_task(client, tmp_dir, document, settings.pdf_download_timeout_seconds)
                    for document in documents
                )
            )

    extracted_iter = iter(extracted)
    updated_records = [
        pdm_record.model_copy(update={"documents": [next(extracted_iter) for _ in pdm_record.documents]})
        for pdm_record in pdm_records
    ]

    needs_ocr = sum(document.needs_ocr for document in extracted)
    with_text = sum(bool(document.text) for document in extracted)
    logger.info(f"Extracted text for {with_text}/{len(documents)} PDF(s) ({needs_ocr} likely need OCR)")
    return updated_records
