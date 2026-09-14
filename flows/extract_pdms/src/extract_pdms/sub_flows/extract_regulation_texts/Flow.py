import tempfile
from pathlib import Path
from typing import cast

from prefect import flow, get_run_logger, unmapped

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
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

    Fan-out is `.map()`, throttled entirely by the task's own tag-based
    concurrency limit (registered below) -- no asyncio.gather/Semaphore here.
    """
    logger = get_run_logger()

    concurrency = settings.pdf_download_concurrency
    await ensure_concurrency_limit(EXTRACT_PDF_TEXT_TAG, concurrency)

    documents = [document for pdm_record in pdm_records for document in pdm_record.documents]

    with tempfile.TemporaryDirectory(prefix="extract_pdms_") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)

        # .map() (like asyncio.gather) preserves input order, so `extracted`
        # lines up 1:1 with `documents` with no extra bookkeeping. The cast
        # works around Prefect's Task.map() stub not special-casing async
        # tasks -- at runtime .result() already returns the awaited
        # RegulationDocument values, not coroutines. Each mapped call opens
        # its own httpx client (see extract_pdf_text_task) rather than
        # sharing one, since Prefect's default task runner executes every
        # mapped async call on its own event loop.
        extracted = cast(
            "list[RegulationDocument]",
            extract_pdf_text_task.map(
                unmapped(tmp_dir), documents, unmapped(settings.pdf_download_timeout_seconds)
            ).result(),
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
