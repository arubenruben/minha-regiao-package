import asyncio
import json
import tempfile
from pathlib import Path
from typing import cast

from prefect import flow, get_run_logger, unmapped
from scrapling.fetchers import AsyncStealthySession
from tqdm import tqdm

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch
from extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_pdms.services.Logging import configure_file_logging
from extract_pdms.Settings import settings
from extract_pdms.tasks.ExtractPdfText import (
    EXTRACT_PDF_TEXT_TAG,
    extract_pdf_text_task,
)
from extract_pdms.tasks.FetchRegulationDocuments import (
    FETCH_REGULATION_DOCUMENTS_TAG,
    fetch_regulation_documents_task,
)
from extract_pdms.tasks.SearchMunicipio import (
    SEARCH_MUNICIPIO_TAG,
    search_municipio_task,
)

configure_file_logging(settings.logs_dir)


def _write_output(pdm_records: list[PDMRecord]) -> None:
    settings.output_file.parent.mkdir(parents=True, exist_ok=True)
    settings.output_file.write_text(
        json.dumps(
            [pdm_record.model_dump(mode="json") for pdm_record in pdm_records],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


@flow(
    name="extract_pdms",
    description=(
        "Resolve every municipality's PDM (Plano Diretor Municipal) via SNIT and extract "
        "the text of each regulation PDF in its history."
    ),
)
async def extract_pdms() -> list[PDMRecord]:
    """Streams the whole pipeline per municipality instead of batching by
    phase: for each municipality (processed concurrently, capped by its own
    tag-based concurrency limit), search SNIT, resolve its PDM's regulation
    documents, then immediately extract those documents' text -- so PDF
    downloads for an already-resolved municipality run concurrently with
    SNIT lookups still in flight for others, rather than waiting for every
    municipality to be resolved before any text extraction starts.

    Two different concurrency mechanisms are combined deliberately:
      - search_municipio_task/fetch_regulation_documents_task share one
        AsyncStealthySession (launching a browser per municipality would be
        far too expensive), so they're called directly and the per-
        municipality pipelines are fanned out with asyncio.as_completed --
        this keeps everything on this flow's own event loop, which is safe
        for a shared live resource. It does NOT skip their tag-based
        concurrency limits (registered below): the limit is enforced inside
        task execution itself, regardless of whether a task is submitted or
        called directly.
      - extract_pdf_text_task doesn't share a live resource (it opens its
        own httpx client per call), so within each municipality's pipeline
        its documents are fanned out with `.map()`, safe to call from
        inside a coroutine that's itself one of many running concurrently.
    """
    logger = get_run_logger()

    municipalities = SnitSearch.load_municipalities()

    snit_concurrency = settings.snit_concurrency
    await ensure_concurrency_limit(SEARCH_MUNICIPIO_TAG, snit_concurrency)
    await ensure_concurrency_limit(FETCH_REGULATION_DOCUMENTS_TAG, snit_concurrency)
    await ensure_concurrency_limit(EXTRACT_PDF_TEXT_TAG, settings.pdf_download_concurrency)

    with tempfile.TemporaryDirectory(prefix="extract_pdms_") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)

        async with AsyncStealthySession(headless=True, network_idle=True, max_pages=snit_concurrency) as session:

            async def process_municipio(municipio: str) -> list[PDMRecord]:
                records = await search_municipio_task(session, municipio)
                pdm_series = [
                    record
                    for record in records
                    if record["Type"] == "series" and record["Title"].startswith(SnitSearch.PDM_TITLE_PREFIX)
                ]

                results: list[PDMRecord] = []
                for record in pdm_series:
                    pdm_record = await fetch_regulation_documents_task(session, municipio, record)
                    if pdm_record is None:
                        continue

                    if not pdm_record.documents:
                        results.append(pdm_record)
                        continue

                    extracted = cast(
                        "list[RegulationDocument]",
                        extract_pdf_text_task.map(
                            unmapped(tmp_dir),
                            pdm_record.documents,
                            unmapped(settings.pdf_download_timeout_seconds),
                        ).result(),
                    )
                    results.append(pdm_record.model_copy(update={"documents": extracted}))

                return results

            pdm_records: list[PDMRecord] = []
            with tqdm(total=len(municipalities), desc="Processing municipalities", unit="city") as progress:
                for coro in asyncio.as_completed([process_municipio(m) for m in municipalities]):
                    pdm_records.extend(await coro)
                    progress.update(1)

    _write_output(pdm_records)
    logger.info(f"Wrote {len(pdm_records)} PDM record(s) to {settings.output_file}")

    return pdm_records


if __name__ == "__main__":
    asyncio.run(extract_pdms())
