import asyncio
import json
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger
from scrapling.fetchers import AsyncStealthySession
from tqdm import tqdm

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.services import SnitSearch
from extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_pdms.services.MunicipioPipeline import process_municipio
from extract_pdms.Settings import settings
from extract_pdms.tasks.ExtractPdfText import EXTRACT_PDF_TEXT_TAG
from extract_pdms.tasks.FetchRegulationDocuments import FETCH_REGULATION_DOCUMENTS_TAG
from extract_pdms.tasks.SearchMunicipio import SEARCH_MUNICIPIO_TAG


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
    phase: each municipality's search -> resolve -> extract-text pipeline
    (see extract_pdms.services.MunicipioPipeline.process_municipio) is
    fanned out concurrently via asyncio.as_completed, so PDF downloads for
    an already-resolved municipality run while SNIT lookups are still in
    flight for others, rather than waiting for every municipality to be
    resolved before any text extraction starts.

    process_municipio's search/fetch calls share one AsyncStealthySession
    (launching a browser per municipality would be far too expensive), so
    they're called directly rather than via .submit()/.map() -- this keeps
    everything on this flow's own event loop, which is safe for a shared
    live resource, and still fully respects their tag-based concurrency
    limits (registered below): the limit is enforced inside task execution
    itself, regardless of whether a task is submitted or called directly.
    Its PDF extraction step doesn't share a live resource, so it uses
    `.map()` instead.
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
            pipeline_coros = [
                process_municipio(session, tmp_dir, municipio, settings.pdf_download_timeout_seconds)
                for municipio in municipalities
            ]

            pdm_records: list[PDMRecord] = []
            with tqdm(total=len(municipalities), desc="Processing municipalities", unit="city") as progress:
                for coro in asyncio.as_completed(pipeline_coros):
                    pdm_records.extend(await coro)
                    progress.update(1)

    _write_output(pdm_records)
    logger.info(f"Wrote {len(pdm_records)} PDM record(s) to {settings.output_file}")

    return pdm_records


if __name__ == "__main__":
    asyncio.run(extract_pdms())
