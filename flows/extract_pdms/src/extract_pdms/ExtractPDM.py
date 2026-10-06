import asyncio
import tempfile
from pathlib import Path
from typing import cast

from prefect import flow, get_run_logger, task, unmapped
from prefect.utilities.annotations import quote
from tqdm import tqdm

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch
from extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_pdms.services.OutputStore import OutputStore
from extract_pdms.services.PDMRepository import find_processed_documents, persist_pdms
from extract_pdms.Settings import settings
from minha_regiao.loader.DatabaseLoader import DatabaseLoader
from minha_regiao.loader.JsonFileLoader import JsonFileLoader
from extract_pdms.tasks.ExtractPdfText import EXTRACT_PDF_TEXT_TAG
from extract_pdms.tasks.FetchRegulationDocuments import FETCH_REGULATION_DOCUMENTS_TAG
from extract_pdms.tasks.ProcessMunicipio import (
    PROCESS_MUNICIPIO_TAG,
    process_municipio_task,
)
from extract_pdms.tasks.SearchMunicipio import SEARCH_MUNICIPIO_TAG


@task(name="find_processed_documents", persist_result=False)
async def find_processed_documents_task() -> dict[str, RegulationDocument]:
    """The documents already persisted in the database, keyed by url -- or
    `{}` when `"database"` isn't in `load_targets`, in which case the
    database plays no part in this run. `persist_result=False`: the result
    carries every document's full text, so it isn't hashed or persisted
    (and a cached copy could never be trusted to still match the database).
    """
    logger = get_run_logger()

    if "database" not in settings.load_targets:
        return {}

    processed_by_url = await find_processed_documents(settings.database_url)

    logger.info(f"Found {len(processed_by_url)} already processed document(s) in the database")
    return processed_by_url


@task(name="persist_pdms", persist_result=False)
async def persist_pdms_task(
    records: list[PDMRecord], processed_by_url: dict[str, RegulationDocument]
) -> None:
    logger = get_run_logger()

    loader = DatabaseLoader[PDMRecord](
        lambda batch: persist_pdms(settings.database_url, batch, processed_by_url)
    )
    await loader.load(records)

    logger.info(
        f"Persisted PDM records for {len(records)} municipalities to the database"
    )


@task(name="write_pdms_json")
async def write_pdms_json_task(records: list[PDMRecord]) -> None:
    logger = get_run_logger()

    await JsonFileLoader[PDMRecord](settings.output_file).load(records)

    logger.info(f"Wrote {len(records)} PDM record(s) to {settings.output_file}")


@flow(
    name="extract_pdms",
    description=(
        "Resolve every municipality's PDM (Plano Diretor Municipal) via SNIT and extract "
        "the text of each regulation PDF in its history."
    ),
)
async def extract_pdms() -> list[PDMRecord]:
    """Fans out one Prefect task run per municipality via `.map()`, capped
    at `settings.snit_concurrency` concurrently running task runs (see the
    tag-based concurrency limit registered below). Each mapped call opens
    its own AsyncStealthySession (see extract_pdms.tasks.ProcessMunicipio):
    Prefect's task runner executes every mapped call on its own fresh event
    loop in its own thread, so a session can't be shared *across* calls the
    way it's shared across steps *within* one municipality's own pipeline
    (see extract_pdms.services.MunicipioPipeline.process_municipio).

    Idempotent and resumable, at the document level: search and fetch
    always re-run for every municipality (they're cheap SNIT metadata
    lookups), but `output_store` persists each regulation document's
    download/text-extraction result -- via its own locked critical
    section, see OutputStore.record -- to `settings.state_file` as soon
    as it's produced, and a document already recorded there (regardless of
    whether it succeeded) is reused on the next run instead of being
    downloaded and parsed again. When `"database"` is in `load_targets` the
    database is a second source of idempotence, for when that state file is
    missing: `processed_by_url` -- every `PDMDocument` already in Postgres,
    read once at the start of the run -- is consulted after `output_store`,
    and a document found in either is reused. A document in
    `processed_by_url` is also not written back to the database (see
    extract_pdms.services.PDMRepository.persist_pdms), so each document
    reaches Postgres exactly once. See
    extract_pdms.services.MunicipioPipeline._extract_documents.
    """
    logger = get_run_logger()

    processed_by_url = await find_processed_documents_task()

    output_store = OutputStore(settings.state_file)

    municipalities = SnitSearch.load_municipalities()

    await ensure_concurrency_limit(PROCESS_MUNICIPIO_TAG, settings.snit_concurrency)
    await ensure_concurrency_limit(SEARCH_MUNICIPIO_TAG, settings.snit_concurrency)
    await ensure_concurrency_limit(
        FETCH_REGULATION_DOCUMENTS_TAG, settings.snit_concurrency
    )
    await ensure_concurrency_limit(
        EXTRACT_PDF_TEXT_TAG, settings.pdf_download_concurrency
    )

    with tempfile.TemporaryDirectory(prefix="extract_pdms_") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)

        futures = process_municipio_task.map(
            municipalities,
            unmapped(tmp_dir),
            unmapped(settings.pdf_download_timeout_seconds),
            unmapped(output_store),
            # `quote` keeps Prefect from re-walking every document in this
            # (potentially huge) dict for each mapped task run -- see
            # extract_pdms.tasks.ProcessMunicipio.
            unmapped(quote(processed_by_url)),
        )

        with tqdm(
            total=len(municipalities), desc="Processing municipalities", unit="city"
        ) as progress:
            for future in futures:
                cast("None", future.result())
                progress.update(1)

    pdm_records = output_store.records

    if "database" in settings.load_targets:
        await persist_pdms_task(pdm_records, processed_by_url)

    if "json" in settings.load_targets:
        await write_pdms_json_task(pdm_records)

    return pdm_records


if __name__ == "__main__":
    asyncio.run(extract_pdms())
