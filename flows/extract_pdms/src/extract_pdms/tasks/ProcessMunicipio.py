from pathlib import Path

from prefect import task
from prefect.utilities.annotations import quote
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services.MunicipioPipeline import process_municipio
from extract_pdms.services.OutputStore import OutputStore
from extract_pdms.Settings import settings
from extract_pdms.tasks.PersistPdms import persist_pdms_task

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM), sized directly from settings.snit_concurrency
# -- that one setting is the number of municipalities processed in parallel.
PROCESS_MUNICIPIO_TAG = "pdm-municipio-pipeline"

# A municipality's own pipeline only ever has one browser call in flight at
# a time (search, then one fetch per PDM record, awaited sequentially), so
# its session never needs more than a single tab.
_PAGES_PER_MUNICIPIO_SESSION = 1


def _has_records_to_persist(
    records: list[PDMRecord], processed_by_url: dict[str, RegulationDocument]
) -> bool:
    """False when there is nothing to write: no records, or every document of
    every record already in `processed_by_url`. A record with no documents at
    all still counts -- its `PDM` row (title/identifier/source_url) is itself
    something to write, and `processed_by_url` can't say whether it exists.
    """
    return any(
        not record.documents
        or any(str(document.url) not in processed_by_url for document in record.documents)
        for record in records
    )


@task(name="process_municipio", tags=[PROCESS_MUNICIPIO_TAG], persist_result=False)
async def process_municipio_task(
    municipio: str,
    tmp_dir: Path,
    pdf_download_timeout_seconds: float,
    output_store: OutputStore,
    processed_by_url: dict[str, RegulationDocument],
) -> None:
    """Resolves one municipality's PDM(s), extracts their regulation text,
    records the result into `output_store` (persisting it to disk) and, as
    the last step, writes it to the database when `"database"` is in
    `load_targets`. `process_municipio` only returns once every one of the
    municipality's documents has reached its final state, so the database
    never sees a document half-processed; the persist is skipped when
    there's nothing to write (see `_has_records_to_persist`). Several of
    these task runs persist at the same time, from different threads --
    `minha_regiao.database.DatabaseManager.connection` serialises them.
    Mapped once per municipality (see extract_pdms.ExtractPDM), each call
    opens its own AsyncStealthySession rather than sharing one: Prefect's
    task runner executes each mapped call on its own fresh event loop in
    its own thread, so a session couldn't be shared across calls anyway.

    `output_store` is a live object shared (and internally locked) across
    every mapped call, so -- like the session below -- it's passed directly
    rather than through a cache-key-hashed parameter; that's safe here
    because persist_result is off.

    `processed_by_url` is the documents already persisted in the database
    (see extract_pdms.ExtractPDM.find_processed_documents_task), the same
    read-only dict for every mapped call; the caller wraps it in `quote` so
    Prefect doesn't walk its whole contents again for each task run.
    """
    async with AsyncStealthySession(
        headless=True, network_idle=True, max_pages=_PAGES_PER_MUNICIPIO_SESSION
    ) as session:
        records = await process_municipio(
            session, tmp_dir, municipio, pdf_download_timeout_seconds, output_store, processed_by_url
        )

    output_store.record(municipio, records)

    if "database" in settings.load_targets and _has_records_to_persist(records, processed_by_url):
        # `quote`: same reason as in ExtractPDM -- skips Prefect walking
        # every document in `processed_by_url` again for this nested task run.
        await persist_pdms_task(records, quote(processed_by_url))
