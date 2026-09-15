from pathlib import Path

from prefect import task
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.services.MunicipioPipeline import process_municipio
from extract_pdms.services.OutputStore import OutputStore

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM), sized directly from settings.snit_concurrency
# -- that one setting is the number of municipalities processed in parallel.
PROCESS_MUNICIPIO_TAG = "pdm-municipio-pipeline"

# A municipality's own pipeline only ever has one browser call in flight at
# a time (search, then one fetch per PDM record, awaited sequentially), so
# its session never needs more than a single tab.
_PAGES_PER_MUNICIPIO_SESSION = 1


@task(name="process_municipio", tags=[PROCESS_MUNICIPIO_TAG], persist_result=False)
async def process_municipio_task(
    municipio: str,
    tmp_dir: Path,
    pdf_download_timeout_seconds: float,
    output_store: OutputStore,
) -> None:
    """Resolves one municipality's PDM(s), extracts their regulation text,
    and records the result into `output_store` (persisting it to disk).
    Mapped once per municipality (see extract_pdms.ExtractPDM), each call
    opens its own AsyncStealthySession rather than sharing one: Prefect's
    task runner executes each mapped call on its own fresh event loop in
    its own thread, so a session couldn't be shared across calls anyway.

    `output_store` is a live object shared (and internally locked) across
    every mapped call, so -- like the session below -- it's passed directly
    rather than through a cache-key-hashed parameter; that's safe here
    because persist_result is off.
    """
    async with AsyncStealthySession(
        headless=True, network_idle=True, max_pages=_PAGES_PER_MUNICIPIO_SESSION
    ) as session:
        records = await process_municipio(session, tmp_dir, municipio, pdf_download_timeout_seconds, output_store)

    output_store.record(municipio, records)
