import asyncio

from prefect import flow, get_run_logger
from scrapling.fetchers import AsyncStealthySession
from tqdm import tqdm

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_pdms.Settings import settings
from extract_pdms.sub_flows.find_pdms_in_snit.services import SnitSearch
from extract_pdms.sub_flows.find_pdms_in_snit.tasks.FetchRegulationDocuments import (
    FETCH_REGULATION_DOCUMENTS_TAG,
    fetch_regulation_documents_task,
)
from extract_pdms.sub_flows.find_pdms_in_snit.tasks.SearchMunicipio import (
    SEARCH_MUNICIPIO_TAG,
    search_municipio_task,
)


@flow(name="find_pdms_in_snit")
async def find_pdms_in_snit(municipalities: list[str]) -> list[PDMRecord]:
    """Searches SNIT for every municipality's series records, keeps the ones
    that are a "Plano Diretor Municipal", and resolves each into a
    PDMRecord with its full regulation-document history (metadata only --
    PDF text extraction is a separate sub-flow, see extract_regulation_texts).

    Both tasks here share one AsyncStealthySession (launching a browser per
    municipality would be far too expensive), so fan-out can't go through
    Task.map()/.submit(): Prefect's default task runner executes every
    mapped/submitted async call on its own thread with its own fresh event
    loop (ThreadPoolTaskRunner.submit -> asyncio.run(...) per call), and a
    session shared across those breaks (confirmed against extract_pdf_text_task
    before it was changed to open its own client per call -- see that
    sub-flow's history). Calling the tasks directly and fanning out with
    asyncio.gather/as_completed keeps everything on this flow's own event
    loop, which is safe. This does NOT skip Prefect's tag-based concurrency
    limits (registered below): the limit is enforced inside task execution
    itself, regardless of whether the task was submitted or called directly.
    """
    logger = get_run_logger()

    concurrency = settings.snit_concurrency
    await ensure_concurrency_limit(SEARCH_MUNICIPIO_TAG, concurrency)
    await ensure_concurrency_limit(FETCH_REGULATION_DOCUMENTS_TAG, concurrency)

    async with AsyncStealthySession(headless=True, network_idle=True, max_pages=concurrency) as session:

        async def search(municipio: str) -> tuple[str, list[dict]]:
            return municipio, await search_municipio_task(session, municipio)

        pdm_series: list[tuple[str, dict]] = []
        with tqdm(total=len(municipalities), desc="Searching SNIT", unit="city") as progress:
            for coro in asyncio.as_completed([search(m) for m in municipalities]):
                municipio, records = await coro
                pdm_series.extend(
                    (municipio, record)
                    for record in records
                    if record["Type"] == "series" and record["Title"].startswith(SnitSearch.PDM_TITLE_PREFIX)
                )
                progress.update(1)

        pdm_records: list[PDMRecord] = []
        with tqdm(total=len(pdm_series), desc="Fetching PDM regulations", unit="pdm") as progress:
            fetch_coros = [
                fetch_regulation_documents_task(session, municipio, record) for municipio, record in pdm_series
            ]
            for coro in asyncio.as_completed(fetch_coros):
                pdm_record = await coro
                if pdm_record is not None:
                    pdm_records.append(pdm_record)
                progress.update(1)

    logger.info(f"Resolved {len(pdm_records)} PDM(s) across {len(municipalities)} municipalities")
    return pdm_records
