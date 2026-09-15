from prefect import task
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.services import SnitSearch
from extract_pdms.Settings import settings

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM) so this task, which drives a shared browser
# session, is capped at the server level.
SEARCH_MUNICIPIO_TAG = "pdm-snit-search"


@task(
    name="search_municipio",
    tags=[SEARCH_MUNICIPIO_TAG],
    retries=settings.snit_task_retries,
    retry_delay_seconds=settings.snit_retry_delay_seconds,
    persist_result=False,
)
async def search_municipio_task(session: AsyncStealthySession, municipio: str) -> list[dict]:
    """Searches SNIT for every series/service record registered against
    `municipio`. Raises on failure -- retried by Prefect (see the task's
    `retries`, since SNIT's portal occasionally serves a broken response
    under load) -- rather than swallowing the error here; the caller (see
    extract_pdms.services.MunicipioPipeline.process_municipio) is
    responsible for treating an exhausted-retries failure as "no records"
    so one municipality's search going down doesn't abort the whole flow.
    """
    payload = await SnitSearch.search_municipio(session, municipio)
    return payload.get("results", [])
