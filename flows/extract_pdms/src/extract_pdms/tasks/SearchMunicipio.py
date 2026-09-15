from prefect import task
from prefect.tasks import exponential_backoff
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
    retry_delay_seconds=exponential_backoff(backoff_factor=settings.snit_retry_delay_seconds),
    retry_jitter_factor=settings.snit_retry_jitter_factor,
    persist_result=False,
)
async def search_municipio_task(session: AsyncStealthySession, municipio: str) -> list[dict]:
    """Searches SNIT for every series/service record registered against
    `municipio`. Raises on failure -- retried by Prefect with exponential
    backoff (see the task's `retries`/`retry_delay_seconds`, since SNIT's
    portal occasionally serves a broken/anti-bot response under load and
    spacing retries out further each time gives it room to recover) --
    rather than swallowing the error here; the caller (see
    extract_pdms.services.MunicipioPipeline.process_municipio) is
    responsible for treating an exhausted-retries failure as "no records"
    so one municipality's search going down doesn't abort the whole flow.
    Idempotent: a retry just re-runs the same read-only SNIT lookup.
    """
    payload = await SnitSearch.search_municipio(session, municipio)
    return payload.get("results", [])
