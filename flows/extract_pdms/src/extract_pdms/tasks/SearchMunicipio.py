from prefect import get_run_logger, task
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.services import SnitSearch

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM) so this task, which drives a shared browser
# session, is capped at the server level.
SEARCH_MUNICIPIO_TAG = "pdm-snit-search"


@task(name="search_municipio", tags=[SEARCH_MUNICIPIO_TAG], persist_result=False)
async def search_municipio_task(session: AsyncStealthySession, municipio: str) -> list[dict]:
    """Searches SNIT for every series/service record registered against
    `municipio`. Returns an empty list (rather than raising) on failure, so
    one municipality's search going down doesn't abort the whole flow.
    """
    logger = get_run_logger()

    try:
        payload = await SnitSearch.search_municipio(session, municipio)
    except Exception:
        logger.exception(f"{municipio}: SNIT search failed")
        return []

    return payload.get("results", [])
