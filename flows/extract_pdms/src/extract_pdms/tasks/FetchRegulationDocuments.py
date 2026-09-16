from prefect import task
from prefect.tasks import exponential_backoff
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch
from extract_pdms.Settings import settings

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM) so this task, which drives the same shared
# browser session as search_municipio, is capped at the server level.
FETCH_REGULATION_DOCUMENTS_TAG = "pdm-snit-regulamento"


@task(
    name="fetch_regulation_documents",
    tags=[FETCH_REGULATION_DOCUMENTS_TAG],
    retries=settings.snit_task_retries,
    retry_delay_seconds=exponential_backoff(backoff_factor=settings.snit_retry_delay_seconds),
    retry_jitter_factor=settings.snit_retry_jitter_factor,
    persist_result=False,
)
async def fetch_regulation_documents_task(
    session: AsyncStealthySession, municipio: str, record: dict
) -> PDMRecord:
    """Resolves a "Plano Diretor Municipal" series `record` into a
    PDMRecord with its full regulation-document history (metadata only --
    PDF text extraction happens separately, see extract_pdf_text_task).
    Raises on failure -- retried by Prefect with exponential backoff (see
    the task's `retries`/`retry_delay_seconds`, since SNIT's portal
    occasionally serves a broken/anti-bot response under load and spacing
    retries out further each time gives it room to recover) -- rather than
    swallowing the error here; the caller (see
    extract_pdms.services.MunicipioPipeline.process_municipio) is
    responsible for treating an exhausted-retries failure as "this PDM
    couldn't be resolved" so one PDM failing doesn't abort the whole flow.
    Idempotent: a retry just re-runs the same read-only SNIT lookup.
    """
    documents = await SnitSearch.fetch_pdm_pdf_urls(session, record["Identifier"])

    return PDMRecord(
        municipio=municipio,
        title=record["Title"],
        identifier=record["Identifier"],
        documents=[RegulationDocument(**document) for document in documents],
    )
