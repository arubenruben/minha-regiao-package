from prefect import get_run_logger, task
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_pdms.ExtractPDM) so this task, which drives the same shared
# browser session as search_municipio, is capped at the server level.
FETCH_REGULATION_DOCUMENTS_TAG = "pdm-snit-regulamento"


@task(name="fetch_regulation_documents", tags=[FETCH_REGULATION_DOCUMENTS_TAG], persist_result=False)
async def fetch_regulation_documents_task(
    session: AsyncStealthySession, municipio: str, record: dict
) -> PDMRecord | None:
    """Resolves a "Plano Diretor Municipal" series `record` into a
    PDMRecord with its full regulation-document history (metadata only --
    PDF text extraction happens separately, see extract_pdf_text_task).
    Returns None (rather than raising) if the lookup itself fails, so one
    PDM failing doesn't abort the whole flow.
    """
    logger = get_run_logger()

    try:
        documents = await SnitSearch.fetch_pdm_pdf_urls(session, record["Identifier"])
    except Exception:
        logger.exception(f"{municipio}: regulation lookup failed for {record['Title']}")
        return None

    return PDMRecord(
        municipio=municipio,
        title=record["Title"],
        identifier=record["Identifier"],
        documents=[RegulationDocument(**document) for document in documents],
    )
