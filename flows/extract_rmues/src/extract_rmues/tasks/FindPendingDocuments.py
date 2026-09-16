from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.services.RMURepository import find_documents_missing_pdf_url
from extract_rmues.Settings import settings


def _pending_documents_from_entries(
    entries: list[RMUERegulation],
) -> list[PendingDocument]:
    return [
        PendingDocument(
            table=table,
            municipality=entry.municipality,
            name=document.name,
            dre_url=document.dre_url,
        )
        for entry in entries
        for table, documents in (
            ("rmue", entry.urbanization_documents),
            ("fee_regulation", entry.fee_documents),
        )
        for document in documents
    ]


@task(name="find_documents_missing_pdf_url")
async def find_pending_documents_task(
    entries: list[RMUERegulation],
) -> list[PendingDocument]:
    """When `"database"` is in `load_targets`, resolves PDF urls for every
    document still missing one in Postgres -- including backlog from prior
    runs. Otherwise falls back to this run's freshly-parsed `entries`
    in-memory, so the flow needs no database at all.
    """
    logger = get_run_logger()

    if "database" in settings.load_targets:
        documents = await find_documents_missing_pdf_url(settings.database_url)
    else:
        documents = _pending_documents_from_entries(entries)

    logger.info(f"Found {len(documents)} documents missing a PDF url")
    return documents
