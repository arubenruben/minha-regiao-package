from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.RMURepository import find_documents_missing_pdf_url
from extract_rmues.Settings import settings


@task(name="find_documents_missing_pdf_url")
async def find_pending_documents_task() -> list[PendingDocument]:
    logger = get_run_logger()

    documents = await find_documents_missing_pdf_url(settings.database_url)

    logger.info(f"Found {len(documents)} documents missing a PDF url")
    return documents
