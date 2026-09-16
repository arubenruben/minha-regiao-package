from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.RMURepository import update_pdf_urls
from extract_rmues.Settings import settings


@task(name="update_pdf_urls")
async def update_pdf_urls_task(documents: list[PendingDocument]) -> int:
    logger = get_run_logger()

    updated = await update_pdf_urls(settings.database_url, documents)

    logger.info(f"Persisted {updated}/{len(documents)} PDF urls")
    return updated
