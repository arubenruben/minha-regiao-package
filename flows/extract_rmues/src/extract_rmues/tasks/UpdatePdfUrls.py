from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.RMURepository import update_pdf_urls
from extract_rmues.Settings import settings


@task(name="update_pdf_urls")
async def update_pdf_urls_task(documents: list[PendingDocument]) -> int:
    """Writes resolved PDF urls back to Postgres when `"database"` is in
    `load_targets`. Otherwise there's no persisted row to update, so this
    just counts how many were resolved.
    """
    logger = get_run_logger()

    if "database" in settings.load_targets:
        updated = await update_pdf_urls(settings.database_url, documents)
        logger.info(f"Persisted {updated}/{len(documents)} PDF urls")
    else:
        updated = sum(1 for document in documents if document.pdf_url is not None)
        logger.info(f"Resolved {updated}/{len(documents)} PDF urls (not persisted)")

    return updated
