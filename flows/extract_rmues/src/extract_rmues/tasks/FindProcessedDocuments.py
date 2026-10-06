from prefect import get_run_logger, task

from extract_rmues.schema.RMUERegulation import RegulationDocument
from extract_rmues.services.RMURepository import find_processed_documents
from extract_rmues.Settings import settings


@task(name="find_processed_documents", persist_result=False)
async def find_processed_documents_task() -> dict[str, RegulationDocument]:
    """The documents already persisted in the database, keyed by `dre_url` --
    or `{}` when `"database"` isn't in `load_targets`, in which case the
    database plays no part in this run. `persist_result=False`: the result
    carries every document's full text, so it isn't hashed or persisted
    (and a cached copy could never be trusted to still match the database).
    """
    logger = get_run_logger()

    if "database" not in settings.load_targets:
        return {}

    processed_by_url = await find_processed_documents(settings.database_url)

    logger.info(f"Found {len(processed_by_url)} already processed document(s) in the database")
    return processed_by_url
