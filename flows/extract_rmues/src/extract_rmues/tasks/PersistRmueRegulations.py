from minha_regiao.loader.DatabaseLoader import DatabaseLoader
from prefect import get_run_logger, task

from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.services.RMURepository import persist_rmue_regulations
from extract_rmues.Settings import settings


@task(name="persist_rmue_regulations")
async def persist_rmue_page_task(entries: list[RMUERegulation]) -> int:
    logger = get_run_logger()

    persisted_count = 0

    async def _persist(records: list[RMUERegulation]) -> int:
        nonlocal persisted_count
        persisted, unmatched_cities, skipped_documents = await persist_rmue_regulations(
            settings.database_url, records
        )

        logger.info(f"Persisted {persisted} regulation documents")
        if unmatched_cities:
            logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")
        if skipped_documents:
            logger.warning(f"Skipped documents (no parseable year): {sorted(skipped_documents)}")

        persisted_count = persisted
        return persisted

    await DatabaseLoader[RMUERegulation](_persist).load(entries)
    return persisted_count
