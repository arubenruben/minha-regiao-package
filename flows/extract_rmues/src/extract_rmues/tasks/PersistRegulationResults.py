from minha_regiao.loader.DatabaseLoader import DatabaseLoader
from prefect import get_run_logger, task

from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.services.RMURepository import persist_regulation_results
from extract_rmues.Settings import settings


@task(name="persist_regulation_results")
async def persist_regulation_results_task(entries: list[RMUERegulation]) -> int:
    logger = get_run_logger()

    persisted_count = 0

    async def _persist(records: list[RMUERegulation]) -> int:
        nonlocal persisted_count
        persisted, unmatched_cities = await persist_regulation_results(settings.database_url, records)

        logger.info(f"Persisted {persisted} document result(s) (pdf_url/status/raw_text/structure) to the database")
        if unmatched_cities:
            logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")

        persisted_count = persisted
        return persisted

    await DatabaseLoader[RMUERegulation](_persist).load(entries)
    return persisted_count
