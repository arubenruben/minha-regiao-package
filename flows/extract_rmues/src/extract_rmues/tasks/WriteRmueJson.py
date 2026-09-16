from minha_regiao.loader.JsonFileLoader import JsonFileLoader
from prefect import get_run_logger, task

from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.Settings import settings


@task(name="write_rmue_entries_json")
async def write_rmue_page_json_task(entries: list[RMUERegulation]) -> None:
    logger = get_run_logger()

    await JsonFileLoader[RMUERegulation](settings.output_file).load(entries)

    logger.info(f"Wrote {len(entries)} RMUE entries to {settings.output_file}")
