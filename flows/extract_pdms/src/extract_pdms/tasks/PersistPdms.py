from minha_regiao.loader.DatabaseLoader import DatabaseLoader
from prefect import get_run_logger, task

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services.PDMRepository import persist_pdms
from extract_pdms.Settings import settings


@task(name="persist_pdms", persist_result=False)
async def persist_pdms_task(
    records: list[PDMRecord], processed_by_url: dict[str, RegulationDocument]
) -> None:
    """Writes `records` to the database -- called once per municipality, as
    the last step of its own `process_municipio_task` (see
    extract_pdms.tasks.ProcessMunicipio), with just that municipality's
    records, each already carrying all of its documents in their final state.

    `persist_result=False`: `processed_by_url` carries every already
    persisted document's full text, so it isn't hashed or persisted.
    """
    logger = get_run_logger()

    persisted = 0

    async def _persist(batch: list[PDMRecord]) -> int:
        nonlocal persisted
        persisted = await persist_pdms(settings.database_url, batch, processed_by_url)
        return persisted

    await DatabaseLoader[PDMRecord](_persist).load(records)

    logger.info(f"Persisted {persisted}/{len(records)} PDM record(s) to the database")
