import logging

from tortoise.transactions import in_transaction

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.PDM import PDM
from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult

logger = logging.getLogger(__name__)


async def find_cities_missing_pdm(db_url: str) -> list[CityWebsite]:
    async with connection(db_url):
        resolved_city_ids = {pdm.city_id async for pdm in PDM.all()}
        cities = [
            CityWebsite(id=city.id, name=city.name, website=city.website)
            async for city in City.filter(website__isnull=False)
            if city.id not in resolved_city_ids
        ]

    return cities


async def persist_pdm_results(db_url: str, results: list[PDMResult]) -> int:
    """Persists each result in its own transaction: a result is either fully
    committed or fully rolled back, but a failure on one city's result must
    not roll back any other city already committed in this same batch.
    """
    async with connection(db_url):
        persisted = 0
        for result in results:
            try:
                async with in_transaction():
                    _, created = await PDM.get_or_create(
                        city_id=result.city_id,
                        defaults={"source_url": result.source_url, "pdf_url": result.pdf_url},
                    )
            except Exception:
                logger.warning(f"Failed to persist PDM for city {result.city_id}", exc_info=True)
                continue

            if created:
                persisted += 1

    return persisted
