from tortoise.transactions import in_transaction

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.PDM import PDM
from minha_regiao.flows.extract_pdms.schema.CityWebsite import CityWebsite
from minha_regiao.flows.extract_pdms.schema.PDMResult import PDMResult


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
    async with connection(db_url):
        persisted = 0
        async with in_transaction():
            for result in results:
                _, created = await PDM.get_or_create(
                    city_id=result.city_id,
                    defaults={"source_url": result.source_url, "pdf_url": result.pdf_url},
                )
                if created:
                    persisted += 1

    return persisted
