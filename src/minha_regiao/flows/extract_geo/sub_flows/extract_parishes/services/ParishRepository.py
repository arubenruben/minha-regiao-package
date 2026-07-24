from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.Parish import Parish, ParishEra
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.schema.ParishCityLink import ParishCityLink


async def persist_parishes(db_url: str, links: list[ParishCityLink], era: ParishEra) -> int:
    async with connection(db_url):
        persisted = 0
        for link in links:
            await Parish.update_or_create(
                ine_code=link.ine_code,
                era=era,
                defaults={"name": link.name, "city_id": link.city.id},
            )
            persisted += 1

    return persisted
