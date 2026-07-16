import logging

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.District import District
from minha_regiao.flows.extract_districts.schema.DistrictReference import DistrictReference
from minha_regiao.flows.extract_districts.services.DistrictReferenceLoader import match_district_name

logger = logging.getLogger(__name__)


async def persist_districts(db_url: str, references: list[DistrictReference]) -> int:
    async with connection(db_url):
        persisted = 0
        for reference in references:
            await District.get_or_create(name=reference.name)
            persisted += 1

    return persisted


async def fetch_district_wikipedia_urls(db_url: str) -> dict[str, str | None]:
    async with connection(db_url):
        return {district.name: district.wikipedia_url for district in await District.all()}


async def assign_city_districts(db_url: str, references: list[DistrictReference]) -> int:
    async with connection(db_url):
        updated = 0
        unmatched = []

        for city in await City.all():
            district_name = match_district_name(city.ine_code, references)
            district = await District.get_or_none(name=district_name) if district_name else None

            if district is None:
                unmatched.append(city.name)
                continue

            if city.district_id != district.id:
                city.district_id = district.id
                await city.save(update_fields=["district_id"])
                updated += 1

    if unmatched:
        logger.warning(f"Could not resolve district for: {sorted(unmatched)}")

    return updated
