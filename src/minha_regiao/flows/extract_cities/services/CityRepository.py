from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.flows.extract_cities.schema.CityContacts import CityContacts


async def persist_cities(db_url: str, contacts: list[CityContacts]) -> int:
    async with connection(db_url):
        persisted = 0
        for contact in contacts:
            await City.update_or_create(
                ine_code=contact.ine_code,
                defaults={
                    "name": contact.municipality,
                    "email": contact.town_hall.email,
                    "website": contact.town_hall.website,
                },
            )
            persisted += 1

    return persisted
