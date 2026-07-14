from tortoise.models import Model

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.FeeRegulation import FeeRegulation
from minha_regiao.entity.RMUE import RMUE
from minha_regiao.flows.extract_cities.services.FuzzyMatch import resolve_name
from minha_regiao.flows.extract_rmues.schema.RMUERegulation import RegulationDocument, RMUERegulation
from minha_regiao.flows.extract_rmues.services.RegulationMetadata import extract_year, is_complete


async def _persist_documents(model: type[Model], city: City, documents: list[RegulationDocument]) -> tuple[int, list[str]]:
    persisted = 0
    skipped = []

    for document in documents:
        year = extract_year(document.name)
        if year is None:
            skipped.append(document.name)
            continue

        _, created = await model.get_or_create(
            city=city,
            dre_url=document.dre_url,
            defaults={"year": year, "name": document.name, "is_complete": is_complete(document.name)},
        )
        if created:
            persisted += 1

    return persisted, skipped


async def persist_rmue_regulations(db_url: str, entries: list[RMUERegulation]) -> tuple[int, list[str], list[str]]:
    """Matches each entry's municipality name against `City` and upserts one
    `RMUE`/`FeeRegulation` row per document. Returns the number of rows
    created, the municipality names that couldn't be matched, and the
    document names whose year couldn't be parsed (and were therefore skipped).
    """
    async with connection(db_url):
        cities_by_name = {city.name: city async for city in City.all()}

        persisted = 0
        unmatched_cities = []
        skipped_documents = []

        for entry in entries:
            matched_name = resolve_name(entry.municipality, cities_by_name)
            if matched_name is None:
                unmatched_cities.append(entry.municipality)
                continue

            city = cities_by_name[matched_name]

            rmue_persisted, rmue_skipped = await _persist_documents(RMUE, city, entry.urbanization_documents)
            fee_persisted, fee_skipped = await _persist_documents(FeeRegulation, city, entry.fee_documents)

            persisted += rmue_persisted + fee_persisted
            skipped_documents.extend(rmue_skipped + fee_skipped)

    return persisted, unmatched_cities, skipped_documents
