import asyncio

from prefect import flow, task, get_run_logger

from minha_regiao.flows.extract_districts.Settings import settings
from minha_regiao.flows.extract_districts.schema.DistrictReference import DistrictReference
from minha_regiao.flows.extract_districts.services.DistrictReferenceLoader import load_district_references
from minha_regiao.flows.extract_districts.services.DistrictRepository import assign_city_districts, persist_districts


@task(name="load_district_references")
def load_references() -> list[DistrictReference]:
    logger = get_run_logger()

    references = load_district_references()

    logger.info(f"Loaded {len(references)} district references")
    return references


@task(name="persist_districts")
def persist(references: list[DistrictReference]) -> int:
    logger = get_run_logger()

    persisted = asyncio.run(persist_districts(settings.database_url, references))

    logger.info(f"Persisted {persisted} districts")
    return persisted


@task(name="assign_city_districts")
def assign(references: list[DistrictReference]) -> int:
    logger = get_run_logger()

    updated = asyncio.run(assign_city_districts(settings.database_url, references))

    logger.info(f"Assigned a district to {updated} cities")
    return updated


@flow(
    name="extract_districts",
    description="Populate the district table and assign each city to its district by INE code prefix.",
)
def extract_districts() -> int:
    references = load_references()
    persist(references)

    return assign(references)


if __name__ == "__main__":
    extract_districts()
