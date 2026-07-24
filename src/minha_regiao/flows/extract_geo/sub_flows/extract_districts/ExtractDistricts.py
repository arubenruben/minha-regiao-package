import asyncio

from prefect import flow, task, get_run_logger

from minha_regiao.flows.extract_geo.Settings import settings as geo_settings
from minha_regiao.flows.extract_geo.schema.DistrictReference import DistrictReference
from minha_regiao.flows.extract_geo.services.DatasetPublisher import DatasetPublisher
from minha_regiao.flows.extract_geo.services.DatasetRepo import ensure_dataset_repo
from minha_regiao.flows.extract_geo.services.DistrictReferenceLoader import load_district_references
from minha_regiao.flows.extract_geo.sub_flows.extract_districts.Settings import settings
from minha_regiao.flows.extract_geo.sub_flows.extract_districts.schema.DistrictDatasetRecord import (
    DistrictDatasetRecord,
)
from minha_regiao.flows.extract_geo.sub_flows.extract_districts.services.DistrictRepository import (
    assign_city_districts,
    fetch_district_wikipedia_urls,
    persist_districts,
)


@task(name="load_district_references")
def load_references() -> list[DistrictReference]:
    logger = get_run_logger()

    references = load_district_references()

    logger.info(f"Loaded {len(references)} district references")
    return references


@task(name="persist_districts")
def persist(references: list[DistrictReference]) -> int:
    logger = get_run_logger()

    persisted = asyncio.run(persist_districts(geo_settings.database_url, references))

    logger.info(f"Persisted {persisted} districts")
    return persisted


@task(name="assign_city_districts")
def assign(references: list[DistrictReference]) -> int:
    logger = get_run_logger()

    updated = asyncio.run(assign_city_districts(geo_settings.database_url, references))

    logger.info(f"Assigned a district to {updated} cities")
    return updated


@task(name="fetch_district_wikipedia_urls")
def fetch_wikipedia_urls() -> dict[str, str | None]:
    return asyncio.run(fetch_district_wikipedia_urls(geo_settings.database_url))


@task(name="build_district_dataset_records")
def build_district_dataset_records(
    references: list[DistrictReference], wikipedia_urls_by_name: dict[str, str | None]
) -> list[DistrictDatasetRecord]:
    logger = get_run_logger()

    records = [
        DistrictDatasetRecord(
            name=reference.name,
            ine_prefix=reference.ine_prefix,
            wikipedia_url=wikipedia_urls_by_name.get(reference.name),
        )
        for reference in references
    ]

    logger.info(f"Built {len(records)} district dataset records")
    return records


@task(name="publish_district_dataset")
def publish_district_dataset(repo_id: str, records: list[DistrictDatasetRecord]) -> None:
    DatasetPublisher[DistrictDatasetRecord](
        repo_id, geo_settings.hf_api_key, settings.district_dataset_config_name
    ).publish(records)


@flow(
    name="extract_districts",
    description="Populate the district table, assign each city to its district by INE code prefix, "
    "and publish the district dataset.",
)
def extract_districts(geo_dataset_repo_id: str = geo_settings.geo_dataset_repo_id) -> int:
    references = load_references()
    persist(references)
    updated = assign(references)

    wikipedia_urls_by_name = fetch_wikipedia_urls()
    records = build_district_dataset_records(references, wikipedia_urls_by_name)
    ensure_dataset_repo(geo_settings.hf_api_key, geo_dataset_repo_id)
    publish_district_dataset(geo_dataset_repo_id, records)

    return updated


if __name__ == "__main__":
    extract_districts()
