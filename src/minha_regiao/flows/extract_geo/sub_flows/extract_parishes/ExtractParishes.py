import asyncio
from pathlib import Path

from prefect import flow, task, get_run_logger

from minha_regiao.entity.City import City
from minha_regiao.entity.Parish import ParishEra
from minha_regiao.flows.extract_geo.Settings import settings as geo_settings
from minha_regiao.flows.extract_geo.services.DatasetDownloader import download_dataset_file
from minha_regiao.flows.extract_geo.services.DatasetPublisher import DatasetPublisher
from minha_regiao.flows.extract_geo.services.DatasetRepo import ensure_dataset_repo
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.Settings import settings
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.schema.ParishCityLink import ParishCityLink
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.schema.ParishDatasetRecord import ParishDatasetRecord
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.services.CityRepository import (
    index_cities_by_ine_code,
    index_cities_by_name,
    is_mainland_ine_code,
    load_cities,
    match_city_by_parish_ine_code,
    match_city_by_parish_name,
)
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.services.IneCodeLookup import (
    POST_2013_FREGUESIA_CONFIG,
    POST_2021_FREGUESIA_CONFIG,
    PRE_2013_FREGUESIA_CONFIG,
    FreguesiaSheetConfig,
    extract_ambiguous_ine_codes_by_freguesia,
    extract_ine_codes_by_freguesia,
)
from minha_regiao.flows.extract_geo.sub_flows.extract_parishes.services.ParishRepository import persist_parishes


@task(name="download_election_results")
def download_election_results(repo_id: str, filename: str) -> Path:
    logger = get_run_logger()
    logger.info(f"Downloading {filename} from {repo_id}")

    path = download_dataset_file(repo_id, filename, geo_settings.hf_api_key)

    logger.info(f"Downloaded election results to {path}")
    return path


@task(name="build_ine_code_lookup")
def build_ine_code_lookup(
    election_results_path: Path, config: FreguesiaSheetConfig
) -> tuple[dict[str, str], dict[str, list[str]]]:
    logger = get_run_logger()

    ine_codes_by_freguesia = extract_ine_codes_by_freguesia(election_results_path, config)
    ambiguous_ine_codes_by_freguesia = extract_ambiguous_ine_codes_by_freguesia(election_results_path, config)

    logger.info(
        f"Built INE code lookup for {len(ine_codes_by_freguesia)} freguesias "
        f"({len(ambiguous_ine_codes_by_freguesia)} ambiguous names)"
    )
    return ine_codes_by_freguesia, ambiguous_ine_codes_by_freguesia


@task(name="load_cities")
def load_cities_task(db_url: str) -> list[City]:
    logger = get_run_logger()

    cities = asyncio.run(load_cities(db_url))

    logger.info(f"Loaded {len(cities)} cities")
    return cities


@task(name="index_cities_by_ine_code")
def index_cities_by_ine_code_task(cities: list[City]) -> dict[str, City]:
    return index_cities_by_ine_code(cities)


@task(name="index_cities_by_name")
def index_cities_by_name_task(cities: list[City]) -> dict[str, City]:
    return index_cities_by_name(cities)


@task(name="link_parishes_to_cities")
def link_parishes_to_cities(
    ine_codes_by_freguesia: dict[str, str],
    cities_by_ine_code: dict[str, City],
    cities_by_name: dict[str, City],
) -> list[ParishCityLink]:
    logger = get_run_logger()

    links = []
    fuzzy_matched = []
    unmatched = []
    for name, ine_code in ine_codes_by_freguesia.items():
        city = match_city_by_parish_ine_code(ine_code, cities_by_ine_code)

        # A prefix match against a non-mainland code is never trustworthy
        # (see is_mainland_ine_code), so fall back to matching the parish
        # name directly against city names instead.
        if city is None and not is_mainland_ine_code(ine_code):
            city = match_city_by_parish_name(name, cities_by_name)
            if city is not None:
                fuzzy_matched.append((name, city.name))

        if city is None:
            unmatched.append(name)
            continue

        links.append(ParishCityLink(name=name, ine_code=ine_code, city=city))

    if fuzzy_matched:
        logger.info(f"Matched by name instead of INE code: {fuzzy_matched}")
    if unmatched:
        logger.warning(f"No city found for: {sorted(unmatched)}")

    logger.info(f"Linked {len(links)}/{len(ine_codes_by_freguesia)} freguesias to cities")
    return links


@task(name="persist_parishes")
def persist_parishes_task(db_url: str, links: list[ParishCityLink], era: ParishEra) -> int:
    logger = get_run_logger()

    persisted = asyncio.run(persist_parishes(db_url, links, era))

    logger.info(f"Persisted {persisted} {era.value} parishes")
    return persisted


@task(name="build_parish_dataset_records")
def build_parish_dataset_records(links: list[ParishCityLink], era: ParishEra) -> list[ParishDatasetRecord]:
    logger = get_run_logger()

    records = [
        ParishDatasetRecord(
            parish_name=link.name,
            ine_code=link.ine_code,
            era=era.value,
            city_name=link.city.name,
            city_ine_code=link.city.ine_code,
        )
        for link in links
    ]

    logger.info(f"Built {len(records)} {era.value} parish dataset records")
    return records


@task(name="publish_parish_dataset")
def publish_parish_dataset(repo_id: str, records: list[ParishDatasetRecord]) -> None:
    DatasetPublisher[ParishDatasetRecord](
        repo_id, geo_settings.hf_api_key, settings.parish_dataset_config_name
    ).publish(records)


@flow(name="Enrich Freguesias PT", description="Enrich Freguesias PT")
def enrich_freguesias_pt(
    election_results_dataset_repo_id: str = settings.election_results_dataset_repo_id,
    pre_2013_election_results_filename: str = settings.pre_2013_election_results_filename,
    post_2013_election_results_filename: str = settings.post_2013_election_results_filename,
    post_2021_election_results_filename: str = settings.post_2021_election_results_filename,
    geo_dataset_repo_id: str = geo_settings.geo_dataset_repo_id,
):
    pre_2013_election_results_path = download_election_results.with_options(
        name="download_pre_2013_election_results"
    )(election_results_dataset_repo_id, pre_2013_election_results_filename)

    post_2013_election_results_path = download_election_results.with_options(
        name="download_post_2013_election_results"
    )(election_results_dataset_repo_id, post_2013_election_results_filename)

    post_2021_election_results_path = download_election_results.with_options(
        name="download_post_2021_election_results"
    )(election_results_dataset_repo_id, post_2021_election_results_filename)

    pre_2013_ine_codes, pre_2013_ambiguous_ine_codes = build_ine_code_lookup.with_options(
        name="build_pre_2013_ine_code_lookup"
    )(pre_2013_election_results_path, PRE_2013_FREGUESIA_CONFIG)

    post_2013_ine_codes, post_2013_ambiguous_ine_codes = build_ine_code_lookup.with_options(
        name="build_post_2013_ine_code_lookup"
    )(post_2013_election_results_path, POST_2013_FREGUESIA_CONFIG)

    post_2021_ine_codes, post_2021_ambiguous_ine_codes = build_ine_code_lookup.with_options(
        name="build_post_2021_ine_code_lookup"
    )(post_2021_election_results_path, POST_2021_FREGUESIA_CONFIG)

    cities = load_cities_task(geo_settings.database_url)
    cities_by_ine_code = index_cities_by_ine_code_task(cities)
    cities_by_name = index_cities_by_name_task(cities)

    pre_2013_links = link_parishes_to_cities.with_options(name="link_pre_2013_parishes_to_cities")(
        pre_2013_ine_codes, cities_by_ine_code, cities_by_name
    )
    post_2013_links = link_parishes_to_cities.with_options(name="link_post_2013_parishes_to_cities")(
        post_2013_ine_codes, cities_by_ine_code, cities_by_name
    )
    post_2021_links = link_parishes_to_cities.with_options(name="link_post_2021_parishes_to_cities")(
        post_2021_ine_codes, cities_by_ine_code, cities_by_name
    )

    persist_parishes_task.with_options(name="persist_pre_2013_parishes")(
        geo_settings.database_url, pre_2013_links, ParishEra.PRE_2013
    )
    persist_parishes_task.with_options(name="persist_post_2013_parishes")(
        geo_settings.database_url, post_2013_links, ParishEra.POST_2013
    )
    persist_parishes_task.with_options(name="persist_post_2021_parishes")(
        geo_settings.database_url, post_2021_links, ParishEra.POST_2021
    )

    pre_2013_records = build_parish_dataset_records.with_options(name="build_pre_2013_parish_dataset_records")(
        pre_2013_links, ParishEra.PRE_2013
    )
    post_2013_records = build_parish_dataset_records.with_options(name="build_post_2013_parish_dataset_records")(
        post_2013_links, ParishEra.POST_2013
    )
    post_2021_records = build_parish_dataset_records.with_options(name="build_post_2021_parish_dataset_records")(
        post_2021_links, ParishEra.POST_2021
    )

    ensure_dataset_repo(geo_settings.hf_api_key, geo_dataset_repo_id)
    publish_parish_dataset(geo_dataset_repo_id, pre_2013_records + post_2013_records + post_2021_records)


if __name__ == "__main__":
    enrich_freguesias_pt()
