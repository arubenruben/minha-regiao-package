import asyncio
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download
from scrapling.parser import Selector
from prefect import flow, task, get_run_logger
from scrapling.fetchers import StealthyFetcher

from minha_regiao.flows.extract_cities.Settings import settings
from minha_regiao.flows.extract_cities.schema.CityContacts import CityContacts
from minha_regiao.flows.extract_cities.schema.CityDatasetRecord import CityDatasetRecord
from minha_regiao.flows.extract_cities.schema.MunicipalContact import MunicipalContact
from minha_regiao.flows.extract_cities.services.CityDatasetPublisher import CityDatasetPublisher
from minha_regiao.flows.extract_cities.services.CityRepository import persist_cities
from minha_regiao.flows.extract_cities.services.FuzzyMatch import resolve_name
from minha_regiao.flows.extract_cities.services.IneCodeLookup import (
    extract_ambiguous_ine_codes_by_municipality,
    extract_ine_codes_by_municipality,
    match_ine_code,
)
from minha_regiao.flows.extract_cities.services.MunicipalContactParser import parse_municipal_contact_row
from minha_regiao.flows.extract_districts.services.DistrictReferenceLoader import (
    load_district_references,
    match_district_name,
)

@task(name="download_election_results")
def download_election_results(repo_id: str, filename: str) -> Path:
    logger = get_run_logger()
    logger.info(f"Downloading {filename} from {repo_id}")

    path = hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        repo_type="dataset",
        token=settings.hf_api_key or None,
    )

    logger.info(f"Downloaded election results to {path}")
    return Path(path)


@task(name="build_ine_code_lookup")
def build_ine_code_lookup(election_results_path: Path) -> tuple[dict[str, str], dict[str, list[str]]]:
    logger = get_run_logger()

    ine_codes_by_municipality = extract_ine_codes_by_municipality(election_results_path)
    ambiguous_ine_codes_by_municipality = extract_ambiguous_ine_codes_by_municipality(election_results_path)

    logger.info(
        f"Built INE code lookup for {len(ine_codes_by_municipality)} municipalities "
        f"({len(ambiguous_ine_codes_by_municipality)} ambiguous names)"
    )
    return ine_codes_by_municipality, ambiguous_ine_codes_by_municipality


@task(name="fetch_contacts_page")
def fetch_contacts_page(url: str) -> Selector:
    logger = get_run_logger()
    logger.info(f"Fetching contacts page from {url}")

    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)

    logger.info("Fetched contacts page")
    return page


@task(name="extract_contact_rows")
def extract_contact_rows(page: Selector) -> list[Selector]:
    logger = get_run_logger()

    table = page.css("div.content table").first
    rows = table.css("tr")[1:]  # Skip the header row

    logger.info(f"Extracted {len(rows)} contact rows")
    return rows


@task(name="parse_municipal_contacts")
def parse_municipal_contacts(rows: list[Selector]) -> list[MunicipalContact]:
    logger = get_run_logger()

    contacts = [parse_municipal_contact_row(row) for row in rows]

    logger.info(f"Parsed {len(contacts)} municipal contacts")
    return contacts


@task(name="index_city_contacts")
def index_city_contacts(
    town_halls: list[MunicipalContact], municipal_assemblies: list[MunicipalContact]
) -> list[CityContacts]:
    logger = get_run_logger()

    assemblies_by_municipality = {assembly.municipality: assembly for assembly in municipal_assemblies}

    contacts = []

    for town_hall in town_halls:
        assembly_name = resolve_name(town_hall.municipality, assemblies_by_municipality)
        assembly = assemblies_by_municipality.pop(assembly_name, None) if assembly_name else None

        if assembly is None:
            logger.warning(f"No municipal assembly contact found for '{town_hall.municipality}'")
            continue

        if assembly_name != town_hall.municipality:
            logger.info(f"Fuzzy matched town hall '{town_hall.municipality}' to municipal assembly '{assembly_name}'")

        contacts.append(
            CityContacts(municipality=town_hall.municipality, town_hall=town_hall, municipal_assembly=assembly)
        )

    if assemblies_by_municipality:
        logger.warning(f"Unmatched municipal assembly contacts: {sorted(assemblies_by_municipality)}")

    logger.info(f"Indexed {len(contacts)} city contacts")
    return contacts


@task(name="attach_ine_codes")
def attach_ine_codes(
    contacts: list[CityContacts],
    ine_codes_by_municipality: dict[str, str],
    ambiguous_ine_codes_by_municipality: dict[str, list[str]],
) -> list[CityContacts]:
    logger = get_run_logger()

    updated = []
    unmatched = []
    for contact in contacts:
        ine_code = match_ine_code(contact.municipality, ine_codes_by_municipality, ambiguous_ine_codes_by_municipality)
        if ine_code is None:
            unmatched.append(contact.municipality)
        updated.append(contact.model_copy(update={"ine_code": ine_code}))

    if unmatched:
        logger.warning(f"No INE code found for: {sorted(unmatched)}")

    logger.info(f"Attached INE codes to {len(updated) - len(unmatched)}/{len(updated)} city contacts")

    return updated


@task(name="persist_city_contacts")
def persist_city_contacts(contacts: list[CityContacts]) -> list[CityContacts]:
    logger = get_run_logger()

    matched = [contact for contact in contacts if contact.ine_code is not None]
    skipped = len(contacts) - len(matched)
    if skipped:
        logger.warning(f"Skipping {skipped} city contacts with no INE code (cannot upsert without a key)")

    persisted = asyncio.run(persist_cities(settings.database_url, matched))

    logger.info(f"Persisted {persisted} cities")
    return contacts


@task(name="build_city_dataset_records")
def build_city_dataset_records(contacts: list[CityContacts]) -> list[CityDatasetRecord]:
    logger = get_run_logger()

    references = load_district_references()

    unmatched = []
    records = []
    for contact in contacts:
        if contact.ine_code is None:
            continue

        district_name = match_district_name(contact.ine_code, references)
        if district_name is None:
            unmatched.append(contact.municipality)

        records.append(
            CityDatasetRecord(
                city_name=contact.municipality,
                ine_code=contact.ine_code,
                district_name=district_name,
                town_hall_email=contact.town_hall.email,
                town_hall_website=contact.town_hall.website,
            )
        )

    if unmatched:
        logger.warning(f"No district resolved for: {sorted(unmatched)}")

    logger.info(f"Built {len(records)} city dataset records")
    return records


@task(name="ensure_city_dataset_repo")
def ensure_city_dataset_repo(repo_id: str) -> None:
    logger = get_run_logger()
    logger.info(f"Ensuring Hugging Face dataset repo {repo_id} exists")

    api = HfApi(token=settings.hf_api_key)
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True, private=False)


@task(name="publish_city_dataset")
def publish_city_dataset(repo_id: str, records: list[CityDatasetRecord]) -> None:
    CityDatasetPublisher(repo_id, settings.hf_api_key).publish(records)


@flow(
    name="extract_cities",
    description="Extract and index town hall and municipal assembly contacts from the ANMP website by city.",
)
def extract_cities(
    hf_dataset_repo_id: str, presidential_election_results_filename: str, city_dataset_repo_id: str
) -> list[CityContacts]:
    presidential_election_results_path = download_election_results(
        hf_dataset_repo_id, presidential_election_results_filename
    )

    town_hall_page = fetch_contacts_page(settings.anmp_town_hall_url)
    municipal_assembly_page = fetch_contacts_page(settings.anmp_municipal_assembly_url)

    town_hall_rows = extract_contact_rows(town_hall_page)
    municipal_assembly_rows = extract_contact_rows(municipal_assembly_page)

    town_halls = parse_municipal_contacts(town_hall_rows)
    municipal_assemblies = parse_municipal_contacts(municipal_assembly_rows)

    contacts = index_city_contacts(town_halls, municipal_assemblies)

    ine_codes_by_municipality, ambiguous_ine_codes_by_municipality = build_ine_code_lookup(
        presidential_election_results_path
    )
    contacts = attach_ine_codes(contacts, ine_codes_by_municipality, ambiguous_ine_codes_by_municipality)
    contacts = persist_city_contacts(contacts)

    records = build_city_dataset_records(contacts)
    ensure_city_dataset_repo(city_dataset_repo_id)
    publish_city_dataset(city_dataset_repo_id, records)

    return contacts


if __name__ == "__main__":
    extract_cities(
        hf_dataset_repo_id="minharegiao/portuguese-elections",
        presidential_election_results_filename="raw/presidential/PR_2026_Globais.xlsx",
        city_dataset_repo_id="minharegiao/portuguese-cities",
    )
