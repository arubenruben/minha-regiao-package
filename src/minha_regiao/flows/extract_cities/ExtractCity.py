from prefect import flow, task, get_run_logger
from scrapling.fetchers import StealthyFetcher
from scrapling.parser import Selector

from minha_regiao.flows.extract_cities.Settings import settings
from minha_regiao.flows.extract_cities.schema.CityContacts import CityContacts
from minha_regiao.flows.extract_cities.schema.MunicipalContact import MunicipalContact
from minha_regiao.flows.extract_cities.services.MunicipalContactParser import parse_municipal_contact_row


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
        assembly = assemblies_by_municipality.pop(town_hall.municipality, None)
        
        if assembly is None:
            logger.warning(f"No municipal assembly contact found for '{town_hall.municipality}'")
            continue

        contacts.append(
            CityContacts(municipality=town_hall.municipality, town_hall=town_hall, municipal_assembly=assembly)
        )

    if assemblies_by_municipality:
        logger.warning(f"Unmatched municipal assembly contacts: {sorted(assemblies_by_municipality)}")

    logger.info(f"Indexed {len(contacts)} city contacts")
    return contacts


@flow(
    name="extract_cities",
    description="Extract and index town hall and municipal assembly contacts from the ANMP website by city.",
)
def extract_cities() -> list[CityContacts]:
    town_hall_page = fetch_contacts_page(settings.anmp_town_hall_url)
    municipal_assembly_page = fetch_contacts_page(settings.anmp_municipal_assembly_url)

    town_hall_rows = extract_contact_rows(town_hall_page)
    municipal_assembly_rows = extract_contact_rows(municipal_assembly_page)

    town_halls = parse_municipal_contacts(town_hall_rows)
    municipal_assemblies = parse_municipal_contacts(municipal_assembly_rows)

    return index_city_contacts(town_halls, municipal_assemblies)


if __name__ == "__main__":
    extract_cities()
