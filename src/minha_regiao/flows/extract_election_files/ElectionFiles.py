from urllib.parse import urljoin

from scrapling.fetchers import StealthyFetcher
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from minha_regiao.flows.extract_election_files.Settings import settings
from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.schema.MAIWebpage import MAIWebpage
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.EuropeanElections import (
    european_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.parliament_elections.ParliamentElections import (
    parliament_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.presidential_elections.PresidentialElections import (
    presidential_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.referendums.Referendums import referendums
from minha_regiao.flows.extract_election_files.sub_flows.regional_elections.RegionalElections import (
    regional_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.town_hall_elections_files.TownHallElections import (
    town_hall_elections,
)

# Maps a keyword found in an anchor's href path segments to the MAIWebpage
# field it fills. The page lists more categories (e.g. Autárquicas
# Intercalares, Conselho das Comunidades Portuguesas) than MAIWebpage
# models, so matching is done by keyword rather than link order.
URL_FIELD_MAP = {
    "PresidenciaRepublica": "president_url",
    "AssembleiaRepublica": "parliament_url",
    "AutarquiasLocais": "town_hall_url",
    "Regionais": "regional_assembly_url",
    "Referendos": "referendum_url",
    "ParlamentoEuropeu": "european_url",
}


@task(name="fetch_mai_page")
def fetch_mai_page(base_url: str):
    logger = get_run_logger()
    logger.info(f"Fetching MAI page from {base_url}")

    page = StealthyFetcher.fetch(base_url, headless=True, network_idle=True)

    logger.info("Fetched MAI page")
    return page


@task(name="extract_subsite_links", cache_policy=NO_CACHE)
def extract_subsite_links(page) -> list[str]:
    logger = get_run_logger()

    links = page.css("div.subsites ul li a")
    hrefs = [link.attrib["href"] for link in links if link.attrib.get("href")]

    logger.info(f"Extracted {len(hrefs)} subsite links")
    return hrefs


@task(name="map_links_to_fields")
def map_links_to_fields(hrefs: list[str], base_url: str) -> dict[str, str]:
    logger = get_run_logger()

    fields: dict[str, str] = {}
    for href in hrefs:
        segments = href.split("?", 1)[0].strip("/").split("/")
        for keyword, field_name in URL_FIELD_MAP.items():
            if keyword in segments and field_name not in fields:
                fields[field_name] = urljoin(base_url, href)
                break

    logger.info(f"Mapped {len(fields)}/{len(URL_FIELD_MAP)} expected fields: {sorted(fields)}")
    return fields


@task(name="build_mai_webpage")
def build_mai_webpage(fields: dict[str, str]) -> MAIWebpage:
    logger = get_run_logger()

    missing = set(URL_FIELD_MAP.values()) - fields.keys()
    if missing:
        logger.error(f"Missing expected links in MAI webpage: {sorted(missing)}")
        raise ValueError(f"Missing expected links in MAI webpage: {sorted(missing)}")

    return MAIWebpage(**fields)


@task(name="reduce_election_files")
def reduce_election_files(results: list[list[Election]]) -> list[Election]:
    logger = get_run_logger()

    elections = [election for result in results for election in result]

    logger.info(f"Reduced {len(results)} sub-flow results into {len(elections)} election files")
    return elections


@flow(name="fetch_election_files", description="Fetch election files from the specified base URL.")
def fetch_election_files() -> list[Election]:
    logger = get_run_logger()

    logger.info(f"Fetching election files from {settings.seg_mai_base_url}")

    page = fetch_mai_page(settings.seg_mai_base_url)
    hrefs = extract_subsite_links(page)
    fields = map_links_to_fields(hrefs, settings.seg_mai_base_url)
    mai_webpage = build_mai_webpage(fields)

    results = [
        european_elections(mai_webpage.european_url),
        parliament_elections(mai_webpage.parliament_url),
        presidential_elections(mai_webpage.president_url),
        referendums(mai_webpage.referendum_url),
        regional_elections(mai_webpage.regional_assembly_url),
        town_hall_elections(mai_webpage.town_hall_url),
    ]

    return reduce_election_files(results)


if __name__ == "__main__":
    fetch_election_files()
