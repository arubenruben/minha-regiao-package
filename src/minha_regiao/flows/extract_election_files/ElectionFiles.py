from urllib.parse import urljoin

from scrapling.fetchers import StealthyFetcher
from prefect import flow, task, get_run_logger
from minha_regiao.flows.extract_election_files.Settings import settings
from minha_regiao.flows.extract_election_files.schema.MAIWebpage import MAIWebpage
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.EuropeanElections import (
    european_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.historical_elections.HistoricalElections import (
    historical_elections,
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
    "HistoricoEleicoes": "full_historic_url",
    "ParlamentoEuropeu": "european_url",
}


@task(name="fetch_mai_page")
def fetch_mai_page(base_url: str):
    return StealthyFetcher.fetch(base_url, headless=True, network_idle=True)


@task(name="extract_subsite_links")
def extract_subsite_links(page) -> list[str]:
    links = page.css("div.subsites ul li a")
    return [link.attrib["href"] for link in links if link.attrib.get("href")]


@task(name="map_links_to_fields")
def map_links_to_fields(hrefs: list[str], base_url: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for href in hrefs:
        segments = href.split("?", 1)[0].strip("/").split("/")
        for keyword, field_name in URL_FIELD_MAP.items():
            if keyword in segments and field_name not in fields:
                fields[field_name] = urljoin(base_url, href)
                break
    return fields


@task(name="build_mai_webpage")
def build_mai_webpage(fields: dict[str, str]) -> MAIWebpage:
    missing = set(URL_FIELD_MAP.values()) - fields.keys()
    if missing:
        raise ValueError(f"Missing expected links in MAI webpage: {sorted(missing)}")

    return MAIWebpage(**fields)


@flow(name="fetch_election_files", description="Fetch election files from the specified base URL.")
def fetch_election_files() -> list[str]:
    logger = get_run_logger()

    logger.info(f"Fetching election files from {settings.seg_mai_base_url}")

    page = fetch_mai_page(settings.seg_mai_base_url)
    hrefs = extract_subsite_links(page)
    fields = map_links_to_fields(hrefs, settings.seg_mai_base_url)
    mai_webpage = build_mai_webpage(fields)

    results = [
        european_elections(mai_webpage.european_url),
        historical_elections(mai_webpage.full_historic_url),
        parliament_elections(mai_webpage.parliament_url),
        presidential_elections(mai_webpage.president_url),
        referendums(mai_webpage.referendum_url),
        regional_elections(mai_webpage.regional_assembly_url),
        town_hall_elections(mai_webpage.town_hall_url),
    ]

    logger.info(f"Collected {len(results)} sub-flow results")

    return results


if __name__ == "__main__":
    fetch_election_files()
