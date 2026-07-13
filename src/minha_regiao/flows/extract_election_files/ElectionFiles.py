import asyncio
from urllib.parse import urljoin

from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError
from scrapling.fetchers import StealthyFetcher
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from minha_regiao.flows.extract_election_files.Settings import settings
from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.schema.MAIWebpage import MAIWebpage
from minha_regiao.flows.extract_election_files.schema.StructuredElection import StructuredElection
from minha_regiao.flows.extract_election_files.services.ElectionDatasetPublisher import ElectionDatasetPublisher
from minha_regiao.flows.extract_election_files.services.ElectionMetadataExtractor import ElectionMetadataExtractor
from minha_regiao.flows.extract_election_files.services.ElectionRawFileUploader import ElectionRawFileUploader
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.EuropeanElections import (
    european_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.parliament_elections.ParliamentElections import (
    parliament_elections,
)
from minha_regiao.flows.extract_election_files.sub_flows.presidential_elections.PresidentialElections import (
    presidential_elections,
)
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
    "ParlamentoEuropeu": "european_url",
}

HF_DATASET_CONFIG_NAME = "raw_election_files"


@task(name="ensure_huggingface_login")
def ensure_huggingface_login(api: HfApi) -> None:
    logger = get_run_logger()

    try:
        user = api.whoami()
    except HfHubHTTPError as error:
        logger.error("Hugging Face authentication failed")
        raise RuntimeError("Hugging Face authentication failed: check HF_API_KEY") from error

    logger.info(f"Authenticated with Hugging Face as {user['name']}")


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


@task(name="structure_election_files")
def structure_election_files(elections: list[Election]) -> list[StructuredElection]:
    logger = get_run_logger()

    extractor = ElectionMetadataExtractor(settings.google_api_key, settings.gemini_model)
    metadata = asyncio.run(extractor.extract(elections))

    structured = [
        StructuredElection.from_election(election, election_metadata)
        for election, election_metadata in zip(elections, metadata)
    ]

    logger.info(f"Structured {len(structured)} election records")
    return structured


@task(name="ensure_hf_dataset_repo")
def ensure_hf_dataset_repo(api: HfApi, repo_id: str) -> None:
    logger = get_run_logger()
    logger.info(f"Ensuring Hugging Face dataset repo {repo_id} exists")

    api.create_repo(repo_id, repo_type="dataset", exist_ok=True, private=False)


@task(name="upload_raw_election_files")
def upload_raw_election_files(
    api: HfApi, repo_id: str, elections: list[StructuredElection]
) -> list[StructuredElection]:
    return ElectionRawFileUploader(api, repo_id).upload(elections)


@task(name="publish_election_dataset")
def publish_election_dataset(repo_id: str, token: str, config_name: str, elections: list[StructuredElection]) -> None:
    ElectionDatasetPublisher(repo_id, token, config_name).publish(elections)


@flow(name="fetch_election_files", description="Fetch election files from the specified base URL.")
def fetch_election_files() -> list[StructuredElection]:
    logger = get_run_logger()

    api = HfApi(token=settings.hf_api_key)
    ensure_huggingface_login(api)

    logger.info(f"Fetching election files from {settings.seg_mai_base_url}")

    page = fetch_mai_page(settings.seg_mai_base_url)
    hrefs = extract_subsite_links(page)
    fields = map_links_to_fields(hrefs, settings.seg_mai_base_url)
    mai_webpage = build_mai_webpage(fields)

    results = [
        european_elections(mai_webpage.european_url),
        parliament_elections(mai_webpage.parliament_url),
        presidential_elections(mai_webpage.president_url),
        regional_elections(mai_webpage.regional_assembly_url),
        town_hall_elections(mai_webpage.town_hall_url),
    ]

    elections = reduce_election_files(results)
    structured = structure_election_files(elections)

    ensure_hf_dataset_repo(api, settings.hf_dataset_repo_id)
    structured_with_raw_files = upload_raw_election_files(api, settings.hf_dataset_repo_id, structured)
    publish_election_dataset(
        settings.hf_dataset_repo_id, settings.hf_api_key, HF_DATASET_CONFIG_NAME, structured_with_raw_files
    )

    return structured_with_raw_files


if __name__ == "__main__":
    fetch_election_files()
