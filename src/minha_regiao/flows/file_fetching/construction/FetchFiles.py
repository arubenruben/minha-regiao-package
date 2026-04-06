from prefect import flow, get_run_logger
from minha_regiao.flows.file_fetching.construction.sub_flows.FetchPDM import fetch_pdm
from minha_regiao.flows.file_fetching.construction.sub_flows.FetchTownHalls import (
    fetch_town_halls,
)
from minha_regiao.flows.file_fetching.construction.schema.PDMFetchResultDTO import (
    PDMFetchResultDTO,
)


@flow(name="Fetch Files")
def fetch_files() -> PDMFetchResultDTO:
    logger = get_run_logger()

    logger.info("Fetching town hall URLs...")
    town_halls = fetch_town_halls()
    logger.info(f"Found {len(town_halls)} town halls")

    logger.info("Fetching PDM candidates...")
    pdm_result = fetch_pdm(town_halls)
    logger.info(f"Found {pdm_result.total_candidates_found} PDM candidates")

    return pdm_result


if __name__ == "__main__":
    result = fetch_files()
    print(f"Processed {result.town_halls_processed} town halls")
    print(f"Found {result.total_candidates_found} candidates")

    for candidate in result.candidates:
        print(f"  - {candidate.url} (from {candidate.source_town_hall})")
