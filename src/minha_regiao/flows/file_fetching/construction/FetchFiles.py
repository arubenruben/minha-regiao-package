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

    logger.info("=" * 80)
    logger.info("Starting Fetch Files flow")
    logger.info("=" * 80)

    logger.info("Fetching town hall URLs...")
    town_halls = fetch_town_halls()
    logger.info(f"✓ Found {len(town_halls)} town halls")
    for th in town_halls[:5]:  # Log first 5 as examples
        logger.debug(f"  - {th.url}")
    if len(town_halls) > 5:
        logger.debug(f"  ... and {len(town_halls) - 5} more")

    # Set concurrency level: 1 for debug mode, 12 for production
    max_concurrency = 1 if __debug__ else 12
    logger.info(
        f"\nConcurrency level: {max_concurrency} ({'debug mode' if __debug__ else 'production mode'})"
    )

    logger.info("\nFetching PDM candidates...")
    pdm_result = fetch_pdm(town_halls, max_concurrency)
    logger.info(f"✓ Processed {pdm_result.town_halls_processed} town halls")
    logger.info(f"✓ Found {pdm_result.total_candidates_found} PDM candidates")

    logger.info("\nTop PDM candidates found:")
    for candidate in pdm_result.candidates[:5]:
        logger.info(f"  - {candidate.url}")
    if len(pdm_result.candidates) > 5:
        logger.info(f"  ... and {len(pdm_result.candidates) - 5} more")

    logger.info("=" * 80)
    logger.info("Fetch Files flow completed successfully")
    logger.info("=" * 80)

    return pdm_result


if __name__ == "__main__":
    result = fetch_files()
    print(f"Processed {result.town_halls_processed} town halls")
    print(f"Found {result.total_candidates_found} candidates")

    for candidate in result.candidates:
        print(f"  - {candidate.url} (from {candidate.source_town_hall})")
