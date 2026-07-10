from scrapling.fetchers import StealthyFetcher
from prefect import flow, task, get_run_logger
from minha_regiao.flows.extract_election_files.Settings import settings


@flow(name="fetch_election_files", description="Fetch election files from the specified base URL.")
def fetch_election_files(
    
):
    logger = get_run_logger()
    
    logger.info(f"Fetching election files from {settings.seg_mai_base_url}")
    
    page = StealthyFetcher.fetch(settings.seg_mai_base_url, headless=True, network_idle=True)

    pass



if __name__ == "__main__":
    election_files = fetch_election_files()
    