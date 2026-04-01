from prefect import flow, task, get_run_logger
from minha_regiao.flows.file_fetching.construction.sub_flows.FetchPDM import fetch_pdm
from minha_regiao.flows.file_fetching.construction.sub_flows.FetchTownHalls import (
    fetch_town_halls,
)


@flow(name="Fetch Files")
def fetch_files():
    town_hall_urls = fetch_town_halls()

    pdm_candidate_urls = fetch_pdm(town_hall_urls)


if __name__ == "__main__":
    fetch_files()
