from prefect import flow, task, get_run_logger
from minha_regiao.flows.construction.subflows.FetchTownHallWebsites import get_cities



@flow(name="Fetch PDMs")
def fetch_pdms():
    cities = get_cities()