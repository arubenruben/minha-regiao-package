import requests
import instructor
from bs4 import BeautifulSoup
from prefect import flow, task
from minha_regiao.flows.file_fetching.Settings import Settings
from minha_regiao.flows.file_fetching.schema.Elections import Elections
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.exceptions.FileFetchingException import FileFetchingException

settings = Settings()

@task(name="Get Historical Elections Table")
def get_historical_elections_table(url: str):
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', id='MSO_ContentTable')

    if not table:
        raise FileFetchingException("Historical elections table not found on the page.")
    
    return table

@task(name="Extract Election Data from Table")
def extract_election_data_from_table(table) -> Elections:
    client = instructor.from_provider(
        model=settings.ollama_model
    )
    raise NotImplementedError("This function is not yet implemented. It should parse the HTML table and extract the election data into an Elections object.")


@flow(name="Parse Election Historical")
def parse_election_historical(seg_mai_root: SegMaiRoot):
    table = get_historical_elections_table(seg_mai_root.historical_elections_url)
    
    elections = extract_election_data_from_table(table)

if __name__ == "__main__":
    pass