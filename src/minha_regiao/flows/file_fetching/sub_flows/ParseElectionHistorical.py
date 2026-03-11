import requests
from prefect import flow
from bs4 import BeautifulSoup
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.exceptions.FileFetchingException import FileFetchingException

def get_historical_elections_table(url: str):
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', id='MSO_ContentTable')

    if not table:
        raise FileFetchingException("Historical elections table not found on the page.")
    
    return table


@flow(name="Parse Election Historical")
def parse_election_historical(seg_mai_root: SegMaiRoot):
    table = get_historical_elections_table(seg_mai_root.historical_elections_url)

if __name__ == "__main__":
    pass