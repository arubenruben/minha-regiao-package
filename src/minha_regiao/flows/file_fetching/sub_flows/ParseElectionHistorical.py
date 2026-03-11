import requests
from typing import List
from google import genai
from bs4 import BeautifulSoup
from instructor import from_genai
from prefect import flow, task, cache_policies
from minha_regiao.flows.file_fetching.Settings import Settings
from minha_regiao.flows.file_fetching.schema.Election import Election
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.schema.ElectionHistory import ElectionHistory
from minha_regiao.flows.file_fetching.prompts.TableHistoryPrompt import TableHistoryPrompt


settings = Settings()

@task(name="Get Historical Elections Table")
def get_historical_elections_table(url: str):
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', id='MSO_ContentTable')

    if not table:
        raise FileFetchingException("Historical elections table not found on the page.")
    
    return table

@task(name="Extract Election Data from Table", cache_policy=cache_policies.INPUTS)
def extract_election_data_from_table(table) -> List[ElectionHistory]:
    client = from_genai(
        genai.Client(
            api_key=settings.gemini_api_key,
        )
    )

    # Strip tags without content to avoid confusion in parsing
    for tag in table.find_all(lambda tag: not tag.text.strip()):
        tag.decompose()
    
    # Strip proprities and styles from the table to simplify parsing
    for tag in table.find_all(True):
        tag.attrs = {}
    
    elections = client.create(
        model=settings.gemini_model,
        messages=TableHistoryPrompt.prompt(normalize_instructor=True, raw_html=str(table)),
        response_model=List[Election],
    )

    return ElectionHistory.from_elections(elections)


@flow(name="Parse Election Historical")
def parse_election_historical(seg_mai_root: SegMaiRoot) -> List[ElectionHistory]:
    table = get_historical_elections_table(seg_mai_root.historical_elections_url)
    
    return extract_election_data_from_table(table)
