import httpx
from typing import List
from google import genai
from bs4 import BeautifulSoup
from instructor import from_genai
from prefect import flow, task, cache_policies, get_run_logger
from minha_regiao.flows.file_fetching.Settings import Settings
from minha_regiao.flows.file_fetching.schema.Election import Election
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.schema.ElectionHistory import ElectionHistory
from minha_regiao.flows.file_fetching.prompts.TableHistoryPrompt import TableHistoryPrompt


settings = Settings()

@task(name="Get Historical Elections Table")
def get_historical_elections_table(url: str):
    logger = get_run_logger()
    logger.info(f"Fetching historical elections table from {url}")
    
    with httpx.Client(timeout=60.0) as client:
        response = client.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.find('table', id='MSO_ContentTable')

        if not table:
            logger.error("Historical elections table not found on the page")
            raise FileFetchingException("Historical elections table not found on the page.")
        
        logger.info("Successfully retrieved historical elections table")
        return table
def extract_election_data_from_table(table) -> List[ElectionHistory]:
    logger = get_run_logger()
    logger.info("Extracting election data from table using Gemini")
    
    client = from_genai(
        genai.Client(
            api_key=settings.gemini_api_key,
        )
    )

    # Strip tags without content to avoid confusion in parsing
    logger.debug("Cleaning table HTML")
    for tag in table.find_all(lambda tag: not tag.text.strip()):
        tag.decompose()
    
    # Strip proprities and styles from the table to simplify parsing
    for tag in table.find_all(True):
        tag.attrs = {}
    
    logger.info("Sending table to Gemini for parsing")
    elections = client.create(
        model=settings.gemini_model,
        messages=TableHistoryPrompt.prompt(normalize_instructor=True, raw_html=str(table)),
        response_model=List[Election],
    )

    logger.info(f"Successfully extracted {len(elections)} elections from table")
    return ElectionHistory.from_elections(elections)


@flow(name="Parse Election Historical")
def parse_election_historical(seg_mai_root: SegMaiRoot) -> List[ElectionHistory]:
    logger = get_run_logger()
    logger.info("Starting Parse Election Historical flow")
    
    table = get_historical_elections_table(seg_mai_root.historical_elections_url)
    
    election_histories = extract_election_data_from_table(table)
    logger.info(f"Parse Election Historical flow completed with {len(election_histories)} election histories")
    
    return election_histories
