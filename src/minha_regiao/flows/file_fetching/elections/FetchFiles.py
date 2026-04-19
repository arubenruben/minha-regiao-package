import re
import requests
import requests
from typing import List
from datasets import Dataset
from bs4 import BeautifulSoup
from prefect import flow, task, get_run_logger
from tempfile import NamedTemporaryFile
from huggingface_hub import login, file_exists, upload_file
from minha_regiao.flows.file_fetching.elections.Settings import Settings
from minha_regiao.flows.file_fetching.elections.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.flows.file_fetching.elections.schema.ElectionFile import ElectionFile
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.elections.sub_flows.ParseSpreeadSheets import fetch_spreadsheet_files
from minha_regiao.flows.file_fetching.elections.sub_flows.ParseElectionHistorical import parse_election_historical

settings = Settings()

@task(name="Login to Hugging Face")
def login_to_hf():
    logger = get_run_logger()
    logger.info("Logging into Hugging Face...")
    login(token=settings.hf_api_key)
    logger.info("Successfully logged into Hugging Face")

@task(name="Get Root File Links")
def get_root_file_links(url: str = settings.sg_mai_link):
    logger = get_run_logger()
    logger.info(f"Fetching root file links from {url}")
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    
    next_pages = {}

    for a_tag in soup.find_all('a', href=True):
        if re.search(r'EleicoesReferendos.*default\.aspx\?FirstOpen=1$', a_tag['href']):
            next_pages[a_tag.text.strip()] = "https://www.sg.mai.gov.pt" + a_tag['href']

    if not next_pages:
        raise FileFetchingException("No valid links found on the page.")
    
    if len(next_pages) != 9:
        raise FileFetchingException("Unexpected number of links found on the page.")
    
    if "Presidência da República" not in next_pages:
        raise FileFetchingException("Link for 'Presidência da República' not found.")
    
    if "Assembleia da República" not in next_pages:
        raise FileFetchingException("Link for 'Assembleia da República' not found.")
    
    if "Autarquias Locais" not in next_pages:
        raise FileFetchingException("Link for 'Autarquias Locais' not found.")
    
    if "Parlamento Europeu" not in next_pages:
        raise FileFetchingException("Link for 'Parlamento Europeu' not found.")

    if "Regionais" not in next_pages:
        raise FileFetchingException("Link for 'Regionais' not found.")
    
    if "Referendos" not in next_pages:
        raise FileFetchingException("Link for 'Referendos' not found.")
    
    if "Histórico das Eleições" not in next_pages:
        raise FileFetchingException("Link for 'Histórico das Eleições' not found.")
    
    if "Conselho das Comunidades Portuguesas" not in next_pages:
        raise FileFetchingException("Link for 'Conselho das Comunidades Portuguesas' not found.")
    
    if "Autárquicas Intercalares" not in next_pages:
        raise FileFetchingException("Link for 'Autárquicas Intercalares' not found.")

    logger.info(f"Successfully extracted {len(next_pages)} root file links")
    return SegMaiRoot(
        presidential_elections_url=next_pages['Presidência da República'],
        legislative_elections_url=next_pages['Assembleia da República'],
        local_elections_url=next_pages['Autarquias Locais'],
        regional_elections_url=next_pages['Regionais'],
        european_parliament_elections_url=next_pages['Parlamento Europeu'],
        referendums_url=next_pages['Referendos'],
        historical_elections_url=next_pages['Histórico das Eleições'],
        portuguese_council_elections_url=next_pages['Conselho das Comunidades Portuguesas'],
        mid_term_municipal_elections_url=next_pages['Autárquicas Intercalares']
    )

@task(name="Save Files to Hugging Face Repo")
def save_files_to_hf_repo(election_files: List[ElectionFile]):
    logger = get_run_logger()
    logger.info(f"Starting upload of {len(election_files)} election files to Hugging Face repo")
    
    for election_file in election_files:
        # Download the files to a temporary location, push to Hugging Face repo using the API, and then delete the temporary files.
        logger.debug(f"Downloading file from {election_file.file_url}")
        downloaded_file = requests.get(election_file.file_url)

        with NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(downloaded_file.content)
            tmp_file.flush()

            path = f"elections/{election_file.election.election_name}/{election_file.file_url.replace('https://www.sg.mai.gov.pt/AdministracaoEleitoral/', '')}"

            if not file_exists(filename=path, repo_id=settings.hf_repo_name):
                logger.info(f"Uploading {path} to Hugging Face repo")
                upload_file(
                    path_or_fileobj=tmp_file.name,
                    path_in_repo=path,
                    repo_id=settings.hf_repo_name,
                    repo_type="dataset",                    
                    commit_message=f"Add {election_file.election.election_type} election file from {election_file.election.election_date}"
                )
                logger.info(f"Successfully uploaded {path}")
            else:
                logger.info(f"File for {election_file.election.election_type} election from {election_file.election.election_date} already exists in the repo. Skipping upload.")

            # Update the file URL to point to the raw file in the Hugging Face repo
            election_file.hf_file_url = f"https://huggingface.co/datasets/{settings.hf_repo_name}/resolve/main/{path}"

    logger.info("Creating dataset from election files metadata")
    dataset = Dataset.from_list([{
        "election_type": election_file.election.election_type,
        "election_name": election_file.election.election_name,
        "election_date": election_file.election.election_date,
        "file_url": election_file.hf_file_url,
        "hf_file_url": election_file.hf_file_url
    } for election_file in election_files])
    
    logger.info(f"Pushing dataset to Hugging Face hub: {settings.hf_repo_name}")
    dataset.push_to_hub(settings.hf_repo_name, private=False, config_name="election_files_metadata")
    logger.info("Dataset successfully pushed to Hugging Face hub")

@flow(name="Fetch Files")
def fetch_files():
    logger = get_run_logger()
    logger.info("Starting Fetch Files flow")
    
    login_to_hf()

    seg_mai_root = get_root_file_links()

    logger.info("Parsing election historical data")
    elections_history = parse_election_historical(seg_mai_root)
    logger.info(f"Found {len(elections_history)} election histories")

    #TODO: Save to database the elections. Define the ORM / sqlmodel / migration for that 
    
    logger.info("Fetching spreadsheet files")
    election_files = fetch_spreadsheet_files(seg_mai_root, elections_history)
    logger.info(f"Found {len(election_files)} election files")

    save_files_to_hf_repo(election_files)
    
    logger.info("Fetch Files flow completed successfully")

if __name__ == "__main__":
    fetch_files()