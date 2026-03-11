import re
import requests
import requests
from typing import List
from datasets import Dataset
from bs4 import BeautifulSoup
from prefect import flow, task
from tempfile import NamedTemporaryFile
from huggingface_hub import login, file_exists, upload_file
from minha_regiao.flows.file_fetching.Settings import Settings
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.flows.file_fetching.schema.ElectionFile import ElectionFile
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.sub_flows.ParseSpreeadSheets import fetch_spreadsheet_files
from minha_regiao.flows.file_fetching.sub_flows.ParseElectionHistorical import parse_election_historical

settings = Settings()

@task(name="Login to Hugging Face")
def login_to_hf():
    login(token=settings.hf_api_key)

@task(name="Get Root File Links")
def get_root_file_links(url: str = settings.sg_mai_link):
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
    #TODO: Push raw files as git



    for election_file in election_files:
        # Download the files to a temporary location, push to Hugging Face repo using the API, and then delete the temporary files.
        downloaded_file = requests.get(election_file.file_url)

        with NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(downloaded_file.content)
            tmp_file.flush()

            path = f"elections/{election_file.election.election_name}/{election_file.file_url.replace('https://www.sg.mai.gov.pt/AdministracaoEleitoral/', '')}"

            if not file_exists(filename=path, repo_id=settings.hf_repo_name):
                upload_file(
                    path_or_fileobj=tmp_file.name,
                    path_in_repo=path,
                    repo_id=settings.hf_repo_name,
                    repo_type="dataset",                    
                    commit_message=f"Add {election_file.election.election_type} election file from {election_file.election.election_date}"
                )
            else:
                print(f"File for {election_file.election.election_type} election from {election_file.election.election_date} already exists in the repo. Skipping upload.")

            # Update the file URL to point to the raw file in the Hugging Face repo
            election_file.hf_file_url = f"https://huggingface.co/datasets/{settings.hf_repo_name}/resolve/main/{path}"

    dataset = Dataset.from_list([{
        "election_type": election_file.election.election_type,
        "election_name": election_file.election.election_name,
        "election_date": election_file.election.election_date,
        "file_url": election_file.hf_file_url,
        "hf_file_url": election_file.hf_file_url
    } for election_file in election_files])
    
    dataset.push_to_hub(settings.hf_repo_name, private=False, config_name="election_files_metadata")

@flow(name="Fetch Files")
def fetch_files():
    login_to_hf()

    seg_mai_root = get_root_file_links()

    elections_history = parse_election_historical(seg_mai_root)

    #TODO: Save to database the elections. Define the ORM / sqlmodel / migration for that 
    
    election_files = fetch_spreadsheet_files(seg_mai_root, elections_history)

    save_files_to_hf_repo(election_files)

if __name__ == "__main__":
    fetch_files()