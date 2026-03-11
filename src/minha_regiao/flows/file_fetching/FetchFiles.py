import re
import requests
from typing import List
from bs4 import BeautifulSoup
from prefect import flow, task
from minha_regiao.flows.file_fetching.Settings import Settings
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.flows.file_fetching.schema.ElectionFile import ElectionFile
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.sub_flows.ParseSpreeadSheets import fetch_spreadsheet_files
from minha_regiao.flows.file_fetching.sub_flows.ParseElectionHistorical import parse_election_historical

settings = Settings()

@task(name="Login to Hugging Face")
def login_to_hf():
    pass

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

    #TODO: Create huggingface datasets with URL pointing to the raw files in the repo
    
    pass

@flow(name="Fetch Files")
def fetch_files():
    seg_mai_root = get_root_file_links()

    elections_history = parse_election_historical(seg_mai_root)

    #TODO: Save to database the elections. Define the ORM / sqlmodel / migration for that 
    
    election_files = fetch_spreadsheet_files(seg_mai_root, elections_history)

    save_files_to_hf_repo(election_files)

if __name__ == "__main__":
    fetch_files()