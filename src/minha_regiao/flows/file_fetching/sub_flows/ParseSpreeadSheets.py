import requests
from typing import List
from bs4 import BeautifulSoup
from prefect import flow, task
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.flows.file_fetching.schema.ElectionFile import ElectionFile
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.schema.ElectionHistory import ElectionHistory


"""
#https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/PresidenciaRepublica/Paginas/default.aspx?FirstOpen=1
<span style="font-size:13.3333px">Resultados do escrutínio provisório - <a href="/AdministracaoEleitoral/EleicoesReferendos/PresidenciaRepublica/Documents/2ºSufragio-PR2026/PR_2026_Globais_2ºSufrágio.xlsx" target="_blank" title="resultados do Escrutínio Provisório">Folha de Cálculo​</a></span>
"""

"""
    # TODO: Divide into first and second round, as the files are different
    #results = []

    #response = requests.get(base_url)
    #soup = BeautifulSoup(response.text, 'html.parser')

    # # TODO: The breaking point is 1996
    # anchor_tags_after_1996 = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    # anchor_tags_prior_1996 = soup.find_all('a', string=lambda text: text and "Ficheiro de Resultados" in text)

    # for date, election in zip(election_history.election_date, election_history.elections):
    #     # Detect if it is first or second round based on the election name
    #     if "1º Sufrágio" in election.election_name:
    #         pass
    #     elif "2º Sufrágio" in election.election_name:
    #         pass
        
    #     # Extract the year from the date
    #     year = date.year

    #     file_url = None
        
    #     pass

    # return results
"""
def fetch_presidential_elections_first_round_files(base_url: str, election_history: ElectionHistory) -> List[ElectionFile]:
    results = []

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    anchor_tags_after_1996 = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    anchor_tags_prior_1996 = soup.find_all('a', string=lambda text: text and "Ficheiro de Resultados" in text)
    
    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    raise NotImplementedError("Fetching presidential elections first round files is not implemented yet.")


def fetch_presidential_elections_second_round_files(base_url: str, election_history: ElectionHistory) -> List[ElectionFile]:
    results = []

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    anchor_tags_after_1996 = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    anchor_tags_prior_1996 = soup.find_all('a', string=lambda text: text and "Ficheiro de Resultados" in text)

    # Discard all the anchors whose href does not contain "2ºSufragio" or "2ª volta"
    valid_anchor_tags = [a for a in anchor_tags_after_1996 if "2ºSufragio" in a['href'] or "_2." in a['href']]
    valid_anchor_tags += [a for a in anchor_tags_prior_1996 if "2ºSufragio" in a['href'] or "_2." in a['href']]
    
    if len(valid_anchor_tags) != len(election_history.election_date):
        raise ValueError(f"Number of valid anchor tags ({len(valid_anchor_tags)}) does not match number of election dates ({len(election_history.election_date)}) for presidential elections second round.")

    for date, election in zip(election_history.election_date, election_history.elections):
        # Find the anchor tag that contains the PR_YYYY in the href
        anchor_tag = next((a for a in valid_anchor_tags if f"PR_{date.year}" in a['href']), None)

        if anchor_tag is None:
            raise ValueError(f"No valid anchor tag found for presidential election second round in year {date.year}.")
        
        file_url = "https://www.sg.mai.gov.pt" + anchor_tag['href']
        
        results.append(ElectionFile(
            election=election,
            file_url=file_url
        ))

    return results


@task(name="Fetch Presidential Elections Files")
def fetch_presidential_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    # Either all election_history.election_name is "PRESIDENTE DA REPÚBLICA - 2º Sufrágio" or "PRESIDENTE DA REPÚBLICA - 1º Sufrágio", if there is a mix raise an exception
    if all(election.election_name == "PRESIDENTE DA REPÚBLICA - 1º Sufrágio" for election in election_history.elections):
        return fetch_presidential_elections_first_round_files(base_url, election_history)
    elif all(election.election_name == "PRESIDENTE DA REPÚBLICA - 2º Sufrágio" for election in election_history.elections):
        return fetch_presidential_elections_second_round_files(base_url, election_history)

    raise ValueError("Mixed election names in election history. Expected all election names to be either 'PRESIDENTE DA REPÚBLICA - 2º Sufrágio' or 'PRESIDENTE DA REPÚBLICA - 1º Sufrágio'.")
    
@task(name="Fetch Legislative Elections Files")
def fetch_legislative_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []

    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results

@task(name="Fetch Local Elections Files")
def fetch_municipal_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []

    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results

@task(name="Fetch European Parliament Elections Files")
def fetch_european_parliament_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []

    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results

@task(name="Fetch Regional Elections Files")
def fetch_regional_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []

    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results

@task(name="Fetch Referendums Files")
def fetch_referendums_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []
    
    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results

@task(name="Fetch Constitutional Assembly Files")
def fetch_constitutional_assembly_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []

    for date, election in zip(election_history.election_date, election_history.elections):
        pass

    return results    

@flow(name="Fetch Spreadsheet Files")
def fetch_spreadsheet_files(seg_mai_root: SegMaiRoot, elections_histories: List[ElectionHistory]) -> List[ElectionFile]:
    results = {
        'PR': [],
        'AL': [],
        'AR': [],
        'PE': [],
        'ALRAM': [],
        'ALRAA': [],
        'REF': [],
        'AC': []
    }
    
    for election_history in elections_histories:
        if election_history.election_type == 'PR':
            results['PR'].extend(
                fetch_presidential_elections_files(seg_mai_root.presidential_elections_url, election_history)
            )
        elif election_history.election_type == 'AL':
            results['AL'].extend(
                fetch_legislative_elections_files(seg_mai_root.legislative_elections_url, election_history)
            )
        elif election_history.election_type == 'AR':
            results['AR'].extend(
                fetch_municipal_elections_files(seg_mai_root.local_elections_url, election_history)
            )
        elif election_history.election_type == 'PE':
            results['PE'].extend(
                fetch_european_parliament_elections_files(seg_mai_root.european_parliament_elections_url, election_history)
            )
        elif election_history.election_type in ['ALRAM', 'ALRAA']:
            results['ALRAM'].extend(
                fetch_regional_elections_files(seg_mai_root.regional_elections_url, election_history)
            )
        elif election_history.election_type in ['REF']:
            results['REF'].extend(
                fetch_referendums_files(seg_mai_root.referendums_url, election_history)
            )
        elif election_history.election_type in ['AC']:
            results['AC'].extend(
                fetch_constitutional_assembly_files(seg_mai_root.historical_elections_url, election_history)
            )
        else:
            raise ValueError(f"Unknown election type: {election_history.election_type}")
        
        # Check that all the results are not empty, if they are empty raise an exception
        for key, value in results.items():
            if not value:
                raise FileFetchingException(f"No files found for {key} and election history {election_history.election_name} ({election_history.election_type})")

        #TODO: Ensure the number of files is equal to the number of dates in the election history.
    return results.values()