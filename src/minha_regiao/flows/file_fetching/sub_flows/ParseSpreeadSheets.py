import re
import requests
from typing import List
from bs4 import BeautifulSoup
from prefect import flow, task
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot
from minha_regiao.flows.file_fetching.schema.ElectionFile import ElectionFile
from minha_regiao.exceptions.FileFetchingException import FileFetchingException
from minha_regiao.flows.file_fetching.schema.ElectionHistory import ElectionHistory


@task(name="Fetch Presidential Elections First Round Files")
def fetch_presidential_elections_first_round_files(base_url: str, election_history: ElectionHistory) -> List[ElectionFile]:
    results = []

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    anchor_tags_after_1996 = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    anchor_tags_prior_1996 = soup.find_all('a', string=lambda text: text and "Ficheiro de Resultados" in text)

    valid_anchor_tags = [a for a in anchor_tags_after_1996 if "2ºSufragio" not in a['href'] and "_2." not in a['href']]
    valid_anchor_tags += [a for a in anchor_tags_prior_1996 if "2ºSufragio" not in a['href'] and "_2." not in a['href']]

    if len(valid_anchor_tags) != len(election_history.election_date):
        raise ValueError(f"Number of valid anchor tags ({len(valid_anchor_tags)}) does not match number of election dates ({len(election_history.election_date)}) for presidential elections first round.")
        
    for date, election in zip(election_history.election_date, election_history.elections):
        anchor_tag = next((a for a in valid_anchor_tags if f"PR_{date.year}" in a['href']), None)

        if anchor_tag is None:
            raise ValueError(f"No valid anchor tag found for presidential election first round in year {date.year}.")
        
        file_url = "https://www.sg.mai.gov.pt" + anchor_tag['href']
        
        results.append(ElectionFile(
            election=election,
            file_url=file_url
        ))

    return results

@task(name="Fetch Presidential Elections Second Round Files")
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
    
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Fetch all elements with "Folha de Cálculo" in the text of the href
    anchor_tags = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    #TODO: Deal with the votes in the diaspora that are in a different file and have a different naming convention only in the 2005 elections.
    anchor_tag_2005 = soup.find_all('a', href=lambda href: href and re.search(r'/AR2005_Nacional\.xls', href))
    anchor_tags_prior_2002 = soup.find_all('a', href=lambda href: href and re.search(r'/AR\d{4}.xls', href))
    
    valid_anchor_tags = anchor_tags + anchor_tag_2005 + anchor_tags_prior_2002  

    # Remove duplicates hrefs from valid_anchor_tags
    seen_hrefs = set()
    unique_valid_anchor_tags = []
    
    for a in valid_anchor_tags:
        if a['href'] not in seen_hrefs:
            unique_valid_anchor_tags.append(a)
            seen_hrefs.add(a['href'])
    
    # Remove any anchor tag with "VOT_RESID_ESTRANG" in the href as well, as they contain the votes from the diaspora
    unique_valid_anchor_tags = [a for a in unique_valid_anchor_tags if "VOT_RESID_ESTRANG" not in a['href']]

    if len(unique_valid_anchor_tags) != len(election_history.election_date):
        raise ValueError(f"Number of valid anchor tags ({len(unique_valid_anchor_tags)}) does not match number of election dates ({len(election_history.election_date)}) for legislative elections.")

    for date, election in zip(election_history.election_date, election_history.elections):
        # Find anchor tag with year in the href
        anchor_tag = next((a for a in unique_valid_anchor_tags if f"{date.year}" in a['href']), None)
        
        if anchor_tag is None:
            raise ValueError(f"No valid anchor tag found for legislative election in year {date.year}.")

        file_url = "https://www.sg.mai.gov.pt" + anchor_tag['href']
        
        results.append(ElectionFile(
            election=election,
            file_url=file_url
        ))

    return results

@task(name="Fetch Local Elections Files")
def fetch_municipal_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []
    
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Fetch all anchor tags that contain "Assembleia de Freguesia" or "Assembleia Municipal" or "Câmara Municipal" in the text
    # Use get_text() to handle nested tags like <strong> and normalize whitespace to handle &nbsp; and zero-width spaces
    all_anchors = soup.find_all('a')
    anchor_tags = [
        a for a in all_anchors 
        if a.get_text(strip=True) and any(
            keyword in a.get_text(strip=True).replace('\xa0', ' ').replace('&nbsp;', ' ').replace('\u200b', '').replace('\u200c', '').replace('\u200d', '')
            for keyword in ["Assembleia de Freguesia", "Assembleia Municipal", "Câmara Municipal"]
        )
    ]

    if len(anchor_tags) != 3 * len(election_history.election_date):
        raise ValueError(f"Number of valid anchor tags ({len(anchor_tags)}) does not match number of election dates ({3 * len(election_history.election_date)}) for legislative elections.")

    for date, election in zip(election_history.election_date, election_history.elections):
        anchor_tag_cm = next((a for a in anchor_tags if f"CM" in a['href'] and f"{date.year}" in a['href']), None)
        anchor_tag_am = next((a for a in anchor_tags if f"AM" in a['href'] and f"{date.year}" in a['href']), None)
        anchor_tag_af = next((a for a in anchor_tags if f"AF" in a['href'] and f"{date.year}" in a['href']), None)
        
        if anchor_tag_cm is None:
            raise ValueError(f"No valid anchor tag found for Câmara Municipal in year {date.year}.")

        if anchor_tag_am is None:
            raise ValueError(f"No valid anchor tag found for Assembleia Municipal in year {date.year}.")
        
        if anchor_tag_af is None:
            raise ValueError(f"No valid anchor tag found for Assembleia de Freguesia in year {date.year}.")
        
        results.append(ElectionFile(
            election=election,
            file_url="https://www.sg.mai.gov.pt" + anchor_tag_cm['href']
        ))
        
        results.append(ElectionFile(
            election=election,
            file_url="https://www.sg.mai.gov.pt" + anchor_tag_am['href']
        ))
        
        results.append(ElectionFile(
            election=election,
            file_url="https://www.sg.mai.gov.pt" + anchor_tag_af['href']
        ))

    return results

@task(name="Fetch European Parliament Elections Files")
def fetch_european_parliament_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    results = []
    
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Fetch the anchor tags that contain "Folha de Cálculo" in the text of the href
    anchor_tags = soup.find_all('a', string=lambda text: text and "Folha de Cálculo" in text)
    
    anchor_tags += soup.find_all('a', string=lambda text: text and "Ficheiro de Resultados do Escrutínio Provisório" in text.replace('\xa0', ' ').replace('&nbsp;', ' ').replace('\u200b', '').replace('\u200c', '').replace('\u200d', ''))

    anchor_tags += soup.find_all('a', href=lambda href: href and re.search(r'/PE\d{2}\.zip', href))
    
    # Remove duplicates hrefs from anchor_tags
    seen_hrefs = set()
    unique_anchor_tags = []
    for a in anchor_tags:
         if a['href'] not in seen_hrefs:
             unique_anchor_tags.append(a)
             seen_hrefs.add(a['href'])

    # Remove any anchor tag with "Estrangeiro" in the href as well, as they contain the votes from the diaspora
    unique_anchor_tags = [a for a in unique_anchor_tags if "Estrangeiro" not in a['href'] and "VOT_RESID_ESTRANG" not in a['href']]

    if len(unique_anchor_tags) != len(election_history.election_date):
        raise ValueError(f"Number of valid anchor tags ({len(unique_anchor_tags)}) does not match number of election dates ({len(election_history.election_date)}) for European Parliament elections.")

    for date, election in zip(election_history.election_date, election_history.elections):

        anchor_tag = next((a for a in soup.find_all('a', href=True) if f"{date.year}" in a['href']), None)

        if not anchor_tag:
            # Search for PEYY.zip in the href
            anchor_tag = next((a for a in soup.find_all('a', href=True) if re.search(rf'/PE{str(date.year)[-2:]}\.zip', a['href'])), None) 
        
        if anchor_tag is None:
            raise ValueError(f"No valid anchor tag found for European Parliament election in year {date.year}.")
        
        results.append(ElectionFile(
            election=election,
            file_url="https://www.sg.mai.gov.pt" + anchor_tag['href']
        ))

    return results

@task(name="Fetch Regional Elections Files")
def fetch_regional_elections_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    raise NotImplementedError("Regional elections files fetching not implemented yet.")

@task(name="Fetch Referendums Files")
def fetch_referendums_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    raise NotImplementedError("Referendums files fetching not implemented yet.")

@task(name="Fetch Constitutional Assembly Files")
def fetch_constitutional_assembly_files(
    base_url: str, election_history: ElectionHistory
)-> List[ElectionFile]:
    raise NotImplementedError("Constitutional Assembly elections files fetching not implemented yet.")   

@flow(name="Fetch Spreadsheet Files")
def fetch_spreadsheet_files(seg_mai_root: SegMaiRoot, elections_histories: List[ElectionHistory]) -> List[ElectionFile]:
    results = {
        'PR': [],
        'AL': [],
        'AR': [],
        'PE': [],
        #'ALRAM': [],
        #'ALRAA': [],
        #'REF': [],
        #'AC': []
    }
    
    for election_history in elections_histories:
        if election_history.election_type == 'PR':
            results['PR'].extend(
                fetch_presidential_elections_files(seg_mai_root.presidential_elections_url, election_history)
            )
        elif election_history.election_type == 'AL':
            results['AL'].extend(
                fetch_municipal_elections_files(seg_mai_root.local_elections_url, election_history)
            )
        elif election_history.election_type == 'AR':
            results['AR'].extend(
                fetch_legislative_elections_files(seg_mai_root.legislative_elections_url, election_history)
            )
        elif election_history.election_type == 'PE':
            results['PE'].extend(
                fetch_european_parliament_elections_files(seg_mai_root.european_parliament_elections_url, election_history)
            )
        elif election_history.election_type in ['ALRAM', 'ALRAA']:
            # results['ALRAM'].extend(
            #     fetch_regional_elections_files(seg_mai_root.regional_elections_url, election_history)
            # )
            pass
        elif election_history.election_type in ['REF']:
            # results['REF'].extend(
            #     fetch_referendums_files(seg_mai_root.referendums_url, election_history)
            # )
            pass
        elif election_history.election_type in ['AC']:
            # results['AC'].extend(
            #     fetch_constitutional_assembly_files(seg_mai_root.historical_elections_url, election_history)
            # )
            pass
        else:
            raise ValueError(f"Unknown election type: {election_history.election_type}")
        
    # Check that all the results are not empty, if they are empty raise an exception
    for key, value in results.items():
        if not value:
            raise FileFetchingException(f"No files found for {key} and election history {election_history.election_name} ({election_history.election_type})")
        
        # Introducing check for presidential elections that have two turns would require a more complex logic to match the files with the correct election history, so for now we will skip this check for presidential elections        
        if key == "PR":
            continue 

        election_entry = next((election for election in elections_histories if election.election_type == key), None)
        
        if election_entry is None:
            raise ValueError(f"No election entry found for election type {key}")
        
        if key == "AL":
            # For local elections we have 3 files per election (CM, AM and AF), so we need to check that the number of files is 3 times the number of elections in the election history
            if len(value) != 3 * len(election_entry.elections):
                raise FileFetchingException(f"Number of files found for {key} does not match number of elections in the election history. Found {len(value)} files but expected {3 * len(election_entry.elections)} for election history {election_entry.election_name} ({election_entry.election_type})")
        else:
            if len(value) != len(election_entry.elections):
                raise FileFetchingException(f"Number of files found for {key} does not match number of elections in the election history. Found {len(value)} files but expected {len(election_entry.elections)} for election history {election_entry.election_name} ({election_entry.election_type})")
        
    # If all checks pass, flatten the results and return
    flattened_results = []
    
    for key, value in results.items():
        flattened_results.extend(value)

    return flattened_results