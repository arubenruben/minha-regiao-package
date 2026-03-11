from prefect import flow, task
from minha_regiao.flows.file_fetching.schema.Election import Election
from minha_regiao.flows.file_fetching.schema.SegMaiRoot import SegMaiRoot


@task(name="Fetch Presidential Elections Files")
def fetch_presidential_elections_files(
    base_url: str, election: Election
):
    pass

@task(name="Fetch Legislative Elections Files")
def fetch_legislative_elections_files(
    base_url: str, election: Election
):
    pass

@task(name="Fetch Local Elections Files")
def fetch_municipal_elections_files(
    base_url: str, election: Election
):
    pass


@task(name="Fetch European Parliament Elections Files")
def fetch_european_parliament_elections_files(
    base_url: str, election: Election
):
    pass

@task(name="Fetch Regional Elections Files")
def fetch_regional_elections_files(
    base_url: str, election: Election
):
    pass

@flow(name="Fetch Spreadsheet Files")
def fetch_spreadsheet_files(seg_mai_root: SegMaiRoot, election: Election):
    pass