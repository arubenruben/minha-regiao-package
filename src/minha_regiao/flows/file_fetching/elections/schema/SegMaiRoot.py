from minha_regiao.flows.file_fetching.schema.Schema import Schema

class SegMaiRoot(Schema):
    presidential_elections_url: str
    legislative_elections_url: str
    local_elections_url: str
    regional_elections_url: str
    european_parliament_elections_url: str
    referendums_url: str
    historical_elections_url: str
    portuguese_council_elections_url: str
    mid_term_municipal_elections_url: str
