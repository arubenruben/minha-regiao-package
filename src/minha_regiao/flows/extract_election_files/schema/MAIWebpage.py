from minha_regiao.flows.extract_election_files.schema.Schema import Schema

class MAIWebpage(Schema):
    president_url: str
    parliament_url: str
    town_hall_url: str
    regional_assembly_url: str
    referendum_url: str
    full_historic_url: str