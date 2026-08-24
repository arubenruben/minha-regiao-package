from extract_election_files.schema.Schema import Schema

class MAIWebpage(Schema):
    president_url: str
    parliament_url: str
    town_hall_url: str
    regional_assembly_url: str
    european_url: str