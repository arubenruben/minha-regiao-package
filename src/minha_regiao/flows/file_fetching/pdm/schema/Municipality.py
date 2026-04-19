from minha_regiao.flows.file_fetching.pdm.schema.Schema import Schema

class Municipality(Schema):
    name: str
    address: str
    website: str
    email: str