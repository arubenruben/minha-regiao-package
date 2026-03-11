from minha_regiao.flows.file_fetching.schema.Schema import Schema
from minha_regiao.flows.file_fetching.schema.Election import Election

class ElectionFile(Schema):
    election: Election
    file_url: str