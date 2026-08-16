from pydantic import BaseModel


class RegulationDocument(BaseModel):
    name: str
    dre_url: str


class RMUERegulation(BaseModel):
    municipality: str
    urbanization_documents: list[RegulationDocument] = []
    fee_documents: list[RegulationDocument] = []
