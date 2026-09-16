from pydantic import BaseModel

from extract_pdms.schema.RegulationDocument import RegulationDocument


class PDMRecord(BaseModel):
    """A municipality's PDM (Plano Diretor Municipal), identified in SNIT,
    together with every regulation document published against it.
    """

    municipio: str
    title: str
    identifier: str
    documents: list[RegulationDocument]
