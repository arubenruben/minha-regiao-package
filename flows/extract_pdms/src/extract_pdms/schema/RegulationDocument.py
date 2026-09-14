from pydantic import BaseModel, HttpUrl


class RegulationDocument(BaseModel):
    """One dre.pt-published regulation PDF (revision, correction, amendment,
    ...) attached to a PDM, as returned by SNIT's regulamento lookup and
    enriched with metadata parsed from its filename (see
    `extract_pdms.snit_search.build_regulamento_entry`).
    """

    url: HttpUrl
    doc_type: str
    number: str
    year: int
    suffix: int | None = None
    data_publicacao: str | None = None
    dinamica: str | None = None
    publicacao: str | None = None

    # Populated by the PDF-download/text-extraction step.
    text: str | None = None
    needs_ocr: bool = False
