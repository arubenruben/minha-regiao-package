from enum import Enum

from pydantic import BaseModel, HttpUrl


class DocumentStatus(str, Enum):
    """Outcome of a regulation PDF's download/text-extraction step."""

    # Not yet attempted -- e.g. freshly resolved from SNIT metadata, or
    # reused unchanged from a previous run's not-yet-processed record.
    PENDING = "pending"
    OK = "ok"
    # Downloaded and parsed fine but yielded no text at all: an old,
    # scanned regulation that needs OCR (not yet supported).
    NEEDS_OCR = "needs_ocr"
    DOWNLOAD_FAILED = "download_failed"
    EXTRACTION_FAILED = "extraction_failed"


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

    # Populated by the PDF-download/text-extraction step. `text` is null
    # whenever `status` isn't OK -- the file couldn't be read, so there's
    # nothing to report for it.
    status: DocumentStatus = DocumentStatus.PENDING
    text: str | None = None
