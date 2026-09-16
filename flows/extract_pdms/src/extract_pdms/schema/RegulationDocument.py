from enum import Enum

from pydantic import BaseModel, HttpUrl

from extract_pdms.schema.RegulationStructure import StructureNode


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
    # Extraction succeeded, but this document's own notice (identified by
    # doc_type/number/year) couldn't be located inside the downloaded
    # Diário da República page range -- see
    # minha_regiao.gazette.GazetteSegmenter.find_notice_text.
    NOTICE_NOT_FOUND = "notice_not_found"


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

    # Populated by the PDF-download/text-extraction step. `text` and
    # `structure` are null whenever `status` isn't OK -- the file couldn't
    # be read, or (NOTICE_NOT_FOUND) this document's own notice couldn't be
    # isolated from the page range, so there's nothing reliable to report.
    # When `status` is OK, `text` is *not* the whole downloaded page's raw
    # text -- it's already narrowed, by
    # minha_regiao.gazette.GazetteSegmenter.find_notice_text, to just this
    # document's own notice within that (possibly multi-municipality) page
    # range. `structure` is that same narrowed text broken down into its
    # nested Parte/Título/Capítulo/Secção/Subsecção/Artigo hierarchy (see
    # extract_pdms.services.StructureParser and
    # extract_pdms.schema.RegulationStructure); it's additive, `text`
    # remains the authoritative full text of this notice.
    status: DocumentStatus = DocumentStatus.PENDING
    text: str | None = None
    structure: list[StructureNode] | None = None
