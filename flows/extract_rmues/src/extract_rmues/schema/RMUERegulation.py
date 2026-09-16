from enum import Enum

from pydantic import BaseModel

from minha_regiao.gazette.RegulationStructure import StructureNode


class DocumentStatus(str, Enum):
    """Outcome of a document's PDF-url resolution / download / text-
    extraction pipeline (see extract_rmues.tasks.ProcessCity). Mirrors
    extract_pdms.schema.RegulationDocument.DocumentStatus -- same kind of
    per-document pipeline, over a raw DR gazette page range either way --
    plus two states specific to this flow's own earlier steps (resolving
    the PDF url in the first place, and parsing the notice metadata its
    doc_type/number/year come from).
    """

    # Not yet attempted -- e.g. reused unchanged from a previous run's
    # not-yet-processed record.
    PENDING = "pending"
    OK = "ok"
    # The DR detail page never revealed a PDF url (see
    # extract_rmues.services.PDFResolver.resolve_pdf_url).
    PDF_URL_NOT_FOUND = "pdf_url_not_found"
    # This document's name (as printed on the RMUE listing page) doesn't
    # match the expected "<Tipo> n.º <número>/<ano>" shape, so its
    # doc_type/number/year -- needed to locate its own notice inside the
    # downloaded page range -- couldn't be parsed (see
    # extract_rmues.services.RegulationMetadata.parse_notice_metadata).
    METADATA_UNPARSEABLE = "metadata_unparseable"
    DOWNLOAD_FAILED = "download_failed"
    EXTRACTION_FAILED = "extraction_failed"
    # Downloaded and parsed fine but yielded no text at all: an old,
    # scanned regulation that needs OCR (not yet supported).
    NEEDS_OCR = "needs_ocr"
    # Extraction succeeded, but this document's own notice couldn't be
    # located inside the downloaded Diário da República page range -- see
    # minha_regiao.gazette.GazetteSegmenter.find_notice_text_by_heading.
    NOTICE_NOT_FOUND = "notice_not_found"


class RegulationDocument(BaseModel):
    name: str
    dre_url: str

    # Populated by extract_rmues.tasks.ProcessCity's per-city pipeline.
    # `raw_text` and `structure` are null whenever `status` isn't OK -- the
    # PDF couldn't be resolved/downloaded/read, or (NOTICE_NOT_FOUND) this
    # document's own notice couldn't be isolated from the page range, so
    # there's nothing reliable to report. When `status` is OK, `raw_text`
    # is *not* the whole downloaded page's raw text -- it's already
    # narrowed, by minha_regiao.gazette.GazetteSegmenter, to just this
    # document's own notice within that (possibly multi-municipality) page
    # range. `structure` is that same narrowed text broken down into its
    # nested Parte/Título/Capítulo/Secção/Subsecção/Artigo hierarchy (see
    # minha_regiao.gazette.StructureParser and
    # minha_regiao.gazette.RegulationStructure -- shared with extract_pdms);
    # it's additive, `raw_text` remains the authoritative full text of this
    # notice.
    pdf_url: str | None = None
    status: DocumentStatus = DocumentStatus.PENDING
    raw_text: str | None = None
    structure: list[StructureNode] | None = None


class RMUERegulation(BaseModel):
    municipality: str
    urbanization_documents: list[RegulationDocument] = []
    fee_documents: list[RegulationDocument] = []
