from enum import Enum


class RegulationDocumentStatus(str, Enum):
    """Outcome of a regulation document's PDF-url resolution / download /
    text-extraction pipeline, shared by `RMUE`, `FeeRegulation`, and
    `PDMDocument`. Mirrors the union of `extract_rmues.schema.RMUERegulation.
    DocumentStatus` and `extract_pdms.schema.RegulationDocument.
    DocumentStatus` -- the two flow-local enums this column's values are
    always assigned from (via `.value`) -- kept here rather than imported
    from either flow package so the shared `minha_regiao` entities have no
    dependency on flow-specific code.
    """

    PENDING = "pending"
    OK = "ok"
    PDF_URL_NOT_FOUND = "pdf_url_not_found"
    METADATA_UNPARSEABLE = "metadata_unparseable"
    DOWNLOAD_FAILED = "download_failed"
    EXTRACTION_FAILED = "extraction_failed"
    NEEDS_OCR = "needs_ocr"
    NOTICE_NOT_FOUND = "notice_not_found"
