from pydantic import BaseModel

from extract_pdms.schema.PDMRecord import PDMRecord


class ExtractionState(BaseModel):
    """Persisted, resumable state for a run of the extract_pdms flow: every
    PDM record resolved so far, each carrying its own regulation documents
    -- idempotence is tracked per document (see OutputStore), not per PDM
    or per municipality, since a document's download/text-extraction is the
    expensive, failure-prone step worth not repeating.
    """

    records: list[PDMRecord] = []
