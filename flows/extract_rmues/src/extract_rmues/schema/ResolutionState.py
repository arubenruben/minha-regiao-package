from pydantic import BaseModel

from extract_rmues.schema.PendingDocument import PendingDocument


class ResolutionState(BaseModel):
    """Persisted, resumable state for extract_rmues' PDF-url resolution step
    when the flow has no database configured -- idempotence is tracked per
    document (see ResolutionStore), keyed by dre_url.
    """

    documents: list[PendingDocument] = []
