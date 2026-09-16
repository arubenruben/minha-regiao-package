from pydantic import BaseModel

from extract_rmues.schema.RMUERegulation import RMUERegulation


class RMUEExtractionState(BaseModel):
    """Persisted, resumable state for extract_rmues' per-city pipeline (PDF
    url resolution + notice text/structure extraction) -- see OutputStore.
    Idempotence is tracked per document (keyed by dre_url), not per
    municipality: a document already recorded here is reused on the next
    run instead of reopening a browser and re-downloading its PDF,
    regardless of whether that previously succeeded or failed.
    """

    entries: list[RMUERegulation] = []
