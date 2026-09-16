from typing import Literal

from pydantic import BaseModel


class PendingDocument(BaseModel):
    table: Literal["rmue", "fee_regulation"]
    # None when this document was built in-memory from a freshly-parsed
    # RMUERegulation rather than read back from a persisted DB row (see
    # extract_rmues.tasks.FindPendingDocuments) -- there's no row id to
    # carry yet in that case.
    id: int | None = None
    # Which RMUERegulation this document belongs to -- lets the caller
    # group documents by city for per-city processing (see
    # extract_rmues.tasks.ProcessCity) regardless of whether they came from
    # a fresh page parse or a Postgres backlog query.
    municipality: str
    name: str
    dre_url: str
    pdf_url: str | None = None
