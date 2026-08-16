from typing import Literal

from pydantic import BaseModel


class PendingDocument(BaseModel):
    table: Literal["rmue", "fee_regulation"]
    id: int
    dre_url: str
    pdf_url: str | None = None
