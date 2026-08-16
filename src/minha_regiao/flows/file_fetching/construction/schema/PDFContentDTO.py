from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class PDFContentDTO(Schema):
    """Data Transfer Object for fetched PDF content."""

    url: str
    content: str
    number_of_pages: int
    fetched_at: Optional[str] = None
    file_size_bytes: Optional[int] = None

    @property
    def has_content(self) -> bool:
        """Check if the PDF has extracted text content."""
        return bool(self.content and self.content.strip())
