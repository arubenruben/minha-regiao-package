from pydantic import Field
from typing import Sequence, Optional
from minha_regiao.schema.Schema import Schema


class TownHall(Schema):
    website: str
    nif: str
    president: str
    address: str
    postal_code: str
    email: str
    phone: str

    pdm_file: Optional[str] = Field(
        default=None,
        description="URL to the PDM file for this town hall",
    )

    rmue_file: Optional[str] = Field(
        default=None,
        description="URL to the RMUE file for this town hall",
    )

    fees_file: Optional[str] = Field(
        default=None,
        description="URL to the Fees file for this town hall",
    )

    pdm_candidate_files: Optional[Sequence[str]] = Field(
        default=None,
        description="List of URLs to candidate files for the PDM election",
    )

    rmue_candidate_files: Optional[Sequence[str]] = Field(
        default=None,
        description="List of URLs to candidate files for the RMUE election",
    )

    fees_candidate_files: Optional[Sequence[str]] = Field(
        default=None,
        description="List of URLs to candidate files for the Fees election",
    )
