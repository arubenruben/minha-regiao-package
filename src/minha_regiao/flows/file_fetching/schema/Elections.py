from typing import List
from datetime import date
from pydantic import Field
from minha_regiao.flows.file_fetching.schema.Schema import Schema

class Elections(Schema):
    election_type: str
    election_name: str
    election_dates: List[date] = Field(..., description="List of dates where this type of election occurs in ISO format (YYYY-MM-DD)")