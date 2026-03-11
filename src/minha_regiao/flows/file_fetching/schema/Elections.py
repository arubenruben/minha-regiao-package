from datetime import date
from typing import List, Union
from pydantic import Field, field_validator
from minha_regiao.flows.file_fetching.schema.Schema import Schema

class Elections(Schema):
    election_type: str
    election_name: str
    election_dates: List[Union[date, str]] = Field(..., description="List of dates where this type of election occurs in ISO format (YYYY-MM-DD)")

    # Create a validator that casts election_dates to date objects if they are in string format
    @field_validator('election_dates', mode='before')
    def validate_election_dates(cls, v):
        if isinstance(v, list):
            validated_dates = []
            for item in v:
                if isinstance(item, str):
                    try:
                        validated_dates.append(date.fromisoformat(item))
                    except ValueError:
                        raise ValueError(f"Invalid date format: {item}. Expected ISO format (YYYY-MM-DD).")
                elif isinstance(item, date):
                    validated_dates.append(item)
                else:
                    raise ValueError(f"Invalid type for election date: {type(item)}. Expected str or date.")
            return validated_dates
        else:
            raise ValueError("election_dates must be a list of strings or date objects.")