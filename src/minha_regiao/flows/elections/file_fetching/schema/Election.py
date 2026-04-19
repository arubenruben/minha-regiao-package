from datetime import date
from typing import Literal
from pydantic import Field, field_validator
from minha_regiao.flows.elections.file_fetching.schema.Schema import Schema

class Election(Schema):
    election_type: Literal[
        'PR', 
        'AL', 
        'AR', 
        'ALRAM', 
        'PE', 
        'ALRAA', 
        'REF', 
        'AC'
    ]
    election_name: Literal[
        'PRESIDENTE DA REPÚBLICA - 2º Sufrágio',
        'PRESIDENTE DA REPÚBLICA - 1º Sufrágio',
        'AUTARQUIAS LOCAIS',
        'ASSEMBLEIA DA REPÚBLICA',
        'ASSEMBLEIA LEGISLATIVA REGIONAL - MADEIRA',
        'PARLAMENTO EUROPEU',
        'ASSEMBLEIA LEGISLATIVA REGIONAL - AÇORES',
        'REFERENDO NACIONAL (IVG)',
        'REFERENDO NACIONAL (REGIONALIZAÇÃO)',
        'ASSEMBLEIA CONSTITUINTE'
    ]
    election_date: date = Field(..., description="Date of the election in ISO format (YYYY-MM-DD)")

    # Create a validator that casts election_date to a date object if it is in string format
    @field_validator('election_date', mode='before')
    def validate_election_date(cls, v):
        if isinstance(v, str):
            try:
                return date.fromisoformat(v)
            except ValueError:
                raise ValueError(f"Invalid date format: {v}. Expected ISO format (YYYY-MM-DD).")
        elif isinstance(v, date):
            return v
        else:
            raise ValueError(f"Invalid type for election_date: {type(v)}. Expected str or date.")