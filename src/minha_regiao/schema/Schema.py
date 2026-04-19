from abc import ABC
from pydantic import BaseModel, field_validator

class Schema(ABC, BaseModel):
    @field_validator("*", mode="before")
    @classmethod
    def trim_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v