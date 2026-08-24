from pydantic import BaseModel, field_validator


class MunicipalContact(BaseModel):
    municipality: str
    president: str
    address: str
    phone: str
    email: str | None = None
    website: str | None = None

    @field_validator("website")
    @classmethod
    def upgrade_to_https(cls, website: str | None) -> str | None:
        if website and website.startswith("http://"):
            return "https://" + website.removeprefix("http://")
        return website
