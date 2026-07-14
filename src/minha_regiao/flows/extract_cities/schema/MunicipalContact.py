from pydantic import BaseModel


class MunicipalContact(BaseModel):
    municipality: str
    president: str
    address: str
    phone: str
    email: str | None = None
    website: str | None = None
