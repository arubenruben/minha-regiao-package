from pydantic import BaseModel, computed_field

from minha_regiao.flows.extract_cities.schema.MunicipalContact import MunicipalContact


class CityContacts(BaseModel):
    municipality: str
    ine_code: str | None = None
    town_hall: MunicipalContact
    municipal_assembly: MunicipalContact

    # town_hall.president and municipal_assembly.president hold different roles
    # (Presidente da Câmara Municipal vs. Presidente da Assembleia Municipal);
    # these give each an unambiguous name.
    @computed_field
    @property
    def mayor(self) -> str:
        return self.town_hall.president

    @computed_field
    @property
    def assembly_president(self) -> str:
        return self.municipal_assembly.president
