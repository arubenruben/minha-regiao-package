from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    anmp_town_hall_url: str = "https://anmp.pt/municipios/municipios/contactos/?cod=MUN"
    anmp_municipal_assembly_url: str = "https://anmp.pt/municipios/municipios/contactos/?cod=AM"

    election_results_dataset_repo_id: str = "minharegiao/portuguese-elections"
    presidential_election_results_filename: str = "raw/presidential/PR_2026_Globais.xlsx"
    city_dataset_config_name: str = "cities"


settings = Settings()
