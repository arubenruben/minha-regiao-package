from pathlib import Path
from typing import Literal

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

    # Which sinks the flow writes to at the end of its run. Defaults to
    # json-only, so reproducing this flow never requires a database or a
    # Hugging Face token; opt into "database"/"huggingface" explicitly.
    load_targets: list[Literal["database", "huggingface", "json"]] = ["json"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "cities.json"


settings = Settings()
