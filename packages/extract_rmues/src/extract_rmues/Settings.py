from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    rmue_url: str = "https://diariodarepublica.pt/dr/geral/areas-tematicas/regul-municipais"

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"


settings = Settings()
