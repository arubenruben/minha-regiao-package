from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"

    hf_api_key: str | None = None
    geo_dataset_repo_id: str = "minharegiao/portuguese-geo"


settings = Settings()
