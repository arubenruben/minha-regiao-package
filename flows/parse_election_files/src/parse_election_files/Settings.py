from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    hf_api_key: str
    hf_dataset_repo_id: str
    hf_dataset_config_name: str = "raw_election_files"


settings = Settings()
