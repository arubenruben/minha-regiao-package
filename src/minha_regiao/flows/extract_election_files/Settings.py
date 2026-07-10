from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    seg_mai_base_url: str
    #hf_api_token: str
    #hf_dataset_name: str

settings = Settings()
