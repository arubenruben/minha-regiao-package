from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    seg_mai_base_url: str
    openrouter_api_key: str
    openrouter_model: str
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    google_api_key: str | None = None
    gemini_model: str | None = None
    hf_api_key: str
    hf_dataset_repo_id: str

settings = Settings()
