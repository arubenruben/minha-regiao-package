from pathlib import Path
from typing import Literal

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
    # Only required when "huggingface" is in load_targets -- see
    # fetch_election_files, which only authenticates against Hugging Face
    # and only requires these when that sink is active.
    hf_api_key: str | None = None
    hf_dataset_repo_id: str | None = None

    # Which sinks the flow writes its structured election records to.
    # Defaults to json-only: with "huggingface" not in load_targets, neither
    # Hugging Face login, the raw-file upload, nor the dataset publish runs.
    load_targets: list[Literal["huggingface", "json"]] = ["json"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "elections.json"

settings = Settings()
