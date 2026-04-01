from environs import Env
from pydantic_settings import BaseSettings, SettingsConfigDict

Env().read_env(override=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    hf_api_key: str
    hf_repo_name: str
