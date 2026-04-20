import os
from pydantic_settings import BaseSettings, SettingsConfigDict

# Get the directory where Settings.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")


class Settings(BaseSettings):
    smart_proxy_api_key: str

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8")
