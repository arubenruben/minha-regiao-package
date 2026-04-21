import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

# Get the directory where Settings.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8")

    smart_proxy_api_key: Optional[str] = None
    decodo_api_key: Optional[str] = None
