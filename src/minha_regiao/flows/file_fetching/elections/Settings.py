from minha_regiao.BaseSettings import BaseSettings

class Settings(BaseSettings):
    sg_mai_link: str
    ollama_model: str
    
    gemini_api_key: str
    gemini_model: str

    hf_api_key: str
    hf_repo_name: str