# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    google_api_key: str = ""
    environment: str = "development"
    max_concurrent_agents: int = 10
    use_stub_model: bool = False


settings = Settings()
