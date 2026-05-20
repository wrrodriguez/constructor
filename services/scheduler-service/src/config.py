# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://constructor:constructor_dev@localhost:5432/constructor_dev"
    redis_url: str = "redis://:redis_dev_password@localhost:6379/0"
    platform_api_url: str = "http://platform-api:8000"
    internal_secret: str = "internal_dev_secret"
    poll_interval_seconds: int = 30


settings = Settings()
