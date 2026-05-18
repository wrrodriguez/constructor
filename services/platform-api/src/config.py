# src/config.py
from functools import cached_property
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=False)

    database_url: str
    redis_url: str
    jwt_private_key_path: Path
    jwt_public_key_path: Path
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7
    environment: str = "development"
    cors_allowed_origins: list[str] = ["http://localhost:4200"]

    @cached_property
    def jwt_private_key(self) -> str:
        return self.jwt_private_key_path.read_text()

    @cached_property
    def jwt_public_key(self) -> str:
        return self.jwt_public_key_path.read_text()


settings = Settings()
