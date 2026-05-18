# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jwt_private_key_path: Path
    jwt_public_key_path: Path
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7
    environment: str = "development"

    @property
    def jwt_private_key(self) -> str:
        return self.jwt_private_key_path.read_text()

    @property
    def jwt_public_key(self) -> str:
        return self.jwt_public_key_path.read_text()


settings = Settings()
