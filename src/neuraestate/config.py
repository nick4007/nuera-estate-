# src/neuraestate/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # Tell Pydantic to load from .env
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",   # ignore unknown env vars
    )

    # Environment variables (with defaults as fallback)
    DATABASE_URL: str = Field(
        default="postgresql+psycopg2://postgres:postgres@localhost:5433/neuraestate"
    )
    LOG_LEVEL: str = Field(default="INFO")
    APP_ENV: str = Field(default="dev")

# Instantiate settings once, so you can import anywhere
settings = Settings()


