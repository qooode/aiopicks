"""Application configuration models."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings loaded from environment variables or a .env file."""

    app_name: str = Field(default="AIOPicks", alias="APP_NAME")
    server_host: str = Field(default="0.0.0.0", alias="HOST")
    server_port: int = Field(default=3000, alias="PORT")

    trakt_client_id: str | None = Field(default=None, alias="TRAKT_CLIENT_ID")
    trakt_client_secret: str | None = Field(
        default=None, alias="TRAKT_CLIENT_SECRET"
    )
    trakt_access_token: str | None = Field(default=None, alias="TRAKT_ACCESS_TOKEN")
    trakt_redirect_uri: HttpUrl | None = Field(
        default=None, alias="TRAKT_REDIRECT_URI"
    )
    trakt_history_limit: int = Field(default=500, alias="TRAKT_HISTORY_LIMIT", ge=10, le=2000)

    openrouter_api_key: str | None = Field(
        default=None, alias="OPENROUTER_API_KEY"
    )
    openrouter_model: str = Field(
        default="google/gemini-2.5-flash-lite", alias="OPENROUTER_MODEL"
    )

    catalog_count: int = Field(default=6, alias="CATALOG_COUNT", ge=1, le=12)
    refresh_interval_seconds: int = Field(
        default=43_200, alias="REFRESH_INTERVAL", ge=3_600
    )
    response_cache_seconds: int = Field(
        default=1_800, alias="CACHE_TTL", ge=300
    )

    trakt_api_url: HttpUrl = Field(
        default="https://api.trakt.tv", alias="TRAKT_API_URL"
    )
    trakt_authorize_url: HttpUrl = Field(
        default="https://trakt.tv/oauth/authorize", alias="TRAKT_AUTHORIZE_URL"
    )
    openrouter_api_url: HttpUrl = Field(
        default="https://openrouter.ai/api/v1", alias="OPENROUTER_API_URL"
    )
    cinemeta_api_url: HttpUrl = Field(
        default="https://v3-cinemeta.strem.io", alias="CINEMETA_API_URL"
    )

    database_url: str = Field(
        default="sqlite+aiosqlite:///./aiopicks.db", alias="DATABASE_URL"
    )

    environment: Literal["development", "production"] = Field(
        default="development", alias="ENVIRONMENT"
    )

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()  # type: ignore[call-arg]


settings = get_settings()
