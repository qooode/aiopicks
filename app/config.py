"""Application configuration models."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .stable_catalogs import STABLE_CATALOG_COUNT


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
    trakt_history_limit: int = Field(
        default=2_000, alias="TRAKT_HISTORY_LIMIT", ge=10, le=2000
    )

    openrouter_api_key: str | None = Field(
        default=None, alias="OPENROUTER_API_KEY"
    )
    openrouter_model: str = Field(
        default="google/gemini-2.5-flash-lite", alias="OPENROUTER_MODEL"
    )

    catalog_count: int = Field(
        default=STABLE_CATALOG_COUNT,
        alias="CATALOG_COUNT",
        ge=STABLE_CATALOG_COUNT,
        le=STABLE_CATALOG_COUNT,
    )
    catalog_item_count: int = Field(
        default=40, alias="CATALOG_ITEM_COUNT", ge=1, le=100
    )
    refresh_interval_seconds: int = Field(
        default=43_200, alias="REFRESH_INTERVAL", ge=3_600
    )
    response_cache_seconds: int = Field(
        default=1_800, alias="CACHE_TTL", ge=300
    )

    generation_retry_limit: int = Field(
        default=1, alias="GENERATION_RETRY_LIMIT", ge=0, le=10
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
    metadata_addon_url: HttpUrl | None = Field(
        default=None,
        alias="METADATA_ADDON_URL",
        validation_alias=AliasChoices("METADATA_ADDON_URL", "CINEMETA_API_URL"),
    )

    database_url: str = Field(
        default="sqlite+aiosqlite:///./aiopicks.db", alias="DATABASE_URL"
    )

    environment: Literal["development", "production"] = Field(
        default="development", alias="ENVIRONMENT"
    )

    @property
    def cinemeta_api_url(self) -> HttpUrl | None:
        """Maintain backwards compatibility with the previous setting name."""

        return self.metadata_addon_url

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()  # type: ignore[call-arg]


settings = get_settings()
