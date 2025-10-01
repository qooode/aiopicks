"""Application configuration models."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Literal

from pydantic import AliasChoices, Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .stable_catalogs import (
    STABLE_CATALOGS,
    STABLE_CATALOG_COUNT,
    StableCatalogDefinition,
)


DEFAULT_CATALOG_KEYS: tuple[str, ...] = tuple(
    definition.key for definition in STABLE_CATALOGS
)


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
        default=0, alias="TRAKT_HISTORY_LIMIT", ge=0, le=10_000
    )

    openrouter_api_key: str | None = Field(
        default=None, alias="OPENROUTER_API_KEY"
    )
    openrouter_model: str = Field(
        default="google/gemini-2.5-flash-lite", alias="OPENROUTER_MODEL"
    )

    catalog_keys: tuple[str, ...] = Field(
        default=DEFAULT_CATALOG_KEYS,
        alias="CATALOG_KEYS",
    )
    catalog_count: int = Field(
        default=STABLE_CATALOG_COUNT,
        alias="CATALOG_COUNT",
        ge=1,
        le=STABLE_CATALOG_COUNT,
    )
    catalog_item_count: int = Field(
        default=8, alias="CATALOG_ITEM_COUNT", ge=1, le=100
    )
    refresh_interval_seconds: int = Field(
        default=43_200, alias="REFRESH_INTERVAL", ge=3_600
    )
    response_cache_seconds: int = Field(
        default=1_800, alias="CACHE_TTL", ge=300
    )

    generation_retry_limit: int = Field(
        default=3, alias="GENERATION_RETRY_LIMIT", ge=0, le=50
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

    @field_validator("catalog_keys", mode="before")
    @classmethod
    def _parse_catalog_keys(cls, value: object) -> tuple[str, ...]:
        """Normalise catalog key selections from environment values."""

        if value is None:
            return DEFAULT_CATALOG_KEYS
        if isinstance(value, str):
            raw_values = [part.strip() for part in value.split(",")]
        elif isinstance(value, Iterable):
            raw_values = [str(part).strip() for part in value]
        else:
            raise TypeError("CATALOG_KEYS must be a string or iterable of strings")

        cleaned: list[str] = []
        for entry in raw_values:
            if not entry:
                continue
            slug = entry.replace("_", "-").replace(" ", "-").lower()
            slug = "-".join(filter(None, slug.split("-")))
            if not slug:
                continue
            if slug not in DEFAULT_CATALOG_KEYS:
                raise ValueError("Unknown catalog keys configured")
            if slug not in cleaned:
                cleaned.append(slug)
        if not cleaned:
            return DEFAULT_CATALOG_KEYS
        return tuple(cleaned)

    @model_validator(mode="after")
    def _sync_catalog_configuration(self) -> "Settings":
        """Ensure catalog counts mirror the configured keys."""

        expected = len(self.catalog_keys)
        if "catalog_count" in self.model_fields_set:
            if self.catalog_count != expected:
                raise ValueError(
                    "CATALOG_COUNT must match the number of configured catalog keys"
                )
        else:
            self.catalog_count = expected
        return self

    @property
    def catalog_definitions(self) -> tuple[StableCatalogDefinition, ...]:
        """Return ordered catalog lane definitions for the selected keys."""

        definition_map = {definition.key: definition for definition in STABLE_CATALOGS}
        return tuple(definition_map[key] for key in self.catalog_keys)

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
