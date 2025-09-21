"""Application configuration models."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Iterable, Literal, Sequence

from pydantic import AliasChoices, Field, HttpUrl, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .stable_catalogs import STABLE_CATALOGS, STABLE_CATALOG_COUNT


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
        default=2_000, alias="TRAKT_HISTORY_LIMIT", ge=10, le=10_000
    )

    openrouter_api_key: str | None = Field(
        default=None, alias="OPENROUTER_API_KEY"
    )
    openrouter_model: str = Field(
        default="google/gemini-2.5-flash-lite", alias="OPENROUTER_MODEL"
    )

    enabled_catalogs: tuple[str, ...] = Field(
        default=tuple(definition.key for definition in STABLE_CATALOGS),
        validation_alias=AliasChoices(
            "ENABLED_CATALOGS",
            "CATALOG_KEYS",
            "CATALOGS",
            "CATALOG_COUNT",
        ),
    )
    catalog_item_count: int = Field(
        default=40, alias="CATALOG_ITEM_COUNT", ge=1, le=100
    )
    combine_for_you_catalogs: bool = Field(
        default=False, alias="COMBINE_FOR_YOU_CATALOGS"
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

    @computed_field(return_type=int)
    @property
    def catalog_count(self) -> int:
        """Return the number of enabled catalog lanes."""

        return len(self.enabled_catalogs)

    @field_validator("enabled_catalogs", mode="before")
    @classmethod
    def _parse_enabled_catalogs(cls, value: object) -> Iterable[str] | object:
        """Normalise enabled catalog inputs from strings or numeric counts."""

        if value is None or value == "":
            return tuple(definition.key for definition in STABLE_CATALOGS)
        if isinstance(value, (tuple, list, set)):
            return tuple(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return tuple(definition.key for definition in STABLE_CATALOGS)
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, Sequence):
                    return tuple(parsed)
            parts = [part.strip() for part in stripped.split(",") if part.strip()]
            return tuple(parts) if parts else tuple(definition.key for definition in STABLE_CATALOGS)
        if isinstance(value, (int, float)):
            count = int(value)
            if count <= 0:
                return tuple(definition.key for definition in STABLE_CATALOGS)
            limited = min(count, STABLE_CATALOG_COUNT)
            return tuple(
                definition.key for definition in STABLE_CATALOGS[:limited]
            )
        return value

    @field_validator("enabled_catalogs")
    @classmethod
    def _validate_enabled_catalogs(
        cls, value: Iterable[str]
    ) -> tuple[str, ...]:
        """Ensure enabled catalogs exist and de-duplicate while preserving order."""

        valid_keys = {definition.key for definition in STABLE_CATALOGS}
        resolved: list[str] = []
        seen: set[str] = set()
        for raw_key in value:
            key = str(raw_key).strip().lower()
            if not key or key in seen or key not in valid_keys:
                continue
            seen.add(key)
            resolved.append(key)
        if not resolved:
            raise ValueError("At least one catalog must be enabled")
        return tuple(resolved)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()  # type: ignore[call-arg]


settings = get_settings()
