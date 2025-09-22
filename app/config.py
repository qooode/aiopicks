"""Application configuration models."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from collections.abc import Iterable, Sequence

from pydantic import AliasChoices, Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .stable_catalogs import STABLE_CATALOGS, STABLE_CATALOG_COUNT, StableCatalogDefinition


DEFAULT_CATALOG_KEYS: tuple[str, ...] = tuple(definition.key for definition in STABLE_CATALOGS)


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
        default=1_000, alias="TRAKT_HISTORY_LIMIT", ge=10, le=10_000
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
    catalog_count: int | None = Field(
        default=None,
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
        default=3, alias="GENERATION_RETRY_LIMIT", ge=0, le=10
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

    @property
    def catalog_definitions(self) -> tuple[StableCatalogDefinition, ...]:
        """Return the configured stable catalog definitions in order."""

        definitions_by_key = {definition.key: definition for definition in STABLE_CATALOGS}
        return tuple(definitions_by_key[key] for key in self.catalog_keys)

    @field_validator("catalog_keys", mode="before")
    @classmethod
    def _parse_catalog_keys(cls, value: object) -> tuple[str, ...] | object:
        """Parse raw catalog key inputs into a tuple of unique identifiers."""

        if value is None or value == "":
            return DEFAULT_CATALOG_KEYS

        if isinstance(value, str):
            candidates: Iterable[str] = (part.strip() for part in value.split(","))
        elif isinstance(value, Sequence):
            candidates = (str(part).strip() for part in value)
        elif isinstance(value, Iterable):
            candidates = (str(part).strip() for part in value)
        else:
            raise ValueError("CATALOG_KEYS must be a comma-separated string or list of keys")

        filtered: list[str] = []
        for candidate in candidates:
            identifier = candidate.casefold()
            if not identifier:
                continue
            if identifier not in filtered:
                filtered.append(identifier)

        if not filtered:
            return DEFAULT_CATALOG_KEYS
        return tuple(filtered)

    @model_validator(mode="after")
    def _validate_catalog_configuration(self) -> "Settings":
        """Ensure configured catalogs line up with available stable definitions."""

        if not self.catalog_keys:
            raise ValueError("At least one catalog key must be configured")

        available = {definition.key for definition in STABLE_CATALOGS}
        invalid = sorted(set(self.catalog_keys) - available)
        if invalid:
            keys = ", ".join(invalid)
            raise ValueError(f"Unknown catalog keys configured: {keys}")

        resolved_count = len(self.catalog_keys)
        if self.catalog_count is None:
            object.__setattr__(self, "catalog_count", resolved_count)
        elif self.catalog_count != resolved_count:
            raise ValueError(
                "CATALOG_COUNT must match the number of configured catalog keys"
            )

        return self

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()  # type: ignore[call-arg]


settings = get_settings()
