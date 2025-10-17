"""High level orchestration for catalog generation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import random
from contextlib import suppress
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    HttpUrl,
    ValidationError,
    field_validator,
)
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import Settings, DEFAULT_CATALOG_KEYS
from ..db_models import CatalogRecord, Profile
from ..models import Catalog, CatalogBundle, CatalogItem
from ..stable_catalogs import STABLE_CATALOGS, StableCatalogDefinition
from ..utils import slugify
from .metadata_addon import MetadataAddonClient, MetadataMatch
from .openrouter import OpenRouterClient
from .openai import OpenAIClient
from .trakt import HistoryBatch, TraktClient

logger = logging.getLogger(__name__)


class ManifestConfig(BaseModel):
    """Normalized view of query parameters controlling profile selection."""

    profile_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("profile", "profileId"),
    )
    openrouter_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("openrouterKey", "openRouterKey"),
    )
    openrouter_model: str | None = Field(
        default=None,
        validation_alias=AliasChoices("openrouterModel", "openRouterModel"),
    )
    # OpenAI overrides
    openai_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("openaiKey", "openAIKey"),
    )
    openai_model: str | None = Field(
        default=None,
        validation_alias=AliasChoices("openaiModel", "openAIModel"),
    )
    generator_mode: str | None = Field(
        default=None,
        validation_alias=AliasChoices("engine", "generator", "mode", "discoveryEngine"),
        description="Discovery engine selection: 'openrouter', 'openai', or 'local'",
    )
    manifest_name: str | None = Field(
        default=None,
        max_length=120,
        validation_alias=AliasChoices("manifestName", "addonName"),
    )
    catalog_keys: tuple[str, ...] | None = Field(
        default=None,
        validation_alias=AliasChoices("catalogKeys", "catalog_keys"),
    )
    catalog_item_count: int | None = Field(
        default=None,
        ge=1,
        le=100,
        validation_alias=AliasChoices(
            "catalogItems", "catalogItemCount", "itemsPerCatalog"
        ),
    )
    generation_retry_limit: int | None = Field(
        default=None,
        ge=0,
        le=50,
        validation_alias=AliasChoices(
            "generationRetries", "retryLimit", "maxRetries"
        ),
    )
    refresh_interval: int | None = Field(
        default=None,
        ge=3_600,
        validation_alias=AliasChoices("refreshInterval", "refreshSeconds"),
    )
    response_cache: int | None = Field(
        default=None,
        ge=300,
        validation_alias=AliasChoices("cacheTtl", "cacheTTL", "cacheSeconds"),
    )
    trakt_history_limit: int | None = Field(
        default=None,
        ge=0,
        le=10_000,
        validation_alias=AliasChoices("traktHistoryLimit", "historyLimit"),
    )
    trakt_client_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("traktClientId", "traktClientID"),
    )
    trakt_access_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices("traktAccessToken", "traktToken"),
    )
    metadata_addon_url: HttpUrl | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "metadataAddon",
            "metadataAddonUrl",
            "cinemetaUrl",
        ),
    )

    @classmethod
    def from_query(cls, params: Mapping[str, str]) -> "ManifestConfig":
        return cls.from_request(params)

    @classmethod
    def from_request(
        cls, params: Mapping[str, str], *, profile_id: str | None = None
    ) -> "ManifestConfig":
        payload = dict(params)
        if profile_id is not None:
            payload["profile"] = profile_id
        return cls.model_validate(payload)

    @field_validator("catalog_keys", mode="before")
    @classmethod
    def _parse_catalog_keys(cls, value: object) -> object:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            raw_values = [part.strip() for part in value.split(",")]
        elif isinstance(value, Sequence):
            raw_values = [str(part).strip() for part in value]
        else:
            raise TypeError("catalogKeys must be a string or iterable of strings")

        cleaned: list[str] = []
        for entry in raw_values:
            if not entry:
                continue
            slug = entry.replace("_", "-").replace(" ", "-").lower()
            slug = "-".join(part for part in slug.split("-") if part)
            if not slug:
                continue
            if slug not in DEFAULT_CATALOG_KEYS:
                raise ValueError("Unknown catalog keys configured")
            if slug not in cleaned:
                cleaned.append(slug)
        if not cleaned:
            return None
        return tuple(cleaned)

    @field_validator(
        "catalog_item_count",
        "generation_retry_limit",
        "refresh_interval",
        "response_cache",
        "trakt_history_limit",
        mode="before",
    )
    @classmethod
    def _parse_optional_int(cls, value: object) -> object:
        if value is None or value == "":
            return None
        if isinstance(value, int):
            return value
        try:
            return int(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("Value must be an integer") from exc

    @field_validator(
        "profile_id",
        "openrouter_key",
        "openrouter_model",
        "generator_mode",
        "manifest_name",
        "trakt_client_id",
        "trakt_access_token",
        "metadata_addon_url",
        mode="before",
    )
    @classmethod
    def _strip_blank(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @field_validator("generator_mode")
    @classmethod
    def _normalize_engine(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered in {"openrouter", "ai", "llm"}:
            return "openrouter"
        if lowered in {"openai", "open-ai"}:
            return "openai"
        if lowered in {"local", "offline"}:
            return "local"
        raise ValueError("generator mode must be 'openrouter' or 'local'")


@dataclass
class ProfileState:
    """Snapshot of a stored profile used for runtime decisions."""

    id: str
    openrouter_api_key: str
    openrouter_model: str
    generator_mode: str
    trakt_client_id: str | None
    trakt_access_token: str | None
    catalog_keys: tuple[str, ...]
    catalog_item_count: int
    generation_retry_limit: int
    refresh_interval_seconds: int
    response_cache_seconds: int
    trakt_history_limit: int
    next_refresh_at: datetime | None
    last_refreshed_at: datetime | None
    metadata_addon_url: str | None = None
    trakt_movie_history_count: int = 0
    trakt_show_history_count: int = 0
    trakt_history_refreshed_at: datetime | None = None
    trakt_history_snapshot: dict[str, Any] | None = None
    # OpenAI credentials
    openai_api_key: str = ""
    openai_model: str = ""


@dataclass
class ProfileContext:
    """Resolved profile along with whether a refresh is required."""

    state: ProfileState
    force_refresh: bool = False


@dataclass
class ProfileIdentity:
    """Lightweight identity information for a profile."""

    id: str
    display_name: str | None = None


@dataclass
class ProfileStatus:
    """Expose runtime information about a profile for the config UI."""

    state: ProfileState
    has_catalogs: bool
    needs_refresh: bool
    refreshing: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "profileId": self.state.id,
            "generator": self.state.generator_mode,
            "openrouterModel": self.state.openrouter_model,
            "openaiModel": self.state.openai_model,
            "catalogItemCount": self.state.catalog_item_count,
            "catalogKeys": list(self.state.catalog_keys),
            "generationRetryLimit": self.state.generation_retry_limit,
            "refreshIntervalSeconds": self.state.refresh_interval_seconds,
            "responseCacheSeconds": self.state.response_cache_seconds,
            "metadataAddon": self.state.metadata_addon_url,
            "traktHistoryLimit": self.state.trakt_history_limit,
            "traktHistory": self._history_payload(),
            "lastRefreshedAt": (
                self.state.last_refreshed_at.isoformat() if self.state.last_refreshed_at else None
            ),
            "nextRefreshAt": (
                self.state.next_refresh_at.isoformat() if self.state.next_refresh_at else None
            ),
            "hasCatalogs": self.has_catalogs,
            "needsRefresh": self.needs_refresh,
            "refreshing": self.refreshing,
            "ready": self.has_catalogs and not self.refreshing,
        }

    def _history_payload(self) -> dict[str, Any]:
        payload = {
            "movies": self.state.trakt_movie_history_count,
            "shows": self.state.trakt_show_history_count,
            "refreshedAt": (
                self.state.trakt_history_refreshed_at.isoformat()
                if self.state.trakt_history_refreshed_at
                else None
            ),
        }
        if self.state.trakt_history_snapshot:
            payload["stats"] = self.state.trakt_history_snapshot
        return payload


@dataclass(slots=True)
class WatchedMediaIndex:
    """Fingerprints and samples of completed titles per content type."""

    fingerprints: set[str]
    recent_titles: list[str]


class CatalogService:
    """Coordinates Trakt ingestion with AI catalog generation."""

    _CATALOG_SCOPE_SEPARATOR = "__"

    def __init__(
        self,
        settings: Settings,
        trakt_client: TraktClient,
        openrouter_client: OpenRouterClient,
        openai_client: OpenAIClient,
        metadata_client: MetadataAddonClient,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._settings = settings
        self._trakt = trakt_client
        self._openrouter = openrouter_client
        self._openai = openai_client
        self._metadata_client = metadata_client
        self._session_factory = session_factory
        self._default_metadata_addon_url = getattr(
            metadata_client, "default_base_url", None
        )
        self._catalog_definitions = tuple(STABLE_CATALOGS)
        self._catalog_definition_map = {
            definition.key: definition for definition in self._catalog_definitions
        }
        self._all_catalog_keys = tuple(self._catalog_definition_map.keys())
        self._default_catalog_keys = self._normalise_catalog_keys(
            settings.catalog_keys, fallback=self._all_catalog_keys
        )
        self._locks: dict[str, asyncio.Lock] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_poll_seconds = 60
        self._refresh_jobs: dict[str, asyncio.Task[None]] = {}

    async def start(self) -> None:
        """Initialise the service and launch the refresh loop."""

        await self._ensure_default_profile()
        default_state = await self._load_profile_state("default")
        if default_state:
            await self.ensure_catalogs(default_state, force=True)
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        """Stop the background refresh loop."""

        if self._refresh_task is None:
            return
        self._refresh_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._refresh_task
        self._refresh_task = None

    async def list_manifest_catalogs(
        self, config: ManifestConfig
    ) -> tuple[ProfileState, list[dict[str, Any]]]:
        """Return manifest catalog entries for the resolved profile."""

        state = await self.prepare_profile(config, wait_for_refresh=True)
        catalogs = await self._load_catalogs(state.id)
        grouped: dict[str, list[Catalog]] = {"movie": [], "series": []}
        for catalog in catalogs:
            grouped.setdefault(catalog.type, []).append(catalog)
        manifest_entries: list[dict[str, Any]] = []
        for content_type in ("movie", "series"):
            for catalog in grouped.get(content_type, []):
                manifest_entries.append(catalog.to_manifest_entry())
        return state, manifest_entries

    async def get_catalog_payload(
        self,
        config: ManifestConfig,
        content_type: str,
        catalog_id: str,
    ) -> dict[str, Any]:
        """Return the catalog payload for a profile/content combination."""

        state: ProfileState | None = None
        try:
            state = await self.prepare_profile(config, wait_for_refresh=True)
        except ValueError:
            state = None

        if state is not None:
            catalog = await self._load_single_catalog(state.id, content_type, catalog_id)
            if catalog is not None:
                return catalog.to_catalog_response()

        fallback_catalog = await self._load_catalog_any_profile(
            catalog_id, content_type=content_type
        )
        if fallback_catalog is not None:
            return fallback_catalog.to_catalog_response()
        profile_ref = state.id if state is not None else "unknown"
        raise KeyError(f"Catalog {catalog_id} not found for profile {profile_ref}")

    async def prepare_profile(
        self, config: ManifestConfig, *, wait_for_refresh: bool = True
    ) -> ProfileState:
        """Resolve the profile, ensure catalogs are current, and return its state."""

        context = await self._resolve_profile(config)
        return await self.ensure_catalogs(
            context.state,
            force=context.force_refresh,
            wait=wait_for_refresh,
        )

    async def ensure_catalogs(
        self, state: ProfileState, *, force: bool = False, wait: bool = True
    ) -> ProfileState:
        """Refresh catalogs for the profile if the cache is stale."""

        lock = self._locks.setdefault(state.id, asyncio.Lock())
        async with lock:
            latest_state = await self._load_profile_state(state.id) or state
            needs_refresh, has_catalogs = await self._needs_refresh(latest_state)
            if force:
                needs_refresh = True
            if not needs_refresh:
                await self._ensure_catalog_scope(latest_state)
                return latest_state
            if wait or not has_catalogs:
                await self._refresh_catalogs(latest_state)
                refreshed_state = await self._load_profile_state(state.id)
                final_state = refreshed_state or latest_state
                await self._ensure_catalog_scope(final_state)
                return final_state
            self._schedule_refresh(latest_state, force=True)
            return latest_state

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._refresh_poll_seconds)
            try:
                await self._refresh_due_profiles()
            except Exception as exc:  # pragma: no cover - background safety net
                logger.exception("Scheduled refresh failed: %s", exc)

    async def _refresh_due_profiles(self) -> None:
        now = datetime.utcnow()
        async with self._session_factory() as session:
            stmt = select(Profile.id).where(
                Profile.next_refresh_at.is_not(None),
                Profile.next_refresh_at <= now,
            )
            result = await session.execute(stmt)
            profile_ids = [row[0] for row in result.all()]

        for profile_id in profile_ids:
            state = await self._load_profile_state(profile_id)
            if state is None:
                continue
            await self.ensure_catalogs(state, force=True, wait=False)

    async def _needs_refresh(self, state: ProfileState) -> tuple[bool, bool]:
        has_catalogs = await self._has_catalogs(state.id)
        if state.last_refreshed_at is None:
            return True, has_catalogs
        if not has_catalogs:
            return True, has_catalogs
        expires_at = state.last_refreshed_at + timedelta(
            seconds=state.response_cache_seconds
        )
        return datetime.utcnow() >= expires_at, has_catalogs

    async def _has_catalogs(self, profile_id: str) -> bool:
        async with self._session_factory() as session:
            stmt = (
                select(CatalogRecord.id)
                .where(CatalogRecord.profile_id == profile_id)
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

    def _schedule_refresh(self, state: ProfileState, *, force: bool = False) -> None:
        existing = self._refresh_jobs.get(state.id)
        if existing and not existing.done():
            return

        async def _runner() -> None:
            try:
                await self.ensure_catalogs(state, force=force, wait=True)
            except Exception as exc:  # pragma: no cover - background safety net
                logger.exception(
                    "Background refresh for profile %s failed: %s", state.id, exc
                )
            finally:
                self._refresh_jobs.pop(state.id, None)

        self._refresh_jobs[state.id] = asyncio.create_task(_runner())

    def request_refresh(self, state: ProfileState, *, force: bool = False) -> None:
        """Expose background refresh scheduling to API consumers."""

        self._schedule_refresh(state, force=force)

    async def _refresh_catalogs(self, state: ProfileState) -> None:
        logger.info(
            "Refreshing catalogs for profile %s via model %s",
            state.id,
            state.openrouter_model,
        )

        # Always fetch the full Trakt history for generation (no user-configurable limit)
        movie_history_batch, show_history_batch = await asyncio.gather(
            self._trakt.fetch_history(
                "movies",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=None,
            ),
            self._trakt.fetch_history(
                "shows",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=None,
            ),
        )
        movie_history = movie_history_batch.items
        show_history = show_history_batch.items

        movie_total, show_total, snapshot = await self._gather_trakt_history_metadata(
            state,
            movie_batch=movie_history_batch,
            show_batch=show_history_batch,
        )

        # Persist stats while reflecting that a full-history scan was used (limit=0)
        await self._store_trakt_history_stats(
            state,
            history_limit=0,
            movie_total=movie_total,
            show_total=show_total,
            snapshot=snapshot,
        )

        summary = self._build_summary(
            movie_history,
            show_history,
            state=state,
            catalog_item_count=state.catalog_item_count,
        )
        seed = secrets.token_hex(4)
        catalogs: dict[str, dict[str, Catalog]] | None = None
        metadata_url = state.metadata_addon_url or self._default_metadata_addon_url
        watched_index = self._build_watched_index(movie_history, show_history)
        # Collect fingerprints of items we most recently served to this profile
        served_index = await self._build_served_index(state.id)
        exclusion_payload = self._serialise_watched_index(watched_index)
        definitions = self._definitions_for_keys(state.catalog_keys)

        mode = getattr(state, "generator_mode", "openrouter")
        use_local = False
        if mode == "local":
            use_local = True
        elif mode == "openrouter" and not bool(state.openrouter_api_key):
            use_local = True
        elif mode == "openai" and not bool(getattr(state, "openai_api_key", "")):
            use_local = True

        if use_local:
            catalogs = await self._generate_local_catalogs(
                movie_history,
                show_history,
                definitions=definitions,
                seed=seed,
                item_limit=state.catalog_item_count,
                watched=watched_index,
                served=served_index,
                trakt_client_id=state.trakt_client_id,
                trakt_access_token=state.trakt_access_token,
            )
            await self._enrich_catalogs_with_metadata(catalogs, metadata_url)
        else:
            try:
                if mode == "openai":
                    bundle = await self._openai.generate_catalogs(
                        summary,
                        seed=seed,
                        api_key=state.openai_api_key,
                        model=(state.openai_model or self._settings.openai_model),
                        exclusions=exclusion_payload,
                        retry_limit=state.generation_retry_limit,
                        definitions=definitions,
                    )
                else:
                    bundle = await self._openrouter.generate_catalogs(
                    summary,
                    seed=seed,
                    api_key=state.openrouter_api_key,
                    model=state.openrouter_model,
                    exclusions=exclusion_payload,
                    retry_limit=state.generation_retry_limit,
                    definitions=definitions,
                    )
                catalogs = self._bundle_to_dict(bundle)
                self._prune_watched_items(catalogs, watched_index)
                await self._enrich_catalogs_with_metadata(catalogs, metadata_url)
                if not (catalogs["movie"] or catalogs["series"]):
                    logger.warning(
                        "AI returned an empty catalog bundle for profile %s; falling back to history",
                        state.id,
                    )
                    catalogs = None
            except Exception as exc:
                logger.exception(
                    "AI generation failed for profile %s, falling back to history data: %s",
                    state.id,
                    exc,
                )
                catalogs = None

        if catalogs is None:
            # Prefer full local generation over tiny fallback so all selected lanes appear
            try:
                catalogs = await self._generate_local_catalogs(
                    movie_history,
                    show_history,
                    definitions=definitions,
                    seed=seed,
                    item_limit=state.catalog_item_count,
                    watched=watched_index,
                    served=served_index,
                    trakt_client_id=state.trakt_client_id,
                    trakt_access_token=state.trakt_access_token,
                )
                await self._enrich_catalogs_with_metadata(catalogs, metadata_url)
            except Exception:
                # As a last resort, surface simple history compilations
                catalogs = self._build_fallback_catalogs(
                    movie_history,
                    show_history,
                    seed=seed,
                    item_limit=state.catalog_item_count,
                )
                await self._enrich_catalogs_with_metadata(catalogs, metadata_url)

        await self._store_catalogs(state, catalogs)

    async def _store_catalogs(
        self,
        state: ProfileState,
        catalogs: dict[str, dict[str, Catalog]],
    ) -> None:
        now = datetime.utcnow()
        scoped_catalogs = self._scope_catalog_payloads(state.id, catalogs)
        async with self._session_factory() as session:
            await session.execute(
                delete(CatalogRecord).where(CatalogRecord.profile_id == state.id)
            )
            for content_type, catalog_map in scoped_catalogs.items():
                for position, catalog in enumerate(catalog_map.values()):
                    payload = catalog.model_dump(mode="json")
                    expires_at = catalog.generated_at + timedelta(
                        seconds=state.response_cache_seconds
                    )
                    record = CatalogRecord(
                        profile_id=state.id,
                        content_type=content_type,
                        catalog_id=catalog.id,
                        title=catalog.title,
                        description=catalog.description,
                        seed=catalog.seed,
                        position=position,
                        payload=payload,
                        generated_at=catalog.generated_at,
                        expires_at=expires_at,
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(record)
            next_refresh = now + timedelta(seconds=state.refresh_interval_seconds)
            await session.execute(
                update(Profile)
                .where(Profile.id == state.id)
                .values(
                    last_refreshed_at=now,
                    next_refresh_at=next_refresh,
                    updated_at=now,
                )
            )
            await session.commit()

    def _scope_catalog_payloads(
        self, profile_id: str, catalogs: dict[str, dict[str, Catalog]]
    ) -> dict[str, dict[str, Catalog]]:
        scoped: dict[str, dict[str, Catalog]] = {}
        for content_type, catalog_map in catalogs.items():
            scoped_map: dict[str, Catalog] = {}
            for catalog in catalog_map.values():
                _, base_id = self._split_scoped_catalog_id(catalog.id)
                public_id = self._scoped_catalog_id(profile_id, base_id)
                scoped_catalog = catalog.model_copy(update={"id": public_id})
                scoped_map[public_id] = scoped_catalog
            scoped[content_type] = scoped_map
        return scoped

    async def _load_profile_state(self, profile_id: str) -> ProfileState | None:
        async with self._session_factory() as session:
            profile = await session.get(Profile, profile_id)
            if profile is None:
                return None
            return self._profile_to_state(profile)

    def _profile_to_state(self, profile: Profile) -> ProfileState:
        return ProfileState(
            id=profile.id,
            openrouter_api_key=profile.openrouter_api_key,
            openrouter_model=profile.openrouter_model,
            openai_api_key=getattr(profile, "openai_api_key", ""),
            openai_model=getattr(profile, "openai_model", ""),
            generator_mode=getattr(profile, "generator_mode", "openrouter") or "openrouter",
            trakt_client_id=profile.trakt_client_id,
            trakt_access_token=profile.trakt_access_token,
            catalog_keys=self._normalise_catalog_keys(
                getattr(profile, "catalog_keys", None)
            ),
            catalog_item_count=getattr(
                profile, "catalog_item_count", self._settings.catalog_item_count
            ),
            generation_retry_limit=getattr(
                profile,
                "generation_retry_limit",
                self._settings.generation_retry_limit,
            ),
            refresh_interval_seconds=profile.refresh_interval_seconds,
            response_cache_seconds=profile.response_cache_seconds,
            trakt_history_limit=getattr(
                profile,
                "trakt_history_limit",
                self._settings.trakt_history_limit,
            ),
            next_refresh_at=profile.next_refresh_at,
            last_refreshed_at=profile.last_refreshed_at,
            metadata_addon_url=getattr(profile, "metadata_addon_url", None),
            trakt_movie_history_count=getattr(
                profile, "trakt_movie_history_count", 0
            ),
            trakt_show_history_count=getattr(
                profile, "trakt_show_history_count", 0
            ),
            trakt_history_refreshed_at=getattr(
                profile, "trakt_history_refreshed_at", None
            ),
            trakt_history_snapshot=getattr(
                profile, "trakt_history_snapshot", None
            ),
        )

    async def _gather_trakt_history_metadata(
        self,
        state: ProfileState,
        *,
        movie_batch: HistoryBatch | None,
        show_batch: HistoryBatch | None,
    ) -> tuple[int | None, int | None, dict[str, Any] | None]:
        """Combine Trakt history totals with richer aggregated stats."""

        movie_total = (
            movie_batch.total if movie_batch is not None and movie_batch.fetched else None
        )
        show_total = (
            show_batch.total if show_batch is not None and show_batch.fetched else None
        )
        snapshot: dict[str, Any] | None = None

        if not state.trakt_access_token:
            return movie_total, show_total, snapshot

        stats = await self._trakt.fetch_stats(
            client_id=state.trakt_client_id,
            access_token=state.trakt_access_token,
        )
        if stats:
            movie_watched = self._extract_trakt_watched(stats, "movies")
            show_watched = self._extract_trakt_watched(stats, "shows")
            if movie_watched is not None:
                movie_total = movie_watched
            if show_watched is not None:
                show_total = show_watched
            snapshot_data = self._build_trakt_history_snapshot(stats)
            if snapshot_data:
                snapshot = snapshot_data

        return movie_total, show_total, snapshot

    async def _store_trakt_history_stats(
        self,
        state: ProfileState,
        *,
        history_limit: int,
        movie_total: int | None,
        show_total: int | None,
        snapshot: dict[str, Any] | None = None,
    ) -> None:
        """Persist Trakt history counts and keep the in-memory state fresh."""

        state.trakt_history_limit = history_limit
        timestamp: datetime | None = None
        if movie_total is not None:
            state.trakt_movie_history_count = movie_total
            timestamp = datetime.utcnow()
        if show_total is not None:
            state.trakt_show_history_count = show_total
            if timestamp is None:
                timestamp = datetime.utcnow()
        if snapshot is not None:
            state.trakt_history_snapshot = snapshot or None
        if timestamp is not None:
            state.trakt_history_refreshed_at = timestamp

        async with self._session_factory() as session:
            profile = await session.get(Profile, state.id)
            if profile is None:
                return

            updated = False
            if getattr(profile, "trakt_history_limit", None) != history_limit:
                profile.trakt_history_limit = history_limit
                updated = True
            if movie_total is not None and (
                movie_total
                != getattr(profile, "trakt_movie_history_count", None)
            ):
                profile.trakt_movie_history_count = movie_total
                updated = True
            if show_total is not None and (
                show_total != getattr(profile, "trakt_show_history_count", None)
            ):
                profile.trakt_show_history_count = show_total
                updated = True
            if snapshot is not None and (
                snapshot or None
            ) != getattr(profile, "trakt_history_snapshot", None):
                profile.trakt_history_snapshot = snapshot or None
                updated = True
            if timestamp is not None and (
                getattr(profile, "trakt_history_refreshed_at", None) != timestamp
            ):
                profile.trakt_history_refreshed_at = timestamp
                updated = True
            if timestamp is None and not updated:
                # Keep the in-memory timestamp aligned with the stored value.
                state.trakt_history_refreshed_at = getattr(
                    profile, "trakt_history_refreshed_at", None
                )
            if updated:
                now = datetime.utcnow()
                profile.updated_at = now
                await session.commit()

    @staticmethod
    def _extract_trakt_watched(
        stats: Mapping[str, Any], section: str
    ) -> int | None:
        """Extract the watched counter for a given stats section."""

        segment = stats.get(section)
        if isinstance(segment, Mapping):
            value = segment.get("watched")
            if isinstance(value, int) and value >= 0:
                return value
        return None

    @staticmethod
    def _build_trakt_history_snapshot(stats: Mapping[str, Any]) -> dict[str, Any]:
        """Normalise rich Trakt stats for UI consumption."""

        def _clean(section: str, keys: tuple[str, ...]) -> dict[str, int]:
            raw = stats.get(section)
            if not isinstance(raw, Mapping):
                return {}
            cleaned: dict[str, int] = {}
            for key in keys:
                value = raw.get(key)
                if isinstance(value, int) and value >= 0:
                    cleaned[key] = value
            return cleaned

        snapshot: dict[str, Any] = {}
        movies = _clean("movies", ("watched", "plays", "minutes"))
        shows = _clean("shows", ("watched",))
        episodes = _clean("episodes", ("watched", "plays", "minutes"))

        if movies:
            snapshot["movies"] = movies
        if shows:
            snapshot["shows"] = shows
        if episodes:
            snapshot["episodes"] = episodes

        minute_sources = []
        movie_minutes = movies.get("minutes")
        episode_minutes = episodes.get("minutes")
        if isinstance(movie_minutes, int):
            minute_sources.append(movie_minutes)
        if isinstance(episode_minutes, int):
            minute_sources.append(episode_minutes)
        if minute_sources:
            snapshot["totalMinutes"] = sum(minute_sources)

        return snapshot

    async def _maybe_refresh_trakt_history_stats(
        self, state: ProfileState
    ) -> ProfileState:
        """Refresh cached Trakt counts if the data is stale."""

        if not state.trakt_access_token:
            return state

        refreshed_at = state.trakt_history_refreshed_at
        if refreshed_at and datetime.utcnow() - refreshed_at < timedelta(hours=12):
            return state

        movie_batch, show_batch = await asyncio.gather(
            self._trakt.fetch_history(
                "movies",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=1,
            ),
            self._trakt.fetch_history(
                "shows",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=1,
            ),
        )

        movie_total, show_total, snapshot = await self._gather_trakt_history_metadata(
            state,
            movie_batch=movie_batch,
            show_batch=show_batch,
        )

        await self._store_trakt_history_stats(
            state,
            history_limit=state.trakt_history_limit,
            movie_total=movie_total,
            show_total=show_total,
            snapshot=snapshot,
        )
        return state

    async def _ensure_catalog_scope(self, state: ProfileState) -> None:
        """Ensure stored catalog identifiers include the profile namespace."""

        async with self._session_factory() as session:
            stmt = select(CatalogRecord).where(CatalogRecord.profile_id == state.id)
            result = await session.execute(stmt)
            records = result.scalars().all()
            updated = False
            for record in records:
                scope, base_id = self._split_scoped_catalog_id(record.catalog_id)
                if scope == state.id:
                    continue
                new_id = self._scoped_catalog_id(state.id, base_id)
                record.catalog_id = new_id
                payload = record.payload if isinstance(record.payload, dict) else {}
                record.payload = {**payload, "id": new_id}
                updated = True
            if updated:
                await session.commit()

    def _clean_catalog_key(self, value: object) -> str:
        if isinstance(value, str):
            raw = value
        else:
            raw = str(value or "")
        slug = raw.strip().replace("_", "-").replace(" ", "-").lower()
        return "-".join(part for part in slug.split("-") if part)

    def _normalise_catalog_keys(
        self,
        keys: Sequence[str] | None,
        *,
        fallback: Sequence[str] | None = None,
    ) -> tuple[str, ...]:
        cleaned: list[str] = []
        if keys:
            for entry in keys:
                slug = self._clean_catalog_key(entry)
                if not slug:
                    continue
                if slug not in self._catalog_definition_map:
                    continue
                if slug not in cleaned:
                    cleaned.append(slug)
        if cleaned:
            return tuple(cleaned)

        base_fallback = fallback
        if base_fallback is None:
            base_fallback = getattr(self, "_default_catalog_keys", self._all_catalog_keys)

        fallback_cleaned: list[str] = []
        for entry in base_fallback:
            slug = self._clean_catalog_key(entry)
            if not slug:
                continue
            if slug not in self._catalog_definition_map:
                continue
            if slug not in fallback_cleaned:
                fallback_cleaned.append(slug)
        return tuple(fallback_cleaned)

    def _definitions_for_keys(
        self, keys: Sequence[str] | None
    ) -> tuple[StableCatalogDefinition, ...]:
        selection = self._normalise_catalog_keys(keys)
        return tuple(
            self._catalog_definition_map[key]
            for key in selection
            if key in self._catalog_definition_map
        )

    async def _enrich_catalogs_with_metadata(
        self,
        catalogs: dict[str, dict[str, Catalog]],
        metadata_addon_url: str | None,
    ) -> None:
        """Populate missing identifiers and artwork via a metadata add-on."""

        effective_url = metadata_addon_url or self._default_metadata_addon_url
        if not effective_url:
            return

        lookup_tasks: dict[
            tuple[str, str, int | None], asyncio.Task[MetadataMatch | None]
        ] = {}
        for catalog_map in catalogs.values():
            for catalog in catalog_map.values():
                for item in catalog.items:
                    if item.imdb_id and item.poster:
                        continue
                    title = (item.title or "").strip()
                    if not title:
                        continue
                    key = (item.type, title.casefold(), item.year)
                    if key in lookup_tasks:
                        continue

                    async def _lookup(
                        *,
                        title: str = title,
                        content_type: str = item.type,
                        year: int | None = item.year,
                    ) -> MetadataMatch | None:
                        return await self._metadata_client.lookup(
                            title,
                            content_type=content_type,
                            year=year,
                            base_url=effective_url,
                        )

                    lookup_tasks[key] = asyncio.create_task(_lookup())

        if not lookup_tasks:
            return

        results = await asyncio.gather(
            *lookup_tasks.values(), return_exceptions=True
        )
        matches: dict[tuple[str, str, int | None], MetadataMatch] = {}
        for key, result in zip(lookup_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.warning(
                    "Metadata add-on lookup failed for %s: %s", key, result
                )
                continue
            if result is None:
                continue
            matches[key] = result

        looked_up_keys = set(lookup_tasks.keys())
        for catalog_map in catalogs.values():
            for catalog_id, catalog in list(catalog_map.items()):
                updated_items: list[CatalogItem] = []
                for item in catalog.items:
                    title = (item.title or "").strip()
                    if not title:
                        if item.imdb_id and item.poster:
                            updated_items.append(item)
                        continue
                    key = (item.type, title.casefold(), item.year)
                    if key in looked_up_keys and key not in matches:
                        continue
                    match = matches.get(key)
                    if match is None:
                        updated_items.append(item)
                        continue

                    updates: dict[str, Any] = {}
                    if match.id and item.imdb_id != match.id:
                        updates["imdb_id"] = match.id
                    if match.year and item.year != match.year:
                        updates["year"] = match.year
                    if match.poster and str(item.poster or "") != match.poster:
                        updates["poster"] = match.poster
                    if match.background and str(item.background or "") != match.background:
                        updates["background"] = match.background

                    if updates:
                        # model_copy(update=...) bypasses validation for fields like HttpUrl
                        # which causes noisy Pydantic serialization warnings later. Re-validate
                        # the updated item so URL fields are parsed correctly.
                        try:
                            payload = item.model_dump(mode="json", exclude_none=True)
                            payload.update(updates)
                            updated_items.append(CatalogItem.model_validate(payload))
                        except ValidationError:
                            # If validation fails for the patched item, keep the original
                            updated_items.append(item)
                    else:
                        updated_items.append(item)

                catalog_map[catalog_id] = catalog.model_copy(
                    update={"items": updated_items}
                )

    async def _derive_profile_identity(
        self, config: ManifestConfig
    ) -> ProfileIdentity:
        slug = ""
        if config.profile_id:
            slug = slugify(config.profile_id)
            if slug and slug != "default":
                return ProfileIdentity(id=slug)

        trakt_identity = await self._profile_id_from_trakt(config)
        if trakt_identity is not None:
            return trakt_identity

        if slug == "default":
            return ProfileIdentity(id="default")

        if config.openrouter_key:
            digest = hashlib.sha256(config.openrouter_key.encode("utf-8")).hexdigest()[:12]
            return ProfileIdentity(id=f"user-{digest}")

        if config.openai_key:
            digest = hashlib.sha256(config.openai_key.encode("utf-8")).hexdigest()[:12]
            return ProfileIdentity(id=f"user-{digest}")

        return ProfileIdentity(id="default")

    async def _resolve_profile(self, config: ManifestConfig) -> ProfileContext:
        identity = await self._derive_profile_identity(config)
        profile_id = identity.id
        async with self._session_factory() as session:
            profile = await session.get(Profile, profile_id)
            created = False
            refresh_required = False
            now = datetime.utcnow()

            if profile is None:
                desired_mode = (
                    (config.generator_mode or self._settings.generator_mode)
                    if hasattr(self._settings, "generator_mode")
                    else (config.generator_mode or "openrouter")
                )
                desired_mode = desired_mode or "openrouter"
                openrouter_key = config.openrouter_key or self._settings.openrouter_api_key
                openai_key = config.openai_key or self._settings.openai_api_key
                if desired_mode == "openrouter" and not openrouter_key:
                    # No key provided for AI mode; fall back to local
                    desired_mode = "local"
                if desired_mode == "openai" and not openai_key:
                    desired_mode = "local"
                metadata_addon = (
                    str(config.metadata_addon_url)
                    if config.metadata_addon_url is not None
                    else self._default_metadata_addon_url
                )
                selected_keys = self._normalise_catalog_keys(config.catalog_keys)
                profile = Profile(
                    id=profile_id,
                    display_name=identity.display_name,
                    openrouter_api_key=openrouter_key or "",
                    openrouter_model=config.openrouter_model or self._settings.openrouter_model,
                    openai_api_key=openai_key or "",
                    openai_model=config.openai_model or self._settings.openai_model,
                    generator_mode=desired_mode,
                    catalog_count=len(selected_keys),
                    catalog_keys=list(selected_keys),
                    catalog_item_count=(
                        config.catalog_item_count
                        or self._settings.catalog_item_count
                    ),
                    generation_retry_limit=(
                        config.generation_retry_limit
                        if config.generation_retry_limit is not None
                        else self._settings.generation_retry_limit
                    ),
                    refresh_interval_seconds=config.refresh_interval or self._settings.refresh_interval_seconds,
                    response_cache_seconds=config.response_cache or self._settings.response_cache_seconds,
                    trakt_client_id=config.trakt_client_id or self._settings.trakt_client_id,
                    trakt_access_token=config.trakt_access_token or self._settings.trakt_access_token,
                    # Always default to full history; do not accept client-provided limits
                    trakt_history_limit=self._settings.trakt_history_limit,
                    metadata_addon_url=metadata_addon,
                    next_refresh_at=now,
                    last_refreshed_at=None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(profile)
                created = True
                refresh_required = True
            else:
                existing_keys_raw = getattr(profile, "catalog_keys", None)
                current_keys = self._normalise_catalog_keys(existing_keys_raw)
                if list(existing_keys_raw or []) != list(current_keys):
                    profile.catalog_keys = list(current_keys)
                    refresh_required = True
                if profile.catalog_count != len(current_keys):
                    profile.catalog_count = len(current_keys)
                if config.catalog_keys is not None:
                    incoming_keys = self._normalise_catalog_keys(config.catalog_keys)
                    if incoming_keys != current_keys:
                        profile.catalog_keys = list(incoming_keys)
                        profile.catalog_count = len(incoming_keys)
                        refresh_required = True
                        current_keys = incoming_keys
                if (
                    identity.display_name
                    and identity.display_name
                    != getattr(profile, "display_name", None)
                ):
                    profile.display_name = identity.display_name
                if config.openrouter_key and config.openrouter_key != profile.openrouter_api_key:
                    profile.openrouter_api_key = config.openrouter_key
                    refresh_required = True
                if config.openrouter_model and config.openrouter_model != profile.openrouter_model:
                    profile.openrouter_model = config.openrouter_model
                    refresh_required = True
                if config.openai_key and config.openai_key != getattr(profile, "openai_api_key", ""):
                    profile.openai_api_key = config.openai_key
                    refresh_required = True
                if config.openai_model and config.openai_model != getattr(profile, "openai_model", ""):
                    profile.openai_model = config.openai_model
                    refresh_required = True
                if config.generator_mode is not None:
                    new_mode = config.generator_mode
                    if new_mode != getattr(profile, "generator_mode", "openrouter"):
                        profile.generator_mode = new_mode
                        refresh_required = True
                if (
                    config.catalog_item_count
                    and config.catalog_item_count != getattr(
                        profile, "catalog_item_count", None
                    )
                ):
                    profile.catalog_item_count = config.catalog_item_count
                    refresh_required = True
                if (
                    config.generation_retry_limit is not None
                    and config.generation_retry_limit
                    != getattr(profile, "generation_retry_limit", None)
                ):
                    profile.generation_retry_limit = config.generation_retry_limit
                    refresh_required = True
                if config.refresh_interval and config.refresh_interval != profile.refresh_interval_seconds:
                    profile.refresh_interval_seconds = config.refresh_interval
                if config.response_cache and config.response_cache != profile.response_cache_seconds:
                    profile.response_cache_seconds = config.response_cache
                if config.trakt_client_id is not None and config.trakt_client_id != profile.trakt_client_id:
                    profile.trakt_client_id = config.trakt_client_id
                    refresh_required = True
                if config.trakt_access_token is not None and config.trakt_access_token != profile.trakt_access_token:
                    profile.trakt_access_token = config.trakt_access_token
                    refresh_required = True
                # Ignore any client-provided Trakt history limit; always use full history
                if config.metadata_addon_url is not None:
                    new_metadata_url = str(config.metadata_addon_url)
                    if new_metadata_url != getattr(profile, "metadata_addon_url", None):
                        profile.metadata_addon_url = new_metadata_url
                        refresh_required = True
                if profile.next_refresh_at is None:
                    profile.next_refresh_at = now
                profile.updated_at = now

            await session.commit()
            await session.refresh(profile)
            state = self._profile_to_state(profile)

        return ProfileContext(state=state, force_refresh=refresh_required or created)

    async def resolve_profile(self, config: ManifestConfig) -> ProfileContext:
        """Expose profile resolution without triggering catalog refreshes."""

        return await self._resolve_profile(config)

    async def get_profile_status(self, profile_id: str) -> ProfileStatus | None:
        state = await self._load_profile_state(profile_id)
        if state is None:
            return None
        state = await self._maybe_refresh_trakt_history_stats(state)
        needs_refresh, has_catalogs = await self._needs_refresh(state)
        refreshing = self._is_refreshing(profile_id)
        if refreshing:
            needs_refresh = True
        return ProfileStatus(
            state=state,
            has_catalogs=has_catalogs,
            needs_refresh=needs_refresh,
            refreshing=refreshing,
        )

    def _is_refreshing(self, profile_id: str) -> bool:
        task = self._refresh_jobs.get(profile_id)
        return bool(task and not task.done())

    def is_refreshing(self, profile_id: str) -> bool:
        """Return whether a refresh task is currently running for the profile."""

        return self._is_refreshing(profile_id)

    async def determine_profile_id(self, config: ManifestConfig) -> str:
        """Expose profile id derivation for external callers."""

        return await self._determine_profile_id(config)

    async def _determine_profile_id(self, config: ManifestConfig) -> str:
        identity = await self._derive_profile_identity(config)
        return identity.id

    async def _profile_id_from_trakt(
        self, config: ManifestConfig
    ) -> ProfileIdentity | None:
        access_token = config.trakt_access_token or self._settings.trakt_access_token
        if not access_token:
            return None

        client_id = config.trakt_client_id or self._settings.trakt_client_id
        try:
            profile = await self._trakt.fetch_user(
                client_id=client_id,
                access_token=access_token,
            )
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to fetch Trakt profile for ID derivation")
            profile = {}

        id_candidates: list[str] = []
        display_candidates: list[str] = []

        def _add_candidate(container: list[str], value: object) -> None:
            if not isinstance(value, str):
                return
            trimmed = value.strip()
            if not trimmed:
                return
            if trimmed in container:
                return
            container.append(trimmed)

        if isinstance(profile, Mapping):
            ids = profile.get("ids")
            if isinstance(ids, Mapping):
                _add_candidate(id_candidates, ids.get("slug"))
            _add_candidate(id_candidates, profile.get("username"))
            _add_candidate(display_candidates, profile.get("name"))
            _add_candidate(display_candidates, profile.get("username"))
            user_section = profile.get("user")
            if isinstance(user_section, Mapping):
                nested_ids = user_section.get("ids")
                if isinstance(nested_ids, Mapping):
                    _add_candidate(id_candidates, nested_ids.get("slug"))
                _add_candidate(id_candidates, user_section.get("username"))
                _add_candidate(display_candidates, user_section.get("name"))
                _add_candidate(display_candidates, user_section.get("username"))

        fallback_display = display_candidates[0] if display_candidates else None
        seen_slugs: set[str] = set()
        for raw_candidate in id_candidates:
            candidate = raw_candidate.strip()
            if not candidate:
                continue
            slug = slugify(candidate)
            if not slug or slug == "catalog":
                continue
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            display_name = None
            for display in display_candidates:
                if slugify(display) == slug:
                    display_name = display
                    break
            if display_name is None:
                display_name = fallback_display or candidate
            return ProfileIdentity(id=f"trakt-{slug}", display_name=display_name)

        digest = hashlib.sha256(access_token.encode("utf-8")).hexdigest()[:12]
        display_name = fallback_display
        return ProfileIdentity(id=f"trakt-{digest}", display_name=display_name)

    def profile_id_from_catalog_id(self, catalog_id: str) -> str | None:
        profile_id, _ = self._split_scoped_catalog_id(catalog_id)
        return profile_id

    async def _ensure_default_profile(self) -> None:
        async with self._session_factory() as session:
            profile = await session.get(Profile, "default")
            now = datetime.utcnow()
            if profile is None:
                # Decide default generator mode
                default_mode = getattr(self._settings, "generator_mode", "openrouter")
                use_openrouter = default_mode == "openrouter" and bool(self._settings.openrouter_api_key)
                use_openai = default_mode == "openai" and bool(self._settings.openai_api_key)
                profile = Profile(
                    id="default",
                    openrouter_api_key=(self._settings.openrouter_api_key or "") if use_openrouter else "",
                    openrouter_model=self._settings.openrouter_model,
                    openai_api_key=(self._settings.openai_api_key or "") if use_openai else "",
                    openai_model=self._settings.openai_model,
                    generator_mode=("openrouter" if use_openrouter else ("openai" if use_openai else "local")),
                    trakt_client_id=self._settings.trakt_client_id,
                    trakt_access_token=self._settings.trakt_access_token,
                    trakt_history_limit=self._settings.trakt_history_limit,
                    catalog_count=len(self._default_catalog_keys),
                    catalog_keys=list(self._default_catalog_keys),
                    catalog_item_count=self._settings.catalog_item_count,
                    generation_retry_limit=self._settings.generation_retry_limit,
                    refresh_interval_seconds=self._settings.refresh_interval_seconds,
                    response_cache_seconds=self._settings.response_cache_seconds,
                    metadata_addon_url=self._default_metadata_addon_url,
                    next_refresh_at=now,
                    last_refreshed_at=None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(profile)
            else:
                updated = False
                stored_keys_raw = getattr(profile, "catalog_keys", None)
                normalised_keys = self._normalise_catalog_keys(stored_keys_raw)
                if list(stored_keys_raw or []) != list(normalised_keys):
                    profile.catalog_keys = list(normalised_keys)
                    updated = True
                desired_keys = self._default_catalog_keys
                if list(normalised_keys) != list(desired_keys):
                    profile.catalog_keys = list(desired_keys)
                    normalised_keys = desired_keys
                    updated = True
                if profile.catalog_count != len(normalised_keys):
                    profile.catalog_count = len(normalised_keys)
                    updated = True
                if not profile.openrouter_api_key and self._settings.openrouter_api_key:
                    profile.openrouter_api_key = self._settings.openrouter_api_key
                    updated = True
                if not profile.openrouter_model:
                    profile.openrouter_model = self._settings.openrouter_model
                    updated = True
                if not profile.openai_api_key and self._settings.openai_api_key:
                    profile.openai_api_key = self._settings.openai_api_key
                    updated = True
                if not profile.openai_model:
                    profile.openai_model = self._settings.openai_model
                    updated = True
                # Keep generator_mode in sync with settings if missing
                if not getattr(profile, "generator_mode", None):
                    profile.generator_mode = getattr(self._settings, "generator_mode", "openrouter")
                    # If AI selected but no key, force local
                    if profile.generator_mode == "openrouter" and not self._settings.openrouter_api_key:
                        profile.generator_mode = "local"
                    if profile.generator_mode == "openai" and not self._settings.openai_api_key:
                        profile.generator_mode = "local"
                    updated = True
                if getattr(profile, "catalog_item_count", None) != self._settings.catalog_item_count:
                    profile.catalog_item_count = self._settings.catalog_item_count
                    updated = True
                if (
                    getattr(profile, "generation_retry_limit", None)
                    != self._settings.generation_retry_limit
                ):
                    profile.generation_retry_limit = self._settings.generation_retry_limit
                    updated = True
                if profile.refresh_interval_seconds != self._settings.refresh_interval_seconds:
                    profile.refresh_interval_seconds = self._settings.refresh_interval_seconds
                    updated = True
                if profile.response_cache_seconds != self._settings.response_cache_seconds:
                    profile.response_cache_seconds = self._settings.response_cache_seconds
                    updated = True
                if getattr(profile, "trakt_history_limit", None) != self._settings.trakt_history_limit:
                    profile.trakt_history_limit = self._settings.trakt_history_limit
                    updated = True
                if getattr(profile, "metadata_addon_url", None) != self._default_metadata_addon_url:
                    profile.metadata_addon_url = self._default_metadata_addon_url
                    updated = True
                if profile.trakt_client_id != self._settings.trakt_client_id:
                    profile.trakt_client_id = self._settings.trakt_client_id
                    updated = True
                if profile.trakt_access_token != self._settings.trakt_access_token:
                    profile.trakt_access_token = self._settings.trakt_access_token
                    updated = True
                if profile.next_refresh_at is None:
                    profile.next_refresh_at = now
                    updated = True
                if updated:
                    profile.updated_at = now
            await session.commit()

    def _bundle_to_dict(self, bundle: CatalogBundle) -> dict[str, dict[str, Catalog]]:
        return {
            "movie": {catalog.id: catalog for catalog in bundle.movie_catalogs},
            "series": {catalog.id: catalog for catalog in bundle.series_catalogs},
        }

    def _build_watched_index(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
    ) -> dict[str, WatchedMediaIndex]:
        return {
            "movie": self._index_history_items(movie_history, key="movie"),
            "series": self._index_history_items(show_history, key="show"),
        }

    def _serialise_watched_index(
        self, index: dict[str, WatchedMediaIndex]
    ) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        for content_type, data in index.items():
            if not data.fingerprints and not data.recent_titles:
                continue
            payload[content_type] = {
                "fingerprints": sorted(data.fingerprints),
                "recent_titles": data.recent_titles[:24],
            }
        return payload

    async def _build_served_index(self, profile_id: str) -> dict[str, set[str]]:
        """Collect fingerprints of items most recently served to a profile.

        Used to avoid returning the exact same titles on subsequent refreshes, while
        still allowing backfill if pools are sparse.
        """
        served: dict[str, set[str]] = {"movie": set(), "series": set()}
        try:
            catalogs = await self._load_catalogs(profile_id)
        except Exception:
            catalogs = []
        for catalog in catalogs:
            fp_set = served.setdefault(catalog.type, set())
            for item in catalog.items:
                try:
                    fp_set |= self._catalog_item_fingerprints(item)
                except Exception:
                    continue
        return served

    def _index_history_items(
        self,
        history: list[dict[str, Any]],
        *,
        key: str,
    ) -> WatchedMediaIndex:
        prefix = "movie" if key == "movie" else "series"
        fingerprints: set[str] = set()
        titles: list[str] = []

        for entry in history:
            media = entry.get(key) or {}
            if not isinstance(media, dict):
                continue
            ids = media.get("ids") or {}
            if not isinstance(ids, dict):
                ids = {}

            imdb = ids.get("imdb")
            if isinstance(imdb, str) and imdb.strip():
                fingerprints.add(f"{prefix}:imdb:{imdb.strip().lower()}")

            trakt = ids.get("trakt")
            if isinstance(trakt, int):
                fingerprints.add(f"{prefix}:trakt:{trakt}")

            tmdb = ids.get("tmdb")
            if isinstance(tmdb, int):
                fingerprints.add(f"{prefix}:tmdb:{tmdb}")

            slug_id = ids.get("slug")
            if isinstance(slug_id, str) and slug_id:
                slug = slugify(slug_id)
                if slug:
                    fingerprints.add(f"{prefix}:slug:{slug}")

            title = media.get("title")
            if isinstance(title, str) and title.strip():
                normalized = title.strip()
                display_year = media.get("year")
                if isinstance(display_year, int):
                    display = f"{normalized} ({display_year})"
                else:
                    display = normalized
                if display not in titles:
                    titles.append(display)
                lowered = normalized.casefold()
                if lowered:
                    fingerprints.add(f"{prefix}:title:{lowered}")
                    if isinstance(display_year, int):
                        fingerprints.add(
                            f"{prefix}:title:{lowered}:{display_year}"
                        )

        return WatchedMediaIndex(fingerprints=fingerprints, recent_titles=titles[:40])

    def _prune_watched_items(
        self,
        catalogs: dict[str, dict[str, Catalog]],
        watched_index: dict[str, WatchedMediaIndex],
    ) -> None:
        """Strip catalog entries that match the viewer's completed history."""

        def _matches(item: CatalogItem, excluded: set[str]) -> bool:
            if not excluded:
                return False
            item_fingerprints = self._catalog_item_fingerprints(item)
            return any(fp in excluded for fp in item_fingerprints)

        for content_type, catalog_map in catalogs.items():
            index = watched_index.get(content_type)
            if index is None or not index.fingerprints:
                continue
            excluded = index.fingerprints
            for catalog_id, catalog in list(catalog_map.items()):
                filtered = [
                    item for item in catalog.items if not _matches(item, excluded)
                ]
                if len(filtered) != len(catalog.items):
                    catalog_map[catalog_id] = catalog.model_copy(
                        update={"items": filtered}
                    )

    def _catalog_item_fingerprints(self, item: CatalogItem) -> set[str]:
        """Build fingerprints mirroring the watched history index."""

        fingerprints: set[str] = set()
        prefix = item.type
        if item.imdb_id:
            fingerprints.add(f"{prefix}:imdb:{item.imdb_id.lower()}")
        if item.trakt_id is not None:
            fingerprints.add(f"{prefix}:trakt:{item.trakt_id}")
        if item.tmdb_id is not None:
            fingerprints.add(f"{prefix}:tmdb:{item.tmdb_id}")
        title = (item.title or "").strip().casefold()
        if title:
            fingerprints.add(f"{prefix}:title:{title}")
            if item.year:
                fingerprints.add(f"{prefix}:title:{title}:{item.year}")
            slug_title = slugify(title)
            if slug_title:
                fingerprints.add(f"{prefix}:slug:{slug_title}")
                if item.year:
                    fingerprints.add(f"{prefix}:slug:{slug_title}:{item.year}")
        return fingerprints

    def _build_summary(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
        *,
        state: ProfileState,
        catalog_item_count: int,
    ) -> dict[str, Any]:
        movie_profile = TraktClient.summarize_history(movie_history, key="movie")
        series_profile = TraktClient.summarize_history(show_history, key="show")

        movie_profile["taste_summary"] = self._describe_taste_profile(
            "movies", movie_profile, movie_history, key="movie"
        )
        series_profile["taste_summary"] = self._describe_taste_profile(
            "series", series_profile, show_history, key="show"
        )
        movie_profile["recent_highlights"] = self._recent_title_summary(
            movie_profile.get("top_titles")
        )
        series_profile["recent_highlights"] = self._recent_title_summary(
            series_profile.get("top_titles")
        )

        summary: dict[str, Any] = {
            "generated_at": datetime.utcnow().isoformat(),
            "catalog_item_count": catalog_item_count,
            "profile": {
                "movies": movie_profile,
                "series": series_profile,
            },
            "lifetime_summary": self._describe_lifetime_stats(state),
        }

        if isinstance(state.trakt_history_snapshot, Mapping):
            summary["stats"] = state.trakt_history_snapshot

        return summary

    def _describe_taste_profile(
        self,
        label: str,
        profile: Mapping[str, Any],
        history: list[dict[str, Any]],
        *,
        key: str,
    ) -> str:
        total = profile.get("total")
        segments: list[str] = []
        if isinstance(total, int) and total > 0:
            segments.append(f"{total} logged {label}")

        genre_values = self._format_counter_items(
            profile.get("top_genres"), unit="plays"
        )
        if genre_values:
            segments.append(f"leans into {self._join_list(genre_values)}")

        language_values = self._format_counter_items(
            profile.get("top_languages"), unit="entries"
        )
        if language_values:
            segments.append(
                f"comfortable with languages like {self._join_list(language_values)}"
            )

        country_values = self._format_counter_items(
            profile.get("top_countries"), unit="productions"
        )
        if country_values:
            segments.append(
                f"often chooses releases from {self._join_list(country_values)}"
            )

        release_summary = self._describe_release_years(history, key=key)
        if release_summary:
            segments.append(release_summary)

        runtime = profile.get("average_runtime")
        if isinstance(runtime, int) and runtime > 0:
            segments.append(f"prefers runtimes around {runtime} min")

        last_watch = profile.get("last_watched_at")
        if isinstance(last_watch, str):
            segments.append(f"latest check-in {last_watch[:10]}")

        if not segments:
            return "No strong signals captured yet."
        return "; ".join(segments)

    @staticmethod
    def _format_counter_items(
        values: Any,
        *,
        limit: int = 4,
        unit: str | None = None,
    ) -> list[str]:
        formatted: list[str] = []
        if isinstance(values, Sequence):
            for entry in values:
                if not isinstance(entry, Sequence) or len(entry) < 2:
                    continue
                label, count = entry[0], entry[1]
                if not isinstance(label, str) or not isinstance(count, int):
                    continue
                if count <= 0:
                    continue
                descriptor = f"{count}" if unit is None else f"{count} {unit}"
                formatted.append(f"{label} ({descriptor})")
                if len(formatted) >= limit:
                    break
        return formatted

    def _describe_release_years(
        self, history: list[dict[str, Any]], *, key: str
    ) -> str | None:
        years: list[int] = []
        for entry in history:
            media = entry.get(key)
            if not isinstance(media, Mapping):
                continue
            year = media.get("year")
            if isinstance(year, int) and 1900 <= year <= 2100:
                years.append(year)
        if not years:
            return None
        decade_counts = Counter((year // 10) * 10 for year in years)
        if not decade_counts:
            return None
        top_decades = [
            f"{decade}s ({count})" for decade, count in decade_counts.most_common(2)
        ]
        if not top_decades:
            return None
        return f"gravitates toward releases from {self._join_list(top_decades)}"

    def _recent_title_summary(self, titles: Any, *, limit: int = 8) -> str:
        if not isinstance(titles, Sequence):
            return "No recent standouts captured."
        filtered = [title for title in titles if isinstance(title, str) and title]
        if not filtered:
            return "No recent standouts captured."
        trimmed = list(filtered[:limit])
        if len(filtered) > limit:
            trimmed[-1] = f"{trimmed[-1]}…"
        return self._join_list(trimmed)

    def _describe_lifetime_stats(self, state: ProfileState) -> str:
        parts: list[str] = []
        movie_total = state.trakt_movie_history_count
        if isinstance(movie_total, int) and movie_total > 0:
            parts.append(f"{movie_total:,} movies tracked overall")
        show_total = state.trakt_show_history_count
        if isinstance(show_total, int) and show_total > 0:
            parts.append(f"{show_total:,} series tracked overall")

        snapshot = state.trakt_history_snapshot
        if isinstance(snapshot, Mapping):
            episodes = snapshot.get("episodes")
            if isinstance(episodes, Mapping):
                episode_watched = episodes.get("watched")
                if isinstance(episode_watched, int) and episode_watched > 0:
                    parts.append(f"{episode_watched:,} episodes logged")
            total_minutes = snapshot.get("totalMinutes")
            if isinstance(total_minutes, int) and total_minutes > 0:
                hours = total_minutes // 60
                if hours > 0:
                    parts.append(f"{hours:,} hours watched")

        history_limit = state.trakt_history_limit
        if isinstance(history_limit, int):
            if history_limit > 0:
                parts.append(
                    f"current refresh samples the last {history_limit} plays per type"
                )
            else:
                parts.append("current refresh scans your entire play history")

        if not parts:
            return "Lifetime stats unavailable; lean on taste summaries above."
        return "; ".join(parts)

    @staticmethod
    def _join_list(items: Sequence[str]) -> str:
        filtered = [item for item in items if isinstance(item, str) and item]
        if not filtered:
            return ""
        if len(filtered) == 1:
            return filtered[0]
        if len(filtered) == 2:
            return " and ".join(filtered)
        return ", ".join(filtered[:-1]) + f", and {filtered[-1]}"

    def _build_fallback_catalogs(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
        *,
        seed: str,
        item_limit: int,
    ) -> dict[str, dict[str, Catalog]]:
        catalogs: dict[str, dict[str, Catalog]] = {"movie": {}, "series": {}}

        if movie_history:
            catalog = self._history_catalog(
                movie_history,
                content_type="movie",
                title="AI Offline: Movies You Loved",
                seed=seed,
                item_limit=item_limit,
            )
            catalogs["movie"][catalog.id] = catalog

        if show_history:
            catalog = self._history_catalog(
                show_history,
                content_type="series",
                title="AI Offline: Series Marathon",
                seed=seed,
                item_limit=item_limit,
            )
            catalogs["series"][catalog.id] = catalog

        if not catalogs["movie"] and not catalogs["series"]:
            now = datetime.utcnow()
            stub_catalog = Catalog(
                id=f"aiopicks-movie-stub-{seed}",
                type="movie",
                title="Connect Trakt to unlock personalized picks",
                description="We need your Trakt API credentials to fetch history before calling the AI.",
                seed=seed,
                items=[],
                generated_at=now,
            )
            catalogs["movie"][stub_catalog.id] = stub_catalog
        return catalogs

    def _history_catalog(
        self,
        history: list[dict[str, Any]],
        *,
        content_type: str,
        title: str,
        seed: str,
        item_limit: int,
    ) -> Catalog:
        key = "movie" if content_type == "movie" else "show"
        items: list[CatalogItem] = []
        for index, entry in enumerate(history[:item_limit]):
            media = entry.get(key) or {}
            if not isinstance(media, dict):
                continue
            ids = media.get("ids") or {}
            data: dict[str, Any] = {
                "name": media.get("title") or f"Unknown {content_type.title()}",
                "type": content_type,
                "description": media.get("overview") or entry.get("summary"),
                "year": media.get("year"),
                "imdb_id": ids.get("imdb"),
                "trakt_id": ids.get("trakt"),
                "tmdb_id": ids.get("tmdb"),
                "runtime_minutes": media.get("runtime"),
                "genres": [g for g in (media.get("genres") or []) if isinstance(g, str)],
            }
            poster = self._extract_image(media)
            if poster:
                data["poster"] = poster
            background = self._extract_image(media, key="background")
            if background:
                data["background"] = background
            try:
                item = CatalogItem.model_validate(data)
            except ValidationError:
                continue
            items.append(item)

        catalog_id = slugify(title) or f"history-{content_type}-{seed}"
        return Catalog(
            id=f"aiopicks-{content_type}-{catalog_id}",
            type=content_type,
            title=title,
            description=f"Curated from your recent {content_type} history.",
            seed=seed,
            items=items,
            generated_at=datetime.utcnow(),
        )

    def _unique_media_from_history(
        self, history: list[dict[str, Any]], *, key: str
    ) -> list[dict[str, Any]]:
        """Collapse history entries into unique media records preserving basic attributes."""
        seen: set[tuple[str, int | None]] = set()
        unique: list[dict[str, Any]] = []
        for entry in history:
            media = entry.get(key) or {}
            if not isinstance(media, dict):
                continue
            title = media.get("title")
            if not isinstance(title, str) or not title.strip():
                continue
            year = media.get("year") if isinstance(media.get("year"), int) else None
            fingerprint = (title.strip().casefold(), year)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            ids = media.get("ids") or {}
            record = {
                "title": title.strip(),
                "year": year,
                "ids": ids if isinstance(ids, dict) else {},
                "genres": [g for g in (media.get("genres") or []) if isinstance(g, str)],
                "language": media.get("language") if isinstance(media.get("language"), str) else None,
                "runtime": media.get("runtime") if isinstance(media.get("runtime"), int) else None,
                "images": media.get("images") if isinstance(media.get("images"), dict) else {},
                "overview": media.get("overview") if isinstance(media.get("overview"), str) else None,
            }
            unique.append(record)
        return unique

    def _catalog_from_media(
        self,
        media_list: list[dict[str, Any]],
        *,
        content_type: str,
        title: str,
        description: str | None,
        seed: str,
        item_limit: int,
    ) -> Catalog:
        items: list[CatalogItem] = []
        for media in media_list[: item_limit]:
            ids = media.get("ids") or {}
            data: dict[str, Any] = {
                "name": media.get("title") or f"Unknown {content_type.title()}",
                "type": content_type,
                "description": media.get("overview"),
                "year": media.get("year"),
                "imdb_id": ids.get("imdb"),
                "trakt_id": ids.get("trakt"),
                "tmdb_id": ids.get("tmdb"),
                "runtime_minutes": media.get("runtime"),
                "genres": [g for g in (media.get("genres") or []) if isinstance(g, str)],
            }
            images = media.get("images") or {}
            if isinstance(images, dict):
                poster = images.get("poster") or images.get("poster_full") or images.get("poster_url")
                if isinstance(poster, str) and poster.startswith("http"):
                    data["poster"] = poster
                background = images.get("background") or images.get("background_full") or images.get("background_url")
                if isinstance(background, str) and background.startswith("http"):
                    data["background"] = background
            try:
                items.append(CatalogItem.model_validate(data))
            except ValidationError:
                continue
        return Catalog(
            id=f"aiopicks-{content_type}-{slugify(title) or f'local-{seed}'}",
            type=content_type,
            title=title,
            description=description or f"Local picks curated from your {content_type} history.",
            seed=seed,
            items=items,
            generated_at=datetime.utcnow(),
        )

    async def _generate_local_catalogs(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
        *,
        definitions: Sequence[StableCatalogDefinition],
        seed: str,
        item_limit: int,
        watched: dict[str, WatchedMediaIndex],
        served: dict[str, set[str]] | None = None,
        trakt_client_id: str | None = None,
        trakt_access_token: str | None = None,
    ) -> dict[str, dict[str, Catalog]]:
        """Heuristic, offline recommender: themed remixes of a user's history.

        This does not fetch new titles; it smartly resurfaces relevant items per lane.
        """
        movie_media = self._unique_media_from_history(movie_history, key="movie")
        show_media = self._unique_media_from_history(show_history, key="show")

        def genre_counter(media_list: list[dict[str, Any]]) -> Counter:
            c: Counter[str] = Counter()
            for m in media_list:
                c.update([g.lower() for g in (m.get("genres") or []) if isinstance(g, str)])
            return c

        movie_genres = genre_counter(movie_media)
        show_genres = genre_counter(show_media)

        def top_genres(counter: Counter[str], n: int) -> list[str]:
            return [g for g, _ in counter.most_common(n) if g]

        def apply_filters(media_list: list[dict[str, Any]], *,
                          include_genres: set[str] | None = None,
                          exclude_genres: set[str] | None = None,
                          language_pred=None,
                          runtime_pred=None) -> list[dict[str, Any]]:
            results: list[dict[str, Any]] = []
            for m in media_list:
                genres = {g.lower() for g in (m.get("genres") or []) if isinstance(g, str)}
                if include_genres and not (genres & include_genres):
                    continue
                if exclude_genres and (genres & exclude_genres):
                    continue
                if language_pred:
                    lang = m.get("language")
                    try:
                        if not language_pred(lang):
                            continue
                    except Exception:
                        pass
                if runtime_pred:
                    rt = m.get("runtime")
                    try:
                        if not runtime_pred(rt):
                            continue
                    except Exception:
                        pass
                results.append(m)
            return results

        catalogs: dict[str, dict[str, Catalog]] = {"movie": {}, "series": {}}
        # Cache candidates per content type and source
        candidate_cache: dict[str, dict[str, list[dict[str, Any]]]] = {
            # favor recommendation/related over generic pools
            "movie": {"recommended": [], "related": [], "popular": [], "trending": []},
            "series": {"recommended": [], "related": [], "popular": [], "trending": []},
        }
        # Track session-wide uniqueness across all lanes per content type
        session_seen: dict[str, set[tuple[str, int | None]]] = {"movie": set(), "series": set()}

        # Build lightweight preference weights from the user's own history to re-rank picks
        def _normalize(counter: Counter[str]) -> dict[str, float]:
            total = sum(v for v in counter.values() if isinstance(v, int) and v > 0)
            if total <= 0:
                return {}
            return {k: float(v) / float(total) for k, v in counter.items() if v > 0}

        # Genre and language affinity per content type
        genre_weights_map = {
            "movie": _normalize(movie_genres),
            "series": _normalize(show_genres),
        }
        language_counts_movie: Counter[str] = Counter(
            [
                (m.get("language") or "").strip().lower()
                for m in movie_media
                if isinstance(m.get("language"), str) and m.get("language").strip()
            ]
        )
        language_counts_series: Counter[str] = Counter(
            [
                (m.get("language") or "").strip().lower()
                for m in show_media
                if isinstance(m.get("language"), str) and m.get("language").strip()
            ]
        )
        language_weights_map = {
            "movie": _normalize(language_counts_movie),
            "series": _normalize(language_counts_series),
        }

        # Simple year recency helper (scaled 0..1, favoring recent releases)
        current_year = datetime.utcnow().year
        def _recency_score(year: int | None) -> float:
            if not isinstance(year, int) or year < 1900 or year > current_year + 1:
                return 0.0
            # Favor last ~20 years, gently decay beyond
            window = 20
            delta = max(0, min(window, year - (current_year - window)))
            return float(delta) / float(window)

        # Source priors to prefer Trakt trending/popular a bit in tie-breaks
        _SRC_PRIOR = {"recommended": 0.35, "related": 0.30, "trending": 0.05, "popular": 0.02}

        # Compute relevance score for a candidate using only fields we already fetch
        def _relevance_score(m: dict[str, Any], *, content_type: str) -> float:
            score = 0.0
            # Genre affinity
            gw = genre_weights_map.get(content_type) or {}
            for g in (m.get("genres") or []):
                gkey = str(g).strip().lower()
                if not gkey:
                    continue
                score += gw.get(gkey, 0.0) * 1.0
            # Language preference
            lw = language_weights_map.get(content_type) or {}
            lang = (m.get("language") or "").strip().lower()
            if lang:
                score += lw.get(lang, 0.0) * 0.6
            # Recency bump
            y = m.get("year") if isinstance(m.get("year"), int) else None
            score += _recency_score(y) * 0.4
            # Trakt source prior if present (annotated below)
            src = str(m.get("aiop_src") or "").strip().lower()
            if src in _SRC_PRIOR:
                score += _SRC_PRIOR[src]
            return score

        def _seed_int(*parts: str) -> int:
            joined = "|".join(parts)
            return int(hashlib.sha256(joined.encode("utf-8")).hexdigest(), 16) % (2**32)

        def _rng_for(*parts: str) -> random.Random:
            return random.Random(_seed_int(*parts))

        def _item_keyparts(m: dict[str, Any], *, content_type: str) -> tuple[str, int | None, str, int | None]:
            title = (m.get("title") or "").strip().casefold()
            year = m.get("year") if isinstance(m.get("year"), int) else None
            ids = m.get("ids") or {}
            imdb = (ids.get("imdb") or "").strip().lower() if isinstance(ids.get("imdb"), str) else ""
            trakt_id = ids.get("trakt") if isinstance(ids.get("trakt"), int) else None
            return (title, year, imdb, trakt_id)

        def _noise_for_item(m: dict[str, Any], *, content_type: str, lane_key: str, width: float = 0.08) -> float:
            title, year, imdb, trakt_id = _item_keyparts(m, content_type=content_type)
            core = f"{content_type}:{lane_key}:{title}:{year}:{imdb}:{trakt_id or ''}:{seed}"
            h = hashlib.sha256(core.encode("utf-8")).hexdigest()
            base = int(h[:8], 16) / float(0xFFFFFFFF)
            return (base - 0.5) * 2.0 * width

        # Helper: lane-specific extra weights
        def _lane_bonus(m: dict[str, Any], *, lane_key: str, content_type: str) -> float:
            k = lane_key
            bonus = 0.0
            # Prefer well-rated titles for critics lane
            if k == "critics-love-youll-love":
                try:
                    rating = float(m.get("rating") or 0.0)
                except Exception:
                    rating = 0.0
                # Trakt ratings are 0..10 typically
                bonus += max(0.0, min(1.0, rating / 10.0)) * 0.6
            # Prefer long runtimes for visually stunning movies
            if k == "visually-stunning-for-you" and content_type == "movie":
                rt = m.get("runtime") if isinstance(m.get("runtime"), int) else m.get("runtime_minutes")
                if isinstance(rt, int) and rt >= 110:
                    bonus += 0.25
            # Background watching prefers short episodes; avoid forcing specific genres
            if k == "background-watching" and content_type == "series":
                rt = m.get("runtime") if isinstance(m.get("runtime"), int) else None
                if isinstance(rt, int) and rt <= 30:
                    bonus += 0.2
                genres = {str(g).strip().lower() for g in (m.get("genres") or []) if g}
                # Light, easy-to-follow styles still get a small nudge
                if {"comedy", "animation", "reality", "talk-show", "game-show"} & genres:
                    bonus += 0.12
            # You missed these: de-emphasize recent
            if k == "you-missed-these":
                y = m.get("year") if isinstance(m.get("year"), int) else None
                bonus -= _recency_score(y) * 0.6
            # Independent films lane: favor titles explicitly tagged as indie/independent
            if k == "independent-films" and content_type == "movie":
                genres = {str(g).strip().lower() for g in (m.get("genres") or []) if g}
                if {"indie", "independent"} & genres:
                    bonus += 0.25
            # Favorite actors lane: prioritize picks sourced from actor filmographies
            if k == "starring-your-favorite-actors" and content_type == "movie":
                src = str(m.get("aiop_src") or "").strip().lower()
                if src == "people":
                    bonus += 0.30
            return bonus

        for definition in definitions:
            content_type = definition.content_type
            media_list = movie_media if content_type == "movie" else show_media
            if not media_list:
                continue
            tg = top_genres(movie_genres if content_type == "movie" else show_genres, 6)
            top1, top2, top3 = (tg[0] if len(tg) > 0 else None, tg[1] if len(tg) > 1 else None, tg[2] if len(tg) > 2 else None)
            include: set[str] | None = None
            exclude: set[str] | None = None
            lang_pred = None
            rt_pred = None
            # Lane-specific selection behavior flags
            keep_language_strict = False
            keep_genre_strict = False
            prefer_related_order = False
            lane_local_pools: dict[str, list[dict[str, Any]]] | None = None

            key = definition.key
            k = key.lower()
            if k in {"movies-for-you", "series-for-you"}:
                include = set([g for g in tg[:3] if g]) or None
            elif k == "your-comfort-zone":
                include = set([g for g in tg[:2] if g]) or None
            elif k == "expand-your-horizons":
                include = set([g for g in tg[2:6] if g]) or None
            elif k == "your-next-obsession":
                include = set([g for g in tg[:3] if g]) or None
                rt_pred = lambda r: isinstance(r, int) and r >= 40
            elif k == "you-missed-these":
                # Lean towards older samples in history
                media_list = list(reversed(media_list))
                include = set([g for g in tg[:4] if g]) or None
            elif k == "critics-love-youll-love":
                include = {g for g in ["drama", "biography", "history"] if g in tg}
            elif k == "international-picks":
                lang_pred = lambda lang: isinstance(lang, str) and lang.lower() != "en"
                keep_language_strict = True
                # Build lane-local pools with non-English bias to avoid English-heavy global cache
                # Derive user's non-English top languages; fallback to common non-English set
                lang_counts: Counter[str] = Counter([
                    (m.get("language") or "").strip().lower()
                    for m in media_list
                    if isinstance(m.get("language"), str) and m.get("language").strip() and (m.get("language").strip().lower() != "en")
                ])
                top_non_en = [l for l, _ in lang_counts.most_common(5) if l and l != "en"]
                if not top_non_en:
                    top_non_en = ["es", "fr", "de", "ja", "ko", "zh", "it", "pt", "ru", "hi"]
                lane_local_pools = {src: [] for src in ("recommended", "related", "trending", "popular")}
                # Personalized recs (no language filter server-side)
                try:
                    # Broaden personalized pool for large lane sizes (hard cap 100 by API)
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=min(max(item_limit * 5, 100), 100),
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                for m in recs:
                    try:
                        m["aiop_src"] = "recommended"
                    except Exception:
                        pass
                rng_recs = _rng_for(seed, content_type, "international-recommended")
                shuffled_recs = recs[:]
                rng_recs.shuffle(shuffled_recs)
                lane_local_pools["recommended"] = shuffled_recs
                # Related pool
                seeds = []
                for mm in media_list:
                    ids = mm.get("ids") or {}
                    tid = ids.get("trakt")
                    if isinstance(tid, int):
                        seeds.append(tid)
                rng_seeds = _rng_for(seed, content_type, "international-related-seeds")
                rng_seeds.shuffle(seeds)
                # Use more seeds for related pools when requesting many items
                max_seeds = 8 if item_limit <= 30 else (12 if item_limit <= 60 else 20)
                seeds = seeds[: max_seeds]
                related_collected: list[dict[str, Any]] = []
                for sid in seeds:
                    try:
                        # Use paginated related for large requests to expand pool per seed
                        if item_limit > 50:
                            rel = await self._trakt.fetch_related_paginated(
                                content_type,
                                trakt_id=sid,
                                total_limit=min(max(item_limit // 2, 60), 200),
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                        else:
                            rel = await self._trakt.fetch_related(
                                content_type,
                                trakt_id=sid,
                                limit=min(max(item_limit // 2, 20), 80),
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                    except Exception:
                        rel = []
                    for m in rel:
                        try:
                            m["aiop_src"] = "related"
                        except Exception:
                            pass
                    related_collected.extend(rel)
                seen_pairs_rel: set[tuple[str, int | None]] = set()
                deduped_rel: list[dict[str, Any]] = []
                for m in related_collected:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_pairs_rel:
                        continue
                    seen_pairs_rel.add(kp)
                    deduped_rel.append(m)
                rng_rel = _rng_for(seed, content_type, "international-related")
                shuffled_rel = deduped_rel[:]
                rng_rel.shuffle(shuffled_rel)
                lane_local_pools["related"] = shuffled_rel
                # Trending/popular with non-English language filter
                for src in ("trending", "popular"):
                    try:
                        if item_limit > 50:
                            listing = await self._trakt.fetch_listing_paginated(
                                content_type,
                                list_type=src,
                                total_limit=min(max(item_limit * 6, 200), 800),
                                languages=top_non_en,
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                        else:
                            listing = await self._trakt.fetch_listing(
                                content_type,
                                list_type=src,
                                limit=max(item_limit * 2, 30),
                                languages=top_non_en,
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                    except Exception:
                        listing = []
                    for m in listing:
                        try:
                            m["aiop_src"] = src
                        except Exception:
                            pass
                    rng_gp = _rng_for(seed, content_type, f"international-{src}")
                    shuffled_gp = listing[:]
                    rng_gp.shuffle(shuffled_gp)
                    lane_local_pools[src] = shuffled_gp
            elif k == "your-guilty-pleasures-extended":
                include = {g for g in ["action", "horror", "romance", "comedy", "thriller"] if g in tg}
            elif k == "starring-your-favorite-actors":
                # Do not constrain by user's top genres for actor-centric lane; rely on scoring instead
                include = None
                # Build lane from top actors' filmographies and add robust fallbacks
                lane_local_pools = {src: [] for src in ("people", "recommended", "related", "trending", "popular")}
                # Collect cast from a broader set of seeds to enrich actor pool
                seeds: list[int] = []
                for mm in media_list[:80]:
                    ids = mm.get("ids") or {}
                    tid = ids.get("trakt")
                    if isinstance(tid, int):
                        seeds.append(tid)
                actor_counts: Counter[int] = Counter()
                for sid in seeds[:24]:
                    try:
                        ppl = await self._trakt.fetch_people(
                            content_type,
                            trakt_id=sid,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        ppl = {}
                    cast = ppl.get("cast") if isinstance(ppl, dict) else None
                    if isinstance(cast, list):
                        for role in cast[:25]:
                            person = role.get("person") if isinstance(role, dict) else None
                            if isinstance(person, dict):
                                pid = person.get("ids", {}).get("trakt")
                                if isinstance(pid, int):
                                    actor_counts.update([pid])
                top_actor_ids = [pid for pid, _ in actor_counts.most_common(15)]
                filmography: list[dict[str, Any]] = []
                for pid in top_actor_ids:
                    try:
                        credits = await self._trakt.fetch_person_credits(
                            pid,
                            content_type,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        credits = []
                    for m in credits:
                        try:
                            m["aiop_src"] = "people"
                        except Exception:
                            pass
                    filmography.extend(credits)
                # Dedup by title/year
                seen_py: set[tuple[str, int | None]] = set()
                deduped: list[dict[str, Any]] = []
                for m in filmography:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_py:
                        continue
                    seen_py.add(kp)
                    deduped.append(m)
                rng_people = _rng_for(seed, content_type, "people")
                shuffled_people = deduped[:]
                rng_people.shuffle(shuffled_people)
                lane_local_pools["people"] = shuffled_people
                # Personalized recommendations as fallback
                try:
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=max(item_limit * 3, 60),
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                for m in recs:
                    try:
                        m["aiop_src"] = "recommended"
                    except Exception:
                        pass
                rng_recs = _rng_for(seed, content_type, "people-recommended")
                shuffled_recs = recs[:]
                rng_recs.shuffle(shuffled_recs)
                lane_local_pools["recommended"] = shuffled_recs
                # Related from user's recent seeds
                rel_collected: list[dict[str, Any]] = []
                rel_seeds = seeds[:]
                rng_rel_seeds = _rng_for(seed, content_type, "people-related-seeds")
                rng_rel_seeds.shuffle(rel_seeds)
                for sid in rel_seeds[:8]:
                    try:
                        rel = await self._trakt.fetch_related(
                            content_type,
                            trakt_id=sid,
                            limit=min(max(item_limit // 2, 20), 80),
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        rel = []
                    for m in rel:
                        try:
                            m["aiop_src"] = "related"
                        except Exception:
                            pass
                    rel_collected.extend(rel)
                seen_rel: set[tuple[str, int | None]] = set()
                dedup_rel: list[dict[str, Any]] = []
                for m in rel_collected:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_rel:
                        continue
                    seen_rel.add(kp)
                    dedup_rel.append(m)
                rng_rel = _rng_for(seed, content_type, "people-related")
                shuffled_rel = dedup_rel[:]
                rng_rel.shuffle(shuffled_rel)
                lane_local_pools["related"] = shuffled_rel
                # Generic trending/popular as final fallback
                for src in ("trending", "popular"):
                    try:
                        listing = await self._trakt.fetch_listing(
                            content_type,
                            list_type=src,
                            limit=max(item_limit * 2, 30),
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        listing = []
                    for m in listing:
                        try:
                            m["aiop_src"] = src
                        except Exception:
                            pass
                    rng_gp = _rng_for(seed, content_type, f"people-{src}")
                    shuffled_gp = listing[:]
                    rng_gp.shuffle(shuffled_gp)
                    lane_local_pools[src] = shuffled_gp
            elif k == "visually-stunning-for-you":
                include = {g for g in ["sci-fi", "fantasy", "adventure", "drama", "animation"] if g in tg}
            elif k == "background-watching":
                # Use the user's top genres rather than forcing comedy/animation only.
                # Keep episodes short to suit background viewing and prefer familiar/related picks.
                include = set([g for g in tg[:4] if g]) or None
                rt_pred = lambda r: isinstance(r, int) and r > 0 and r <= 40
                prefer_related_order = True
            elif k == "same-universe-different-story":
                include = set([g for g in tg[:3] if g]) or None
                prefer_related_order = True
                # Build a lane-local 'related' pool that emphasizes true shared-universe entries:
                # 1) Start from Trakt's related titles for user's recent seeds
                # 2) Augment with titles that appear in the credits of multiple principal cast members
                #    of those seeds (e.g., spin-offs with overlapping actors/characters)
                lane_local_pools = {src: [] for src in ("recommended", "related", "trending", "popular")}

                # Personalized recommendations (universe lane still benefits from personal recs as fallback)
                try:
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=min(max(item_limit * 5, 100), 100),
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                for m in recs:
                    try:
                        m["aiop_src"] = "recommended"
                    except Exception:
                        pass
                rng_recs = _rng_for(seed, content_type, "universe-recommended")
                shuffled_recs = recs[:]
                rng_recs.shuffle(shuffled_recs)
                lane_local_pools["recommended"] = shuffled_recs

                # Related: gather from user's own seeds
                seeds: list[int] = []
                for mm in media_list:
                    ids = mm.get("ids") or {}
                    tid = ids.get("trakt")
                    if isinstance(tid, int):
                        seeds.append(tid)
                rng_seeds = _rng_for(seed, content_type, "universe-related-seeds")
                rng_seeds.shuffle(seeds)
                seeds = seeds[:8]

                base_related: list[dict[str, Any]] = []
                for sid in seeds:
                    try:
                        if item_limit > 50:
                            rel = await self._trakt.fetch_related_paginated(
                                content_type,
                                trakt_id=sid,
                                total_limit=min(max(item_limit // 2, 60), 200),
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                        else:
                            rel = await self._trakt.fetch_related(
                                content_type,
                                trakt_id=sid,
                                limit=20,
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                    except Exception:
                        rel = []
                    for m in rel:
                        try:
                            m["aiop_src"] = "related"
                        except Exception:
                            pass
                    base_related.extend(rel)
                # Dedup base related by (title, year)
                seen_pairs_rel: set[tuple[str, int | None]] = set()
                deduped_rel: list[dict[str, Any]] = []
                for m in base_related:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_pairs_rel:
                        continue
                    seen_pairs_rel.add(kp)
                    deduped_rel.append(m)

                # Universe augmentation via shared-cast filmography
                # Collect principal cast across a subset of seeds
                actor_counts: Counter[int] = Counter()
                seed_titles: set[tuple[str, int | None]] = set()
                for mm in media_list[:12]:  # cap for performance
                    ids = mm.get("ids") or {}
                    tid = ids.get("trakt")
                    title = (mm.get("title") or "").strip().casefold()
                    year = mm.get("year") if isinstance(mm.get("year"), int) else None
                    if title:
                        seed_titles.add((title, year))
                    if not isinstance(tid, int):
                        continue
                    try:
                        ppl = await self._trakt.fetch_people(
                            content_type,
                            trakt_id=tid,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        ppl = {}
                    cast = ppl.get("cast") if isinstance(ppl, dict) else None
                    if isinstance(cast, list):
                        # Use order if present; otherwise natural order is fine
                        for role in cast[:10]:  # take top-billed subset
                            person = role.get("person") if isinstance(role, dict) else None
                            if isinstance(person, dict):
                                pid = person.get("ids", {}).get("trakt")
                                if isinstance(pid, int):
                                    actor_counts.update([pid])

                top_actor_ids = [pid for pid, _ in actor_counts.most_common(8)]
                credit_counts: dict[tuple[str, int | None], set[int]] = {}
                for pid in top_actor_ids:
                    try:
                        credits = await self._trakt.fetch_person_credits(
                            pid,
                            content_type,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        credits = []
                    for m in credits:
                        t = (m.get("title") or "").strip().casefold()
                        if not t:
                            continue
                        y = m.get("year") if isinstance(m.get("year"), int) else None
                        kp = (t, y)
                        # Skip the seed shows themselves
                        if kp in seed_titles:
                            continue
                        credit_counts.setdefault(kp, set()).add(pid)

                # Choose titles with at least two distinct shared actors or present in related
                related_set = {
                    ((m.get("title") or "").strip().casefold(),
                     m.get("year") if isinstance(m.get("year"), int) else None)
                    for m in deduped_rel
                }
                universe_candidates: list[dict[str, Any]] = []
                # Map from keypair to exemplar media dict from credits
                sample_by_kp: dict[tuple[str, int | None], dict[str, Any]] = {}
                for pid in top_actor_ids:
                    try:
                        credits = await self._trakt.fetch_person_credits(
                            pid,
                            content_type,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        credits = []
                    for m in credits:
                        t = (m.get("title") or "").strip().casefold()
                        if not t:
                            continue
                        y = m.get("year") if isinstance(m.get("year"), int) else None
                        kp = (t, y)
                        if kp in seed_titles:
                            continue
                        shared = credit_counts.get(kp) or set()
                        if (len(shared) >= 2) or (kp in related_set):
                            if kp not in sample_by_kp:
                                # annotate source for later scoring/debug
                                try:
                                    m["aiop_src"] = "people"
                                except Exception:
                                    pass
                                sample_by_kp[kp] = m
                universe_candidates = list(sample_by_kp.values())

                # Merge universe candidates ahead of base related
                # Ensure no duplicates (by title/year)
                seen_merge: set[tuple[str, int | None]] = set()
                merged_related: list[dict[str, Any]] = []
                for m in universe_candidates + deduped_rel:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_merge:
                        continue
                    seen_merge.add(kp)
                    merged_related.append(m)
                rng_rel = _rng_for(seed, content_type, "universe-related")
                shuffled_rel = merged_related[:]
                rng_rel.shuffle(shuffled_rel)
                lane_local_pools["related"] = shuffled_rel

                # Fallback generic pools
                for src in ("trending", "popular"):
                    try:
                        if item_limit > 50:
                            listing = await self._trakt.fetch_listing_paginated(
                                content_type,
                                list_type=src,
                                total_limit=min(max(item_limit * 6, 200), 800),
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                        else:
                            listing = await self._trakt.fetch_listing(
                                content_type,
                                list_type=src,
                                limit=max(item_limit * 2, 30),
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                    except Exception:
                        listing = []
                    for m in listing:
                        try:
                            m["aiop_src"] = src
                        except Exception:
                            pass
                    rng_gp = _rng_for(seed, content_type, f"universe-{src}")
                    shuffled_gp = listing[:]
                    rng_gp.shuffle(shuffled_gp)
                    lane_local_pools[src] = shuffled_gp
            elif k == "animation-worth-your-time":
                include = {"animation"}
                # Keep this lane strictly animation; do not relax genre in later phases
                keep_genre_strict = True
            elif k == "documentaries-youll-like":
                include = {"documentary"}
                # For this lane we must only show documentaries; do not relax genre.
                keep_genre_strict = True
                # Build lane-local pools specifically filtered to documentaries to avoid starvation
                lane_local_pools = {src: [] for src in ("recommended", "related", "trending", "popular")}
                # Personalized recommendations (unfiltered; lane filters will keep only docs)
                try:
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=100,
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                lane_local_pools["recommended"] = recs
                # Use genre-filtered paginated listings for a large documentary pool
                for src in ("trending", "popular"):
                    try:
                        listing = await self._trakt.fetch_listing_paginated(
                            content_type,
                            list_type=src,
                            total_limit=600,
                            genres=["documentary"],
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        listing = []
                    lane_local_pools[src] = listing
            elif k == "your-top-genre":
                include = {top1} if top1 else None
            elif k == "your-second-genre":
                include = {top2} if top2 else None
            elif k == "your-third-genre":
                include = {top3} if top3 else None
            elif k == "franchises-you-started":
                include = set([g for g in tg[:3] if g]) or None
                prefer_related_order = True
            elif k == "independent-films":
                # Prefer indie-tagged items but do not make the genre constraint hard; use scoring to bias.
                include = {"independent", "indie"}
                keep_genre_strict = False
                prefer_related_order = True
                # Build lane-local pools focused on indie; add related and safe fallbacks
                lane_local_pools = {src: [] for src in ("recommended", "related", "trending", "popular")}
                # Personalized recommendations (unfiltered; lane filters and bonuses will bias indie)
                try:
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=100,
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                lane_local_pools["recommended"] = recs
                # Genre-filtered paginated listings for an indie pool. Try 'indie' then 'independent'.
                indie_pool: list[dict[str, Any]] = []
                try:
                    indie_pool = await self._trakt.fetch_listing_paginated(
                        content_type,
                        list_type="trending",
                        total_limit=600,
                        genres=["indie"],
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    indie_pool = []
                if len(indie_pool) < 50:
                    try:
                        more = await self._trakt.fetch_listing_paginated(
                            content_type,
                            list_type="popular",
                            total_limit=600,
                            genres=["independent"],
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                        indie_pool.extend(more)
                    except Exception:
                        pass
                # Dedup indie pool by (title, year)
                seen_ip: set[tuple[str, int | None]] = set()
                deduped_ip: list[dict[str, Any]] = []
                for m in indie_pool:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_ip:
                        continue
                    seen_ip.add(kp)
                    deduped_ip.append(m)
                lane_local_pools["trending"] = deduped_ip[:]
                # Related from user's own seeds to surface similar low-profile films
                rel_collected: list[dict[str, Any]] = []
                rel_seeds: list[int] = []
                for mm in media_list:
                    ids = mm.get("ids") or {}
                    tid = ids.get("trakt")
                    if isinstance(tid, int):
                        rel_seeds.append(tid)
                rng_rel_seeds = _rng_for(seed, content_type, "indie-related-seeds")
                rng_rel_seeds.shuffle(rel_seeds)
                for sid in rel_seeds[:8]:
                    try:
                        rel = await self._trakt.fetch_related(
                            content_type,
                            trakt_id=sid,
                            limit=min(max(item_limit // 2, 20), 80),
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        rel = []
                    for m in rel:
                        try:
                            m["aiop_src"] = "related"
                        except Exception:
                            pass
                    rel_collected.extend(rel)
                seen_rel_i: set[tuple[str, int | None]] = set()
                dedup_rel_i: list[dict[str, Any]] = []
                for m in rel_collected:
                    t = (m.get("title") or "").strip().casefold()
                    y = m.get("year") if isinstance(m.get("year"), int) else None
                    kp = (t, y)
                    if not t or kp in seen_rel_i:
                        continue
                    seen_rel_i.add(kp)
                    dedup_rel_i.append(m)
                rng_rel_i = _rng_for(seed, content_type, "indie-related")
                shuffled_rel_i = dedup_rel_i[:]
                rng_rel_i.shuffle(shuffled_rel_i)
                lane_local_pools["related"] = shuffled_rel_i
                # Provide generic popular fallback to avoid starvation when tags are missing
                try:
                    pop = await self._trakt.fetch_listing(
                        content_type,
                        list_type="popular",
                        limit=max(item_limit * 2, 40),
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    pop = []
                for m in pop:
                    try:
                        m["aiop_src"] = "popular"
                    except Exception:
                        pass
                rng_pop_i = _rng_for(seed, content_type, "indie-popular")
                shuffled_pop_i = pop[:]
                rng_pop_i.shuffle(shuffled_pop_i)
                lane_local_pools["popular"] = shuffled_pop_i

            # Build unseen candidates from Trakt listings, filtered by lane rules
            def _keypair(m: dict[str, Any]) -> tuple[str, int | None]:
                t = (m.get("title") or "").strip()
                y = m.get("year") if isinstance(m.get("year"), int) else None
                return (t.casefold(), y)

            def _fingerprints(m: dict[str, Any]) -> set[str]:
                ids = m.get("ids") or {}
                fps: set[str] = set()
                prefix = content_type
                imdb = ids.get("imdb")
                if isinstance(imdb, str) and imdb.strip():
                    fps.add(f"{prefix}:imdb:{imdb.strip().lower()}")
                trakt_id = ids.get("trakt")
                if isinstance(trakt_id, int):
                    fps.add(f"{prefix}:trakt:{trakt_id}")
                tmdb = ids.get("tmdb")
                if isinstance(tmdb, int):
                    fps.add(f"{prefix}:tmdb:{tmdb}")
                title = (m.get("title") or "").strip().casefold()
                year = m.get("year") if isinstance(m.get("year"), int) else None
                if title:
                    fps.add(f"{prefix}:title:{title}")
                    if year:
                        fps.add(f"{prefix}:title:{title}:{year}")
                return fps

            watched_fps = set((watched.get("movie") if content_type == "movie" else watched.get("series")).fingerprints if watched else set())
            served_fps = set((served or {}).get(content_type) or set())
            lane: list[dict[str, Any]] = []

            # Build candidate pools once per content type and memoize
            pool_order = ("recommended", "related", "trending", "popular")
            if prefer_related_order:
                pool_order = ("related", "recommended", "trending", "popular")
            # Use lane-local pools if we constructed them above (e.g., international, people)
            if lane_local_pools is None and not all(candidate_cache[content_type].get(src) for src in pool_order):
                # IMPORTANT: Build the shared candidate cache with broad filters only.
                # Lane-specific genre/language constraints are applied later by apply_filters.
                genres_filter = None
                lang_counts: Counter[str] = Counter(
                    [
                        (m.get("language") or "").strip().lower()
                        for m in media_list
                        if isinstance(m.get("language"), str) and m.get("language").strip()
                    ]
                )
                top_langs: list[str] = []

                # Personalized recommendations (if available)
                try:
                    recs = await self._trakt.fetch_recommendations(
                        content_type,
                        limit=max(item_limit * 3, 50),
                        client_id=trakt_client_id,
                        access_token=trakt_access_token,
                    )
                except Exception:
                    recs = []
                seen_pairs: set[tuple[str, int | None]] = set()
                deduped_recs: list[dict[str, Any]] = []
                for m in recs:
                    kp = _keypair(m)
                    if not kp[0] or kp in seen_pairs:
                        continue
                    seen_pairs.add(kp)
                    try:
                        m["aiop_src"] = "recommended"
                    except Exception:
                        pass
                    deduped_recs.append(m)
                rng_recs = _rng_for(seed, content_type, "recommended")
                shuffled_recs = deduped_recs[:]
                rng_recs.shuffle(shuffled_recs)
                candidate_cache[content_type]["recommended"] = shuffled_recs

                # Related to user's own history seeds (limit calls for perf)
                seeds: list[int | str] = []
                for m in media_list:
                    ids = m.get("ids") or {}
                    tid = ids.get("trakt")
                    if isinstance(tid, int):
                        seeds.append(tid)
                rng_seeds = _rng_for(seed, content_type, "related-seeds")
                rng_seeds.shuffle(seeds)
                seeds = seeds[:8]
                related_collected: list[dict[str, Any]] = []
                for sid in seeds:
                    try:
                        rel = await self._trakt.fetch_related(
                            content_type,
                            trakt_id=sid,
                            limit=20,
                            client_id=trakt_client_id,
                            access_token=trakt_access_token,
                        )
                    except Exception:
                        rel = []
                    for m in rel:
                        try:
                            m["aiop_src"] = "related"
                        except Exception:
                            pass
                    related_collected.extend(rel)
                # Dedup related pool
                seen_pairs_rel: set[tuple[str, int | None]] = set()
                deduped_rel: list[dict[str, Any]] = []
                for m in related_collected:
                    kp = _keypair(m)
                    if not kp[0] or kp in seen_pairs_rel:
                        continue
                    seen_pairs_rel.add(kp)
                    deduped_rel.append(m)
                rng_rel = _rng_for(seed, content_type, "related")
                shuffled_rel = deduped_rel[:]
                rng_rel.shuffle(shuffled_rel)
                candidate_cache[content_type]["related"] = shuffled_rel

                # Fallback generic pools, smaller weights and later in order
                for src in ("trending", "popular"):
                    try:
                        # For larger requests, page through listings to gather a bigger unique pool
                        if item_limit > 50:
                            listing = await self._trakt.fetch_listing_paginated(
                                content_type,
                                list_type=src,
                                total_limit=min(max(item_limit * 6, 200), 800),
                                genres=None,
                                languages=None,
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                        else:
                            listing = await self._trakt.fetch_listing(
                                content_type,
                                list_type=src,
                                limit=min(max(item_limit * 2, 30), 100),
                                genres=None,
                                languages=None,
                                client_id=trakt_client_id,
                                access_token=trakt_access_token,
                            )
                    except Exception:
                        listing = []
                    seen_pairs_gp: set[tuple[str, int | None]] = set()
                    deduped_gp: list[dict[str, Any]] = []
                    for m in listing:
                        kp = _keypair(m)
                        if not kp[0] or kp in seen_pairs_gp:
                            continue
                        seen_pairs_gp.add(kp)
                        try:
                            m["aiop_src"] = src
                        except Exception:
                            pass
                        deduped_gp.append(m)
                    rng_gp = _rng_for(seed, content_type, src)
                    shuffled_gp = deduped_gp[:]
                    rng_gp.shuffle(shuffled_gp)
                    candidate_cache[content_type][src] = shuffled_gp

            # Selection strategy with retries to reach item_limit
            def _served_allowed(m: dict[str, Any]) -> bool:
                # Allow a fraction of previously served items to keep the lane fresh
                # but not starve the pool over time. Deterministic per run/lane/item.
                title, year, imdb, trakt_id = _item_keyparts(m, content_type=content_type)
                core = f"{content_type}:{definition.key}:{title}:{year}:{imdb}:{trakt_id or ''}:{seed}:served"
                h = hashlib.sha256(core.encode("utf-8")).hexdigest()
                val = int(h[:8], 16) / float(0xFFFFFFFF)
                # ~30% chance to allow a previously served item in early phases
                return val < 0.30

            def _try_collect(
                candidates: list[dict[str, Any]],
                *,
                include_genres: set[str] | None,
                exclude_genres: set[str] | None,
                language_pred,
                runtime_pred,
                allow_served: bool = False,
            ) -> None:
                nonlocal lane
                if len(lane) >= item_limit:
                    return
                items = apply_filters(
                    candidates,
                    include_genres=include_genres,
                    exclude_genres=exclude_genres,
                    language_pred=language_pred,
                    runtime_pred=runtime_pred,
                )
                for m in items:
                    if len(lane) >= item_limit:
                        break
                    # Skip watched and session duplicates
                    if _fingerprints(m) & watched_fps:
                        continue
                    if not allow_served and (_fingerprints(m) & served_fps) and not _served_allowed(m):
                        continue
                    kp = _keypair(m)
                    if not kp[0] or kp in session_seen[content_type]:
                        continue
                    session_seen[content_type].add(kp)
                    lane.append(m)

            # Resolve pools for this lane (use local override if present)
            pools = lane_local_pools if lane_local_pools is not None else candidate_cache[content_type]
            # Determine source iteration order: prioritise lane-local pools if provided
            if lane_local_pools is not None:
                source_order = list(lane_local_pools.keys()) + [s for s in pool_order if s not in lane_local_pools]
            else:
                source_order = list(pool_order)

            # Phase 1: strict filters, prefer recommended/related first
            for src in source_order:
                _try_collect(
                    pools.get(src, []),
                    include_genres=include,
                    exclude_genres=exclude,
                    language_pred=lang_pred,
                    runtime_pred=rt_pred,
                )
                if len(lane) >= item_limit:
                    break
            # Phase 2: relax language/runtime
            if len(lane) < item_limit:
                saved_lang, saved_rt = lang_pred, rt_pred
                # Keep language strict for dedicated international lane
                new_lang = saved_lang if keep_language_strict else None
                for src in source_order:
                    _try_collect(
                        pools.get(src, []),
                        include_genres=include,
                        exclude_genres=exclude,
                        language_pred=new_lang,
                        runtime_pred=None,
                    )
                    if len(lane) >= item_limit:
                        break
            # Phase 3: relax genres entirely (except lanes where genre must stay strict)
            if len(lane) < item_limit:
                for src in source_order:
                    _try_collect(
                        pools.get(src, []),
                        include_genres=(include if keep_genre_strict else None),
                        exclude_genres=None,
                        # Keep language constraint for international lane
                        language_pred=(lang_pred if keep_language_strict else None),
                        runtime_pred=None,
                    )
                    if len(lane) >= item_limit:
                        break

            # Phase 4: final fill allowing previously served titles if still short
            if len(lane) < item_limit and served_fps:
                for src in source_order:
                    _try_collect(
                        pools.get(src, []),
                        include_genres=(include if keep_genre_strict else None),
                        exclude_genres=None,
                        language_pred=(lang_pred if keep_language_strict else None),
                        runtime_pred=None,
                        allow_served=True,
                    )
                    if len(lane) >= item_limit:
                        break

            # For the indie lane, ensure we fill as many indie-tagged titles as possible,
            # only topping up with non-indie if the pool is sparse.
            if definition.key == "independent-films" and content_type == "movie" and lane:
                def _is_indie(m: dict[str, Any]) -> bool:
                    genres = {str(g).strip().lower() for g in (m.get("genres") or []) if g}
                    return bool({"indie", "independent"} & genres)
                indie_items = [m for m in lane if _is_indie(m)]
                if len(indie_items) >= item_limit:
                    lane = indie_items[:]
                else:
                    non_indie = [m for m in lane if m not in indie_items]
                    lane = indie_items + non_indie[: max(0, item_limit - len(indie_items))]

            noise = lambda m: _noise_for_item(m, content_type=content_type, lane_key=definition.key)
            served_penalty = lambda m: (-0.25 if (_fingerprints(m) & served_fps) else 0.0)
            catalog = self._catalog_from_media(
                sorted(
                    lane,
                    key=lambda m: _relevance_score(m, content_type=content_type)
                    + _lane_bonus(m, lane_key=definition.key, content_type=content_type)
                    + noise(m)
                    + served_penalty(m),
                    reverse=True,
                ),
                content_type=content_type,
                title=definition.title,
                description=definition.description,
                seed=seed,
                item_limit=item_limit,
            )
            catalogs[content_type][catalog.id] = catalog

        return catalogs

    async def _load_catalogs(
        self, profile_id: str, content_type: str | None = None
    ) -> list[Catalog]:
        async with self._session_factory() as session:
            stmt = select(CatalogRecord).where(CatalogRecord.profile_id == profile_id)
            if content_type:
                stmt = stmt.where(CatalogRecord.content_type == content_type)
            stmt = stmt.order_by(CatalogRecord.content_type, CatalogRecord.position)
            result = await session.execute(stmt)
            records = result.scalars().all()
        catalogs: list[Catalog] = []
        for record in records:
            try:
                catalogs.append(Catalog.model_validate(record.payload))
            except ValidationError as exc:
                logger.warning(
                    "Stored catalog for profile %s could not be validated: %s",
                    profile_id,
                    exc,
                )
        return catalogs

    async def _load_single_catalog(
        self, profile_id: str, content_type: str, catalog_id: str
    ) -> Catalog | None:
        async with self._session_factory() as session:
            stmt = select(CatalogRecord).where(
                CatalogRecord.profile_id == profile_id,
                CatalogRecord.content_type == content_type,
                CatalogRecord.catalog_id == catalog_id,
            )
            result = await session.execute(stmt)
            record = result.scalar_one_or_none()
            if record is None:
                return None
            payload = record.payload
        try:
            return Catalog.model_validate(payload)
        except ValidationError as exc:
            logger.warning(
                "Stored catalog %s for profile %s is invalid: %s",
                catalog_id,
                profile_id,
                exc,
            )
            return None

    async def _load_catalog_any_profile(
        self, catalog_id: str, *, content_type: str | None = None
    ) -> Catalog | None:
        async with self._session_factory() as session:
            stmt = select(CatalogRecord).where(CatalogRecord.catalog_id == catalog_id)
            if content_type:
                stmt = stmt.where(CatalogRecord.content_type == content_type)
            stmt = stmt.order_by(CatalogRecord.updated_at.desc())
            result = await session.execute(stmt)
            records = result.scalars().all()

            if not records:
                scope, base_id = self._split_scoped_catalog_id(catalog_id)
                if base_id != catalog_id:
                    return await self._load_catalog_any_profile(
                        base_id, content_type=content_type
                    )
                return None

            updated = False
            candidate: Catalog | None = None
            for record in records:
                scope, base_id = self._split_scoped_catalog_id(record.catalog_id)
                if scope != record.profile_id:
                    new_id = self._scoped_catalog_id(record.profile_id, base_id)
                    record.catalog_id = new_id
                    payload = record.payload if isinstance(record.payload, dict) else {}
                    record.payload = {**payload, "id": new_id}
                    updated = True
                try:
                    candidate = Catalog.model_validate(record.payload)
                    break
                except ValidationError as exc:
                    logger.warning(
                        "Stored catalog %s for profile %s is invalid: %s",
                        record.catalog_id,
                        record.profile_id,
                        exc,
                    )
                    continue
            if updated:
                await session.commit()
            return candidate

    def _extract_image(self, media: dict[str, Any], *, key: str = "poster") -> str | None:
        images = media.get("images") or {}
        if not isinstance(images, dict):
            return None
        url = images.get(key) or images.get(f"{key}_full") or images.get(f"{key}_url")
        if isinstance(url, str) and url.startswith("http"):
            return url
        return None

    def _split_scoped_catalog_id(self, catalog_id: str) -> tuple[str | None, str]:
        separator = self._CATALOG_SCOPE_SEPARATOR
        if separator in catalog_id:
            scope, remainder = catalog_id.split(separator, 1)
            if scope and remainder:
                return scope, remainder
        return None, catalog_id

    def _scoped_catalog_id(self, profile_id: str, base_id: str) -> str:
        base = base_id or "catalog"
        return f"{profile_id}{self._CATALOG_SCOPE_SEPARATOR}{base}"
