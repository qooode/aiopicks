"""High level orchestration for catalog generation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping

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

from ..config import Settings
from ..db_models import CatalogRecord, Profile
from ..models import Catalog, CatalogBundle, CatalogItem
from ..utils import slugify
from .metadata_addon import MetadataAddonClient, MetadataMatch
from .openrouter import OpenRouterClient
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
    manifest_name: str | None = Field(
        default=None,
        max_length=120,
        validation_alias=AliasChoices("manifestName", "addonName"),
    )
    catalog_count: int | None = Field(
        default=None,
        ge=1,
        le=12,
        validation_alias=AliasChoices("catalogCount", "count"),
    )
    catalog_item_count: int | None = Field(
        default=None,
        ge=1,
        le=100,
        validation_alias=AliasChoices(
            "catalogItems", "catalogItemCount", "itemsPerCatalog"
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
        ge=10,
        le=2000,
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

    @field_validator(
        "catalog_count",
        "catalog_item_count",
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


@dataclass
class ProfileState:
    """Snapshot of a stored profile used for runtime decisions."""

    id: str
    openrouter_api_key: str
    openrouter_model: str
    trakt_client_id: str | None
    trakt_access_token: str | None
    catalog_count: int
    catalog_item_count: int
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
            "openrouterModel": self.state.openrouter_model,
            "catalogCount": self.state.catalog_count,
            "catalogItemCount": self.state.catalog_item_count,
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
        metadata_client: MetadataAddonClient,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._settings = settings
        self._trakt = trakt_client
        self._ai = openrouter_client
        self._metadata_client = metadata_client
        self._session_factory = session_factory
        self._default_metadata_addon_url = getattr(
            metadata_client, "default_base_url", None
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

        state = await self.prepare_profile(config, wait_for_refresh=False)
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
            state = await self.prepare_profile(config, wait_for_refresh=False)
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

        movie_history_batch, show_history_batch = await asyncio.gather(
            self._trakt.fetch_history(
                "movies",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=state.trakt_history_limit,
            ),
            self._trakt.fetch_history(
                "shows",
                client_id=state.trakt_client_id,
                access_token=state.trakt_access_token,
                limit=state.trakt_history_limit,
            ),
        )
        movie_history = movie_history_batch.items
        show_history = show_history_batch.items

        movie_total, show_total, snapshot = await self._gather_trakt_history_metadata(
            state,
            movie_batch=movie_history_batch,
            show_batch=show_history_batch,
        )

        await self._store_trakt_history_stats(
            state,
            history_limit=state.trakt_history_limit,
            movie_total=movie_total,
            show_total=show_total,
            snapshot=snapshot,
        )

        summary = self._build_summary(
            movie_history,
            show_history,
            catalog_count=state.catalog_count,
            catalog_item_count=state.catalog_item_count,
        )
        seed = secrets.token_hex(4)
        catalogs: dict[str, dict[str, Catalog]] | None = None
        metadata_url = state.metadata_addon_url or self._default_metadata_addon_url
        watched_index = self._build_watched_index(movie_history, show_history)
        exclusion_payload = self._serialise_watched_index(watched_index)

        try:
            bundle = await self._ai.generate_catalogs(
                summary,
                seed=seed,
                api_key=state.openrouter_api_key,
                model=state.openrouter_model,
                exclusions=exclusion_payload,
            )
            catalogs = self._bundle_to_dict(bundle)
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
            trakt_client_id=profile.trakt_client_id,
            trakt_access_token=profile.trakt_access_token,
            catalog_count=profile.catalog_count,
            catalog_item_count=getattr(
                profile, "catalog_item_count", self._settings.catalog_item_count
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
                        updated_items.append(item.model_copy(update=updates))
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
                openrouter_key = config.openrouter_key or self._settings.openrouter_api_key
                if not openrouter_key:
                    raise ValueError("An OpenRouter API key is required.")
                metadata_addon = (
                    str(config.metadata_addon_url)
                    if config.metadata_addon_url is not None
                    else self._default_metadata_addon_url
                )
                profile = Profile(
                    id=profile_id,
                    display_name=identity.display_name,
                    openrouter_api_key=openrouter_key,
                    openrouter_model=config.openrouter_model or self._settings.openrouter_model,
                    catalog_count=config.catalog_count or self._settings.catalog_count,
                    catalog_item_count=(
                        config.catalog_item_count
                        or self._settings.catalog_item_count
                    ),
                    refresh_interval_seconds=config.refresh_interval or self._settings.refresh_interval_seconds,
                    response_cache_seconds=config.response_cache or self._settings.response_cache_seconds,
                    trakt_client_id=config.trakt_client_id or self._settings.trakt_client_id,
                    trakt_access_token=config.trakt_access_token or self._settings.trakt_access_token,
                    trakt_history_limit=(
                        config.trakt_history_limit
                        or self._settings.trakt_history_limit
                    ),
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
                if config.catalog_count and config.catalog_count != profile.catalog_count:
                    profile.catalog_count = config.catalog_count
                    refresh_required = True
                if (
                    config.catalog_item_count
                    and config.catalog_item_count != getattr(
                        profile, "catalog_item_count", None
                    )
                ):
                    profile.catalog_item_count = config.catalog_item_count
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
                if (
                    config.trakt_history_limit
                    and config.trakt_history_limit
                    != getattr(profile, "trakt_history_limit", None)
                ):
                    profile.trakt_history_limit = config.trakt_history_limit
                    refresh_required = True
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
                if not self._settings.openrouter_api_key:
                    logger.warning(
                        "Skipping default profile creation because OPENROUTER_API_KEY is not set"
                    )
                    return
                profile = Profile(
                    id="default",
                    openrouter_api_key=self._settings.openrouter_api_key,
                    openrouter_model=self._settings.openrouter_model,
                    trakt_client_id=self._settings.trakt_client_id,
                    trakt_access_token=self._settings.trakt_access_token,
                    trakt_history_limit=self._settings.trakt_history_limit,
                    catalog_count=self._settings.catalog_count,
                    catalog_item_count=self._settings.catalog_item_count,
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
                if not profile.openrouter_api_key and self._settings.openrouter_api_key:
                    profile.openrouter_api_key = self._settings.openrouter_api_key
                    updated = True
                if not profile.openrouter_model:
                    profile.openrouter_model = self._settings.openrouter_model
                    updated = True
                if profile.catalog_count != self._settings.catalog_count:
                    profile.catalog_count = self._settings.catalog_count
                    updated = True
                if getattr(profile, "catalog_item_count", None) != self._settings.catalog_item_count:
                    profile.catalog_item_count = self._settings.catalog_item_count
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
                        fingerprints.add(f"{prefix}:title:{lowered}:{display_year}")

        return WatchedMediaIndex(fingerprints=fingerprints, recent_titles=titles[:40])

    def _build_summary(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
        *,
        catalog_count: int,
        catalog_item_count: int,
    ) -> dict[str, Any]:
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "catalog_count": catalog_count,
            "catalog_item_count": catalog_item_count,
            "profile": {
                "movies": TraktClient.summarize_history(movie_history, key="movie"),
                "series": TraktClient.summarize_history(show_history, key="show"),
            },
        }

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

