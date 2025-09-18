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

from pydantic import AliasChoices, BaseModel, Field, ValidationError, field_validator
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import Settings
from ..db_models import CatalogRecord, Profile
from ..models import Catalog, CatalogBundle, CatalogItem
from ..utils import slugify
from .cinemeta import CinemetaClient, CinemetaMatch
from .openrouter import OpenRouterClient
from .trakt import TraktClient

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
    trakt_client_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("traktClientId", "traktClientID"),
    )
    trakt_access_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices("traktAccessToken", "traktToken"),
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
    next_refresh_at: datetime | None
    last_refreshed_at: datetime | None


@dataclass
class ProfileContext:
    """Resolved profile along with whether a refresh is required."""

    state: ProfileState
    force_refresh: bool = False


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


class CatalogService:
    """Coordinates Trakt ingestion with AI catalog generation."""

    _CATALOG_SCOPE_SEPARATOR = "__"

    def __init__(
        self,
        settings: Settings,
        trakt_client: TraktClient,
        openrouter_client: OpenRouterClient,
        cinemeta_client: CinemetaClient,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._settings = settings
        self._trakt = trakt_client
        self._ai = openrouter_client
        self._cinemeta = cinemeta_client
        self._session_factory = session_factory
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

        movie_history = await self._trakt.fetch_history(
            "movies",
            client_id=state.trakt_client_id,
            access_token=state.trakt_access_token,
        )
        show_history = await self._trakt.fetch_history(
            "shows",
            client_id=state.trakt_client_id,
            access_token=state.trakt_access_token,
        )

        summary = self._build_summary(
            movie_history,
            show_history,
            catalog_count=state.catalog_count,
            catalog_item_count=state.catalog_item_count,
        )
        seed = secrets.token_hex(4)
        catalogs: dict[str, dict[str, Catalog]] | None = None

        try:
            bundle = await self._ai.generate_catalogs(
                summary,
                seed=seed,
                api_key=state.openrouter_api_key,
                model=state.openrouter_model,
            )
            catalogs = self._bundle_to_dict(bundle)
            await self._enrich_catalogs_with_cinemeta(catalogs)
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
            await self._enrich_catalogs_with_cinemeta(catalogs)

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
            next_refresh_at=profile.next_refresh_at,
            last_refreshed_at=profile.last_refreshed_at,
        )

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

    async def _enrich_catalogs_with_cinemeta(
        self, catalogs: dict[str, dict[str, Catalog]]
    ) -> None:
        """Populate missing identifiers and artwork by querying Cinemeta."""

        lookup_tasks: dict[
            tuple[str, str, int | None], asyncio.Task[CinemetaMatch | None]
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
                    ) -> CinemetaMatch | None:
                        return await self._cinemeta.lookup(
                            title, content_type=content_type, year=year
                        )

                    lookup_tasks[key] = asyncio.create_task(_lookup())

        if not lookup_tasks:
            return

        results = await asyncio.gather(
            *lookup_tasks.values(), return_exceptions=True
        )
        matches: dict[tuple[str, str, int | None], CinemetaMatch] = {}
        for key, result in zip(lookup_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.warning("Cinemeta lookup failed for %s: %s", key, result)
                continue
            if result is None:
                continue
            matches[key] = result

        if not matches:
            return

        for catalog_map in catalogs.values():
            for catalog_id, catalog in list(catalog_map.items()):
                updated_items: list[CatalogItem] = []
                for item in catalog.items:
                    title = (item.title or "").strip()
                    if not title:
                        updated_items.append(item)
                        continue
                    key = (item.type, title.casefold(), item.year)
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

    async def _resolve_profile(self, config: ManifestConfig) -> ProfileContext:
        profile_id = self._determine_profile_id(config)
        async with self._session_factory() as session:
            profile = await session.get(Profile, profile_id)
            created = False
            refresh_required = False
            now = datetime.utcnow()

            if profile is None:
                openrouter_key = config.openrouter_key or self._settings.openrouter_api_key
                if not openrouter_key:
                    raise ValueError("An OpenRouter API key is required.")
                profile = Profile(
                    id=profile_id,
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
                    next_refresh_at=now,
                    last_refreshed_at=None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(profile)
                created = True
                refresh_required = True
            else:
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

    def determine_profile_id(self, config: ManifestConfig) -> str:
        """Expose profile id derivation for external callers."""

        return self._determine_profile_id(config)

    def _determine_profile_id(self, config: ManifestConfig) -> str:
        if config.profile_id:
            slug = slugify(config.profile_id)
            if slug:
                return slug
        if config.openrouter_key:
            digest = hashlib.sha256(config.openrouter_key.encode("utf-8")).hexdigest()[:12]
            return f"user-{digest}"
        return "default"

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
                    catalog_count=self._settings.catalog_count,
                    catalog_item_count=self._settings.catalog_item_count,
                    refresh_interval_seconds=self._settings.refresh_interval_seconds,
                    response_cache_seconds=self._settings.response_cache_seconds,
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

