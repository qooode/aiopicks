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
    catalog_count: int | None = Field(
        default=None,
        ge=1,
        le=12,
        validation_alias=AliasChoices("catalogCount", "count"),
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
        return cls.model_validate(dict(params))

    @field_validator("catalog_count", "refresh_interval", "response_cache", mode="before")
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
    refresh_interval_seconds: int
    response_cache_seconds: int
    next_refresh_at: datetime | None
    last_refreshed_at: datetime | None


@dataclass
class ProfileContext:
    """Resolved profile along with whether a refresh is required."""

    state: ProfileState
    force_refresh: bool = False


class CatalogService:
    """Coordinates Trakt ingestion with AI catalog generation."""

    def __init__(
        self,
        settings: Settings,
        trakt_client: TraktClient,
        openrouter_client: OpenRouterClient,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._settings = settings
        self._trakt = trakt_client
        self._ai = openrouter_client
        self._session_factory = session_factory
        self._locks: dict[str, asyncio.Lock] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_poll_seconds = 60

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

        state = await self.prepare_profile(config)
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

        state = await self.prepare_profile(config)
        catalog = await self._load_single_catalog(state.id, content_type, catalog_id)
        if catalog is None:
            raise KeyError(f"Catalog {catalog_id} not found for profile {state.id}")
        return catalog.to_catalog_response()

    async def find_meta(
        self,
        config: ManifestConfig,
        content_type: str,
        meta_id: str,
    ) -> dict[str, Any]:
        """Locate a specific meta entry within the stored catalogs."""

        state = await self.prepare_profile(config)
        catalogs = await self._load_catalogs(state.id, content_type=content_type)
        for catalog in catalogs:
            for index, item in enumerate(catalog.items):
                meta = item.to_meta(catalog.id, index)
                if meta["id"] == meta_id:
                    return meta
        raise KeyError(f"Meta {meta_id} not found for profile {state.id}")

    async def prepare_profile(self, config: ManifestConfig) -> ProfileState:
        """Resolve the profile, ensure catalogs are current, and return its state."""

        context = await self._resolve_profile(config)
        return await self.ensure_catalogs(context.state, force=context.force_refresh)

    async def ensure_catalogs(
        self, state: ProfileState, *, force: bool = False
    ) -> ProfileState:
        """Refresh catalogs for the profile if the cache is stale."""

        lock = self._locks.setdefault(state.id, asyncio.Lock())
        async with lock:
            latest_state = await self._load_profile_state(state.id) or state
            needs_refresh = force or await self._needs_refresh(latest_state)
            if not needs_refresh:
                return latest_state
            await self._refresh_catalogs(latest_state)
            refreshed_state = await self._load_profile_state(state.id)
            return refreshed_state or latest_state

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
            await self.ensure_catalogs(state, force=True)

    async def _needs_refresh(self, state: ProfileState) -> bool:
        if state.last_refreshed_at is None:
            return True
        if not await self._has_catalogs(state.id):
            return True
        expires_at = state.last_refreshed_at + timedelta(
            seconds=state.response_cache_seconds
        )
        return datetime.utcnow() >= expires_at

    async def _has_catalogs(self, profile_id: str) -> bool:
        async with self._session_factory() as session:
            stmt = (
                select(CatalogRecord.id)
                .where(CatalogRecord.profile_id == profile_id)
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

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
                movie_history, show_history, seed=seed
            )

        await self._store_catalogs(state, catalogs)

    async def _store_catalogs(
        self,
        state: ProfileState,
        catalogs: dict[str, dict[str, Catalog]],
    ) -> None:
        now = datetime.utcnow()
        async with self._session_factory() as session:
            await session.execute(
                delete(CatalogRecord).where(CatalogRecord.profile_id == state.id)
            )
            for content_type, catalog_map in catalogs.items():
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
            refresh_interval_seconds=profile.refresh_interval_seconds,
            response_cache_seconds=profile.response_cache_seconds,
            next_refresh_at=profile.next_refresh_at,
            last_refreshed_at=profile.last_refreshed_at,
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

    def _determine_profile_id(self, config: ManifestConfig) -> str:
        if config.profile_id:
            slug = slugify(config.profile_id)
            if slug:
                return slug
        if config.openrouter_key:
            digest = hashlib.sha256(config.openrouter_key.encode("utf-8")).hexdigest()[:12]
            return f"user-{digest}"
        return "default"

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
    ) -> dict[str, Any]:
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "catalog_count": catalog_count,
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
    ) -> dict[str, dict[str, Catalog]]:
        catalogs: dict[str, dict[str, Catalog]] = {"movie": {}, "series": {}}

        if movie_history:
            catalog = self._history_catalog(
                movie_history,
                content_type="movie",
                title="AI Offline: Movies You Loved",
                seed=seed,
            )
            catalogs["movie"][catalog.id] = catalog

        if show_history:
            catalog = self._history_catalog(
                show_history,
                content_type="series",
                title="AI Offline: Series Marathon",
                seed=seed,
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
    ) -> Catalog:
        key = "movie" if content_type == "movie" else "show"
        items: list[CatalogItem] = []
        for index, entry in enumerate(history[:10]):
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

    def _extract_image(self, media: dict[str, Any], *, key: str = "poster") -> str | None:
        images = media.get("images") or {}
        if not isinstance(images, dict):
            return None
        url = images.get(key) or images.get(f"{key}_full") or images.get(f"{key}_url")
        if isinstance(url, str) and url.startswith("http"):
            return url
        return None

