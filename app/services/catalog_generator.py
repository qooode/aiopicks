"""High level orchestration for catalog generation."""

from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any

from pydantic import ValidationError

from ..config import Settings
from ..models import Catalog, CatalogBundle, CatalogItem
from ..utils import slugify
from .openrouter import OpenRouterClient
from .trakt import TraktClient

logger = logging.getLogger(__name__)


class CatalogService:
    """Coordinates Trakt ingestion with AI catalog generation."""

    def __init__(
        self,
        settings: Settings,
        trakt_client: TraktClient,
        openrouter_client: OpenRouterClient,
    ):
        self._settings = settings
        self._trakt = trakt_client
        self._ai = openrouter_client
        self._catalogs: dict[str, dict[str, Catalog]] = {"movie": {}, "series": {}}
        self._lock = asyncio.Lock()
        self._last_refresh: datetime | None = None
        self._refresh_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialise the service and launch the refresh loop."""

        await self.ensure_catalogs(force=True)
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

    async def ensure_catalogs(self, *, force: bool = False) -> None:
        """Refresh catalogs if the cache is stale."""

        if not force and not self._should_refresh():
            return

        async with self._lock:
            if not force and not self._should_refresh():
                return
            await self._refresh_catalogs()

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._settings.refresh_interval_seconds)
            try:
                await self.ensure_catalogs(force=True)
            except Exception as exc:  # pragma: no cover - background safety net
                logger.exception("Scheduled refresh failed: %s", exc)

    def _should_refresh(self) -> bool:
        if self._last_refresh is None:
            return True
        expires_at = self._last_refresh + timedelta(seconds=self._settings.response_cache_seconds)
        return datetime.utcnow() >= expires_at

    async def _refresh_catalogs(self) -> None:
        logger.info("Refreshing catalogs via OpenRouter model %s", self._settings.openrouter_model)

        movie_history = await self._trakt.fetch_history("movies")
        show_history = await self._trakt.fetch_history("shows")

        summary = self._build_summary(movie_history, show_history)
        seed = secrets.token_hex(4)

        try:
            bundle = await self._ai.generate_catalogs(summary, seed=seed)
            catalogs = self._bundle_to_dict(bundle)
            if catalogs["movie"] or catalogs["series"]:
                self._catalogs = catalogs
                self._last_refresh = datetime.utcnow()
                logger.info(
                    "Catalog refresh succeeded with %d movie and %d series catalogs",
                    len(catalogs["movie"]),
                    len(catalogs["series"]),
                )
                return
            logger.warning("AI returned an empty catalog bundle, falling back to history data")
        except Exception as exc:
            logger.exception("AI generation failed, falling back to history data: %s", exc)

        fallback = self._build_fallback_catalogs(movie_history, show_history, seed=seed)
        self._catalogs = fallback
        self._last_refresh = datetime.utcnow()

    def _bundle_to_dict(self, bundle: CatalogBundle) -> dict[str, dict[str, Catalog]]:
        return {
            "movie": {catalog.id: catalog for catalog in bundle.movie_catalogs},
            "series": {catalog.id: catalog for catalog in bundle.series_catalogs},
        }

    def _build_summary(
        self,
        movie_history: list[dict[str, Any]],
        show_history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "catalog_count": self._settings.catalog_count,
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
            background = self._extract_image(media, variant="fanart")
            if background:
                data["background"] = background

            try:
                item = CatalogItem.model_validate(data)
            except ValidationError:
                continue
            items.append(item)

        catalog_id = slugify(f"{title}-{seed[:6]}")
        return Catalog(
            id=f"aiopicks-{content_type}-{catalog_id}",
            type=content_type,  # type: ignore[arg-type]
            title=title,
            description="Fallback catalog generated from your Trakt history.",
            seed=seed,
            items=items,
            generated_at=datetime.utcnow(),
        )

    @staticmethod
    def _extract_image(media: dict[str, Any], *, variant: str = "poster") -> str | None:
        images = media.get("images")
        if isinstance(images, dict):
            variant_data = images.get(variant)
            if isinstance(variant_data, dict):
                for key in ("full", "medium", "thumb"):
                    value = variant_data.get(key)
                    if isinstance(value, str) and value.startswith("http"):
                        return value
        # fallback to direct fields some Trakt payloads expose
        field = media.get(f"{variant}")
        if isinstance(field, str) and field.startswith("http"):
            return field
        return None

    async def list_manifest_catalogs(self) -> list[dict[str, object]]:
        await self.ensure_catalogs()
        entries: list[dict[str, object]] = []
        for content_type in ("movie", "series"):
            for catalog in self._catalogs.get(content_type, {}).values():
                entries.append(catalog.to_manifest_entry())
        return entries

    async def get_catalog_payload(self, content_type: str, catalog_id: str) -> dict[str, object]:
        await self.ensure_catalogs()
        catalogs = self._catalogs.get(content_type)
        if not catalogs:
            raise KeyError(f"Unknown catalog type: {content_type}")
        catalog = catalogs.get(catalog_id)
        if not catalog:
            raise KeyError(f"Catalog {catalog_id} not found")
        return catalog.to_catalog_response()

    async def find_meta(self, content_type: str, meta_id: str) -> dict[str, object]:
        await self.ensure_catalogs()
        catalogs = self._catalogs.get(content_type)
        if not catalogs:
            raise KeyError(f"Unknown catalog type: {content_type}")
        for catalog in catalogs.values():
            payload = catalog.to_catalog_response()
            for meta in payload.get("metas", []):
                if meta.get("id") == meta_id:
                    return meta
        raise KeyError(f"Meta {meta_id} not found in {content_type} catalogs")
