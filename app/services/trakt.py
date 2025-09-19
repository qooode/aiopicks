"""Utilities for communicating with the Trakt API."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from ..config import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HistoryBatch:
    """Container for a page of history items and the reported total size."""

    items: list[dict[str, Any]]
    total: int = 0
    fetched: bool = True


class TraktClient:
    """Thin wrapper around the Trakt HTTP API."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient):
        self._settings = settings
        self._client = http_client

    def _headers(
        self,
        *,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> dict[str, str]:
        headers = {
            "trakt-api-version": "2",
        }
        resolved_client_id = client_id or self._settings.trakt_client_id
        resolved_access_token = access_token or self._settings.trakt_access_token
        if resolved_client_id:
            headers["trakt-api-key"] = resolved_client_id
        if resolved_access_token:
            headers["Authorization"] = f"Bearer {resolved_access_token}"
        return headers

    async def fetch_history(
        self,
        content_type: str,
        *,
        client_id: str | None = None,
        access_token: str | None = None,
        limit: int | None = None,
    ) -> HistoryBatch:
        """Fetch the user's viewing history."""

        resolved_client_id = client_id or self._settings.trakt_client_id
        resolved_access_token = access_token or self._settings.trakt_access_token

        if not (resolved_client_id and resolved_access_token):
            logger.info("Trakt credentials missing, returning empty history for %s", content_type)
            return HistoryBatch(items=[], total=0, fetched=False)

        url = f"/sync/history/{content_type}"
        resolved_limit = limit if limit is not None else self._settings.trakt_history_limit
        params = {
            "limit": resolved_limit,
            "extended": "full",
        }
        response = await self._client.get(
            url,
            headers=self._headers(client_id=resolved_client_id, access_token=resolved_access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning("Failed to fetch Trakt history for %s: %s", content_type, response.text)
            return HistoryBatch(items=[], total=0, fetched=False)
        data = response.json()
        if not isinstance(data, list):
            logger.warning("Unexpected Trakt response structure for %s", content_type)
            return HistoryBatch(items=[], total=0, fetched=False)
        total = self._extract_total_count(response, fallback=len(data))
        return HistoryBatch(items=data, total=total, fetched=True)

    @staticmethod
    def _extract_total_count(response: httpx.Response, *, fallback: int = 0) -> int:
        header_value = response.headers.get("x-pagination-item-count")
        if not header_value:
            return fallback
        try:
            return int(header_value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def summarize_history(history: list[dict[str, Any]], *, key: str) -> dict[str, Any]:
        """Summarize a history dataset for the language model."""

        genres: Counter[str] = Counter()
        countries: Counter[str] = Counter()
        languages: Counter[str] = Counter()
        runtimes: list[int] = []
        titles: list[str] = []
        latest_watch: datetime | None = None

        for entry in history:
            media = entry.get(key) or {}
            if not isinstance(media, dict):
                continue
            title = media.get("title")
            if isinstance(title, str):
                titles.append(title)
            genres.update(g for g in (media.get("genres") or []) if isinstance(g, str))
            countries.update(c for c in (media.get("country") or []) if isinstance(c, str))
            language = media.get("language")
            if isinstance(language, str):
                languages.update([language])
            runtime = media.get("runtime")
            if isinstance(runtime, int):
                runtimes.append(runtime)

            watched_at = entry.get("watched_at")
            if isinstance(watched_at, str):
                try:
                    parsed = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                except ValueError:
                    parsed = None
                if parsed and (latest_watch is None or parsed > latest_watch):
                    latest_watch = parsed

        def top_values(counter: Counter[str]) -> list[tuple[str, int]]:
            return counter.most_common(5)

        return {
            "total": len(history),
            "top_titles": titles[:20],
            "top_genres": top_values(genres),
            "top_countries": top_values(countries),
            "top_languages": top_values(languages),
            "average_runtime": sum(runtimes) // len(runtimes) if runtimes else None,
            "last_watched_at": latest_watch.isoformat() if latest_watch else None,
        }
