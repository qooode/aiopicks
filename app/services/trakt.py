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
        target: int | None
        try:
            target = int(resolved_limit) if resolved_limit is not None else None
        except (TypeError, ValueError):
            target = int(self._settings.trakt_history_limit)
        if target is not None and target <= 0:
            target = None

        collected: list[dict[str, Any]] = []
        total = 0
        page = 1
        max_page_size = 100
        remaining = target

        while True:
            page_limit = max_page_size if remaining is None else min(remaining, max_page_size)
            if page_limit <= 0:
                break
            params = {
                "limit": page_limit,
                "page": page,
                "extended": "full",
            }
            response = await self._client.get(
                url,
                headers=self._headers(
                    client_id=resolved_client_id, access_token=resolved_access_token
                ),
                params=params,
            )
            if response.status_code >= 400:
                logger.warning(
                    "Failed to fetch Trakt history for %s: %s",
                    content_type,
                    response.text,
                )
                return HistoryBatch(items=[], total=0, fetched=False)

            data = response.json()
            if not isinstance(data, list):
                logger.warning("Unexpected Trakt response structure for %s", content_type)
                return HistoryBatch(items=[], total=0, fetched=False)

            if total == 0:
                total = self._extract_total_count(response, fallback=len(data))

            if not data:
                break

            collected.extend(data)
            received = len(data)
            if remaining is not None:
                remaining -= received
                if remaining <= 0:
                    break

            if received < page_limit:
                break

            page_count_header = response.headers.get("x-pagination-page-count")
            if page_count_header:
                try:
                    page_count = int(page_count_header)
                except (TypeError, ValueError):
                    page_count = None
                if page_count is not None and page >= page_count:
                    break

            page += 1

        if target is not None and len(collected) > target:
            collected = collected[:target]

        return HistoryBatch(items=collected, total=total or len(collected), fetched=True)

    async def fetch_stats(
        self,
        *,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> dict[str, Any]:
        """Fetch aggregate watch statistics for the authenticated user."""

        resolved_client_id = client_id or self._settings.trakt_client_id
        resolved_access_token = access_token or self._settings.trakt_access_token

        if not (resolved_client_id and resolved_access_token):
            logger.info("Trakt credentials missing, returning empty stats")
            return {}

        response = await self._client.get(
            "/users/me/stats",
            headers=self._headers(
                client_id=resolved_client_id, access_token=resolved_access_token
            ),
        )
        if response.status_code >= 400:
            logger.warning("Failed to fetch Trakt stats: %s", response.text)
            return {}
        data = response.json()
        if not isinstance(data, dict):
            logger.warning("Unexpected Trakt stats response structure")
            return {}
        return data

    async def fetch_listing(
        self,
        content_type: str,
        *,
        list_type: str = "trending",
        limit: int = 100,
        genres: list[str] | None = None,
        languages: list[str] | None = None,
        years: str | None = None,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a generic listing (trending/popular/recommended) from Trakt.

        Only uses endpoints that are widely available. This returns normalized media dicts
        (same shape as history: under key 'movie' or 'show' originally).
        """
        kind = "movies" if content_type == "movie" else "shows"
        path = f"/{kind}/{list_type}"
        params: dict[str, Any] = {
            "limit": max(1, min(int(limit or 100), 100)),
            "page": 1,
            "extended": "full",
        }
        # Trakt supports filters via query string for many list endpoints; pass if provided
        if genres:
            try:
                cleaned = ",".join(sorted({g.strip().lower() for g in genres if g}))
                if cleaned:
                    params["genres"] = cleaned
            except Exception:
                pass
        if languages:
            try:
                cleaned = ",".join(sorted({l.strip().lower() for l in languages if l}))
                if cleaned:
                    params["languages"] = cleaned
            except Exception:
                pass
        if years:
            params["years"] = years

        response = await self._client.get(
            path,
            headers=self._headers(client_id=client_id, access_token=access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to fetch Trakt %s listing for %s: %s",
                list_type,
                content_type,
                response.text,
            )
            return []
        data = response.json()
        if not isinstance(data, list):
            return []

        key = "movie" if content_type == "movie" else "show"
        normalized: list[dict[str, Any]] = []
        for entry in data:
            if isinstance(entry, dict) and key in entry and isinstance(entry[key], dict):
                normalized.append(entry[key])
            elif isinstance(entry, dict) and entry.get("title") and entry.get("ids"):
                normalized.append(entry)
        return normalized

    async def fetch_user(
        self,
        *,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> dict[str, Any]:
        """Return the authenticated user's profile information."""

        resolved_client_id = client_id or self._settings.trakt_client_id
        resolved_access_token = access_token or self._settings.trakt_access_token

        if not (resolved_client_id and resolved_access_token):
            logger.info("Trakt credentials missing, returning anonymous profile")
            return {}

        response = await self._client.get(
            "/users/me",
            headers=self._headers(
                client_id=resolved_client_id, access_token=resolved_access_token
            ),
        )
        if response.status_code >= 400:
            logger.warning("Failed to fetch Trakt user profile: %s", response.text)
            return {}
        data = response.json()
        if not isinstance(data, dict):
            logger.warning("Unexpected Trakt user profile structure")
            return {}
        return data

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
