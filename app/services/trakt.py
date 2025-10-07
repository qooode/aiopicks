"""Utilities for communicating with the Trakt API."""

from __future__ import annotations

import logging
import asyncio
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
        self._max_retries = 3

    def _headers(
        self,
        *,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> dict[str, str]:
        headers = {
            "trakt-api-version": "2",
            "User-Agent": f"{self._settings.app_name} (aiopicks)",
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
            # Retry on transient errors (timeouts, 5xx)
            attempt = 0
            while True:
                try:
                    response = await self._client.get(
                        url,
                        headers=self._headers(
                            client_id=resolved_client_id, access_token=resolved_access_token
                        ),
                        params=params,
                    )
                except httpx.HTTPError as exc:
                    attempt += 1
                    if attempt <= self._max_retries:
                        backoff = min(2 ** (attempt - 1), 5) + (0.1 * attempt)
                        logger.info(
                            "Transient error talking to Trakt (%s). Retrying page %s in %.1fs",
                            exc.__class__.__name__,
                            page,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    logger.warning(
                        "Failed to fetch Trakt history for %s (page %s): %s",
                        content_type,
                        page,
                        exc,
                    )
                    # Return what we've collected so far; mark as not fully fetched
                    return HistoryBatch(items=collected, total=total or len(collected), fetched=False)

                # HTTP response received
                if 500 <= response.status_code < 600:
                    attempt += 1
                    if attempt <= self._max_retries:
                        backoff = min(2 ** (attempt - 1), 5) + (0.1 * attempt)
                        logger.info(
                            "Trakt 5xx during history fetch for %s (page %s). Retrying in %.1fs",
                            content_type,
                            page,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    logger.warning(
                        "Failed to fetch Trakt history for %s: %s",
                        content_type,
                        response.text,
                    )
                    return HistoryBatch(items=collected, total=total or len(collected), fetched=False)
                break

            try:
                data = response.json()
            except ValueError:
                logger.warning("Unexpected non-JSON Trakt response for %s history", content_type)
                return HistoryBatch(items=collected, total=total or len(collected), fetched=False)
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
            # Small delay to avoid hammering Cloudflare during large histories
            await asyncio.sleep(0.1)

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

        attempt = 0
        while True:
            try:
                response = await self._client.get(
                    "/users/me/stats",
                    headers=self._headers(
                        client_id=resolved_client_id, access_token=resolved_access_token
                    ),
                )
            except httpx.HTTPError as exc:
                attempt += 1
                if attempt <= self._max_retries:
                    backoff = min(2 ** (attempt - 1), 5) + (0.1 * attempt)
                    logger.info(
                        "Transient error talking to Trakt stats (%s). Retrying in %.1fs",
                        exc.__class__.__name__,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("Failed to fetch Trakt stats: %s", exc)
                return {}
            if 500 <= response.status_code < 600:
                attempt += 1
                if attempt <= self._max_retries:
                    backoff = min(2 ** (attempt - 1), 5) + (0.1 * attempt)
                    logger.info(
                        "Trakt 5xx during stats fetch. Retrying in %.1fs",
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("Failed to fetch Trakt stats: %s", response.text)
                return {}
            break

        try:
            data = response.json()
        except ValueError:
            return {}
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

    async def fetch_listing_paginated(
        self,
        content_type: str,
        *,
        list_type: str = "trending",
        total_limit: int = 300,
        genres: list[str] | None = None,
        languages: list[str] | None = None,
        years: str | None = None,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch multiple pages of a Trakt listing (trending/popular) until total_limit.

        Dedupes across pages and normalizes to media dicts. Each page is capped at 100.
        """

        kind = "movies" if content_type == "movie" else "shows"
        path = f"/{kind}/{list_type}"
        collected: list[dict[str, Any]] = []
        seen_pairs: set[tuple[str, int | None]] = set()
        page = 1
        per_page = 100

        def _kp(m: dict[str, Any]) -> tuple[str, int | None]:
            title = (m.get("title") or "").strip().casefold()
            year = m.get("year") if isinstance(m.get("year"), int) else None
            return (title, year)

        target = max(1, int(total_limit or 0))
        while len(collected) < target:
            params: dict[str, Any] = {
                "extended": "full",
                "limit": per_page,
                "page": page,
            }
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
                    "Failed to fetch Trakt %s listing page %s for %s: %s",
                    list_type,
                    page,
                    content_type,
                    response.text,
                )
                break
            data = response.json()
            if not isinstance(data, list) or not data:
                break

            key = "movie" if content_type == "movie" else "show"
            page_items: list[dict[str, Any]] = []
            for entry in data:
                if isinstance(entry, dict) and key in entry and isinstance(entry[key], dict):
                    page_items.append(entry[key])
                elif isinstance(entry, dict) and entry.get("title") and entry.get("ids"):
                    page_items.append(entry)

            added = 0
            for m in page_items:
                kp = _kp(m)
                if not kp[0] or kp in seen_pairs:
                    continue
                seen_pairs.add(kp)
                collected.append(m)
                added += 1
                if len(collected) >= target:
                    break

            if added < per_page:
                break
            page += 1

        return collected

    async def fetch_recommendations(
        self,
        content_type: str,
        *,
        limit: int = 100,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch Trakt's personalized recommendations for the authenticated user.

        Requires a valid OAuth access token. Returns normalized media dicts.
        """

        resolved_client_id = client_id or self._settings.trakt_client_id
        resolved_access_token = access_token or self._settings.trakt_access_token
        if not (resolved_client_id and resolved_access_token):
            logger.info("Trakt credentials missing, skipping personalized recommendations for %s", content_type)
            return []

        kind = "movies" if content_type == "movie" else "shows"
        path = f"/recommendations/{kind}"
        params: dict[str, Any] = {
            "limit": max(1, min(int(limit or 100), 100)),
            "extended": "full",
        }
        response = await self._client.get(
            path,
            headers=self._headers(client_id=resolved_client_id, access_token=resolved_access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to fetch Trakt recommendations for %s: %s",
                content_type,
                response.text,
            )
            return []
        data = response.json()
        if not isinstance(data, list):
            return []
        normalized: list[dict[str, Any]] = []
        for entry in data:
            if isinstance(entry, dict) and entry.get("title") and entry.get("ids"):
                normalized.append(entry)
        return normalized

    async def fetch_related_paginated(
        self,
        content_type: str,
        *,
        trakt_id: int | str,
        total_limit: int = 100,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch multiple pages of titles related to a specific Trakt item.

        Dedupes and normalizes; each page is capped at 100.
        """

        kind = "movies" if content_type == "movie" else "shows"
        path = f"/{kind}/{trakt_id}/related"
        collected: list[dict[str, Any]] = []
        seen_pairs: set[tuple[str, int | None]] = set()
        page = 1
        per_page = 100

        def _kp(m: dict[str, Any]) -> tuple[str, int | None]:
            title = (m.get("title") or "").strip().casefold()
            year = m.get("year") if isinstance(m.get("year"), int) else None
            return (title, year)

        target = max(1, int(total_limit or 0))
        while len(collected) < target:
            params: dict[str, Any] = {
                "limit": per_page,
                "page": page,
                "extended": "full",
            }
            response = await self._client.get(
                path,
                headers=self._headers(client_id=client_id, access_token=access_token),
                params=params,
            )
            if response.status_code >= 400:
                logger.warning(
                    "Failed to fetch Trakt related page %s for %s %s: %s",
                    page,
                    content_type,
                    trakt_id,
                    response.text,
                )
                break
            data = response.json()
            if not isinstance(data, list) or not data:
                break

            page_items: list[dict[str, Any]] = []
            for entry in data:
                if isinstance(entry, dict) and entry.get("title") and entry.get("ids"):
                    page_items.append(entry)

            added = 0
            for m in page_items:
                kp = _kp(m)
                if not kp[0] or kp in seen_pairs:
                    continue
                seen_pairs.add(kp)
                collected.append(m)
                added += 1
                if len(collected) >= target:
                    break

            if added < per_page:
                break
            page += 1

        return collected

    async def fetch_related(
        self,
        content_type: str,
        *,
        trakt_id: int | str,
        limit: int = 20,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch titles related to a specific Trakt item (no auth required)."""

        kind = "movies" if content_type == "movie" else "shows"
        path = f"/{kind}/{trakt_id}/related"
        params: dict[str, Any] = {
            "limit": max(1, min(int(limit or 20), 100)),
            "extended": "full",
        }
        response = await self._client.get(
            path,
            headers=self._headers(client_id=client_id, access_token=access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to fetch Trakt related for %s %s: %s",
                content_type,
                trakt_id,
                response.text,
            )
            return []
        data = response.json()
        if not isinstance(data, list):
            return []
        normalized: list[dict[str, Any]] = []
        for entry in data:
            if isinstance(entry, dict) and entry.get("title") and entry.get("ids"):
                normalized.append(entry)
        return normalized

    async def fetch_people(
        self,
        content_type: str,
        *,
        trakt_id: int | str,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> dict[str, Any]:
        """Fetch cast/crew for a specific Trakt item.

        Returns a raw dict from Trakt with keys like 'cast' and 'crew'.
        """

        kind = "movies" if content_type == "movie" else "shows"
        path = f"/{kind}/{trakt_id}/people"
        params: dict[str, Any] = {
            "extended": "full",
        }
        response = await self._client.get(
            path,
            headers=self._headers(client_id=client_id, access_token=access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to fetch Trakt people for %s %s: %s",
                content_type,
                trakt_id,
                response.text,
            )
            return {}
        data = response.json()
        if not isinstance(data, dict):
            return {}
        return data

    async def fetch_person_credits(
        self,
        person_id: int | str,
        content_type: str,
        *,
        limit: int = 200,
        client_id: str | None = None,
        access_token: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch an actor's filmography for the given content type.

        Normalizes to media dicts (like history entries) using keys 'movie' or 'show'.
        """

        segment = "movies" if content_type == "movie" else "shows"
        path = f"/people/{person_id}/{segment}"
        params: dict[str, Any] = {
            "extended": "full",
            "limit": max(1, min(int(limit or 200), 200)),
        }
        response = await self._client.get(
            path,
            headers=self._headers(client_id=client_id, access_token=access_token),
            params=params,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to fetch Trakt filmography for person %s (%s): %s",
                person_id,
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
