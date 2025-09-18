"""Utilities for resolving metadata from The Movie Database (TMDB)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from ..config import Settings
from ..models import CatalogItem

logger = logging.getLogger(__name__)

POSTER_BASE_URL = "https://image.tmdb.org/t/p/w500"
BACKDROP_BASE_URL = "https://image.tmdb.org/t/p/w780"


@dataclass(slots=True)
class TMDBSearchResult:
    """Normalized view of a TMDB search result."""

    tmdb_id: int
    title: str
    overview: str | None
    poster_path: str | None
    backdrop_path: str | None
    year: int | None


class TMDBClient:
    """Client responsible for searching TMDB for media identifiers."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient):
        if not settings.tmdb_api_key:
            raise ValueError("TMDB API key is required when initialising TMDBClient")
        self._settings = settings
        self._client = http_client

    async def enrich_item(self, item: CatalogItem) -> CatalogItem:
        """Fill in missing identifiers and artwork for a catalog item."""

        if item.tmdb_id and (item.imdb_id or item.trakt_id):
            # Item already has reliable identifiers; no need to hit TMDB.
            return item
        if not item.title:
            return item

        try:
            result = await self._search(item.title, content_type=item.type, year=item.year)
        except Exception:  # pragma: no cover - defensive logging branch
            logger.exception("TMDB search failed for %s", item.title)
            return item

        if result is None:
            return item

        update: dict[str, Any] = {}
        if not item.tmdb_id:
            update["tmdb_id"] = result.tmdb_id
        if not item.overview and result.overview:
            update["overview"] = result.overview
        if not item.poster and result.poster_path:
            update["poster"] = self._build_image_url(result.poster_path, POSTER_BASE_URL)
        if not item.background and result.backdrop_path:
            update["background"] = self._build_image_url(result.backdrop_path, BACKDROP_BASE_URL)

        # If we still lack any external identifiers try fetching more detailed info.
        if (item.imdb_id is None or item.trakt_id is None) and update.get("tmdb_id"):
            details = await self._fetch_external_ids(result.tmdb_id, item.type)
            if details:
                if not item.imdb_id and details.get("imdb_id"):
                    update["imdb_id"] = details["imdb_id"]
                if not item.trakt_id and details.get("trakt_id"):
                    update["trakt_id"] = details["trakt_id"]

        if not update:
            return item

        return item.model_copy(update=update)

    async def _search(
        self, title: str, *, content_type: str, year: int | None
    ) -> TMDBSearchResult | None:
        """Return the best search match for the supplied title."""

        endpoint = "/search/movie" if content_type == "movie" else "/search/tv"
        params = {
            "query": title,
            "include_adult": "false",
            "language": "en-US",
            "page": 1,
            "api_key": self._settings.tmdb_api_key,
        }
        if year:
            if content_type == "movie":
                params["year"] = year
            else:
                params["first_air_date_year"] = year

        response = await self._client.get(endpoint, params=params)
        if response.status_code >= 400:
            logger.warning(
                "TMDB search for %s (%s) failed: %s", title, content_type, response.text
            )
            return None
        data = response.json()
        results = data.get("results", [])
        if not results:
            return None

        normalized_title = title.casefold()
        best_match: dict[str, Any] | None = None

        for candidate in results:
            candidate_title = candidate.get("title") or candidate.get("name")
            if not candidate_title:
                continue
            candidate_year = self._extract_year(candidate, content_type)
            if candidate_title.casefold() == normalized_title:
                if year is None or candidate_year == year:
                    best_match = candidate
                    break
            if best_match is None:
                best_match = candidate
            elif year is not None and candidate_year == year:
                best_match = candidate

        if not best_match:
            return None

        return TMDBSearchResult(
            tmdb_id=int(best_match["id"]),
            title=best_match.get("title")
            or best_match.get("name")
            or title,
            overview=best_match.get("overview"),
            poster_path=best_match.get("poster_path"),
            backdrop_path=best_match.get("backdrop_path"),
            year=self._extract_year(best_match, content_type),
        )

    async def _fetch_external_ids(
        self, tmdb_id: int, content_type: str
    ) -> dict[str, Any] | None:
        """Fetch external IDs for a TMDB entity."""

        endpoint = f"/{'movie' if content_type == 'movie' else 'tv'}/{tmdb_id}"
        params = {
            "api_key": self._settings.tmdb_api_key,
            "append_to_response": "external_ids",
        }
        response = await self._client.get(endpoint, params=params)
        if response.status_code >= 400:
            logger.debug(
                "TMDB external id fetch failed for %s: %s", tmdb_id, response.text
            )
            return None
        payload = response.json()
        external = payload.get("external_ids", {})
        data = {
            "imdb_id": payload.get("imdb_id") or external.get("imdb_id"),
            "trakt_id": external.get("trakt_id"),
        }
        return {key: value for key, value in data.items() if value}

    @staticmethod
    def _extract_year(result: dict[str, Any], content_type: str) -> int | None:
        date_key = "release_date" if content_type == "movie" else "first_air_date"
        date_value = result.get(date_key)
        if not isinstance(date_value, str) or len(date_value) < 4:
            return None
        try:
            return int(date_value[:4])
        except ValueError:
            return None

    @staticmethod
    def _build_image_url(path: str, base_url: str) -> str:
        if not path:
            return ""
        if path.startswith("http"):
            return path
        return f"{base_url}{path}"
