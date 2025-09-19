"""Helper client for fetching metadata from Cinemeta-compatible add-ons."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx
from ..utils import slugify

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MetadataMatch:
    """Represents the useful fields returned from a metadata lookup."""

    id: str
    title: str
    type: str
    year: int | None = None
    poster: str | None = None
    background: str | None = None


class MetadataAddonClient:
    """Wrapper around Cinemeta-compatible catalog search endpoints."""

    _SEARCH_PATH = "/catalog/{type}/top/search={query}.json"

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        default_base_url: str | None = None,
    ) -> None:
        self._client = http_client
        self._default_base_url = self._normalize_base_url(default_base_url)
        self._semaphore = asyncio.Semaphore(8)

    @property
    def default_base_url(self) -> str | None:
        """Return the default metadata add-on URL, if configured."""

        return self._default_base_url

    async def lookup(
        self,
        title: str,
        *,
        content_type: str,
        year: int | None = None,
        base_url: str | None = None,
    ) -> MetadataMatch | None:
        """Return the best metadata match for the given title/year."""

        normalized_title = (title or "").strip()
        if not normalized_title:
            return None

        override_base_url = self._normalize_base_url(base_url)
        effective_base = override_base_url or self._default_base_url
        if not effective_base:
            return None

        path = self._SEARCH_PATH.format(
            type=content_type,
            query=quote(normalized_title, safe=""),
        )
        url = f"{effective_base}{path}"

        response: httpx.Response | None = None
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                async with self._semaphore:
                    response = await self._client.get(url)
                response.raise_for_status()
                break
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response else None
                if status == 402 and attempt < max_attempts:
                    await asyncio.sleep(0.1)
                    continue
                logger.warning(
                    "Metadata add-on lookup failed for %s via %s: %s",
                    normalized_title,
                    effective_base,
                    exc,
                )
                return None
            except httpx.HTTPError as exc:
                logger.warning(
                    "Metadata add-on lookup failed for %s via %s: %s",
                    normalized_title,
                    effective_base,
                    exc,
                )
                return None
        else:
            return None

        payload = response.json()
        metas = payload.get("metas") or []
        if not isinstance(metas, list) or not metas:
            return None

        match = self._select_best_match(normalized_title, year, metas, content_type)
        if match is None:
            return None

        candidate_year = self._parse_year(match.get("releaseInfo") or match.get("year"))
        poster = self._ensure_url(match.get("poster") or match.get("thumbnail"))
        background = self._ensure_url(match.get("background") or match.get("fanart"))
        match_id = str(match.get("imdb_id") or match.get("id") or "").strip()
        if not match_id:
            return None

        return MetadataMatch(
            id=match_id,
            title=str(match.get("name") or normalized_title),
            type=str(match.get("type") or content_type),
            year=candidate_year,
            poster=poster,
            background=background,
        )

    def _select_best_match(
        self,
        title: str,
        year: int | None,
        metas: list[Any],
        content_type: str,
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = [
            meta for meta in metas if isinstance(meta, dict)
        ]
        if not candidates:
            return None

        target_slug = slugify(title)

        def candidate_year(meta: dict[str, Any]) -> int | None:
            return self._parse_year(meta.get("releaseInfo") or meta.get("year"))

        exact_year_matches = [
            meta
            for meta in candidates
            if slugify(str(meta.get("name") or "")) == target_slug
            and year is not None
            and candidate_year(meta) == year
        ]
        if exact_year_matches:
            return exact_year_matches[0]

        exact_title_matches = [
            meta
            for meta in candidates
            if slugify(str(meta.get("name") or "")) == target_slug
        ]
        if year is not None and exact_title_matches:
            exact_title_matches.sort(
                key=lambda meta: self._year_delta(candidate_year(meta), year)
            )
            return exact_title_matches[0]

        if year is not None:
            scored = sorted(
                candidates,
                key=lambda meta: self._year_delta(candidate_year(meta), year),
            )
            best = scored[0]
            if candidate_year(best) is not None:
                return best

        if exact_title_matches:
            return exact_title_matches[0]

        return candidates[0]

    @staticmethod
    def _year_delta(candidate: int | None, target: int) -> int:
        if candidate is None:
            return 1_000
        return abs(candidate - target)

    @staticmethod
    def _parse_year(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if not value:
            return None
        text = str(value)
        match = re.search(r"(19|20|21)\d{2}", text)
        if not match:
            return None
        try:
            year = int(match.group(0))
        except ValueError:
            return None
        if 1900 <= year <= 2100:
            return year
        return None

    @staticmethod
    def _ensure_url(value: Any) -> str | None:
        if isinstance(value, str) and value.startswith("http"):
            return value
        return None

    @staticmethod
    def _normalize_base_url(value: str | None) -> str | None:
        if not value:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        normalized = normalized.rstrip("/")
        lowered = normalized.lower()
        for suffix in ("/manifest.json", "/manifest"):
            if lowered.endswith(suffix):
                normalized = normalized[: -len(suffix)].rstrip("/")
                break
        return normalized or None
