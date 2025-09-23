"""Integration helpers for the OpenRouter API."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any

import httpx
from pydantic import ValidationError

from ..config import Settings
from ..models import Catalog, CatalogBundle, CatalogItem
from ..stable_catalogs import STABLE_CATALOGS, StableCatalogDefinition
from ..utils import extract_json_object, slugify

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are AIOPicks, an AI that curates playful but trustworthy movie and series catalogs "
    "for the Stremio media center. You always respond with a single JSON object that matches "
    "the documented schema and never include commentary outside JSON."
)

CATALOG_REQUEST_TEMPLATE = """
You are the trusted cinephile friend helping a power user discover new titles based on their Trakt history.

Trakt insight snapshot (generated at {generated_at} UTC):
- Lifetime footprint: {lifetime_summary}
- Movie taste signals: {movie_taste_summary}
- Recent movie standouts (avoid repeats unless a sequel/continuation is vital): {recent_movies}
- Series taste signals: {series_taste_summary}
- Recent series standouts (avoid repeats unless a sequel/continuation is vital): {recent_series}

This request focuses on the "{title}" lane:
- Intent: {description}
- Content type: {content_label}
- Random seed: {seed}
- Creative brief: Use the seed to explore a fresh corner of their taste—lean into unexpected yet fitting picks.

Rules:
1. Recommend EXACTLY {item_target} {content_label_plural} that match the lane intent and feel fresh to the viewer.
2. Only include titles with an IMDb rating of 7.0 or higher.
3. Skip anything already logged or completed. Known fingerprints to dodge: {avoid_list}
4. Keep every description to one crisp sentence (about 16 words) explaining why it fits the lane.
5. Provide real release years and stay grounded in genuine productions.
6. Set "type" to "{content_type}" for every item.
7. Make this lineup distinct from other seeds and lanes—avoid obvious staples unless the seed demands it.
8. Spotlight overlooked, international, or conversation-sparking choices that still align tightly with the lane brief.

Respond strictly with JSON following this structure:
{{
  "items": [
    {{
      "title": "Title",
      "type": "{content_type}",
      "year": 2024,
      "description": "short sentence"
    }}
  ]
}}
"""


class OpenRouterClient:
    """Client responsible for talking to OpenRouter."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient):
        self._settings = settings
        self._client = http_client

    async def generate_catalogs(
        self,
        summary: dict[str, Any],
        *,
        seed: str,
        api_key: str | None = None,
        model: str | None = None,
        exclusions: dict[str, dict[str, Any]] | None = None,
        retry_limit: int | None = None,
    ) -> CatalogBundle:
        """Generate new catalogs using the configured model."""

        item_target = summary.get(
            "catalog_item_count", self._settings.catalog_item_count
        )
        resolved_model = model or self._settings.openrouter_model
        resolved_key = api_key or self._settings.openrouter_api_key
        if not resolved_key:
            raise RuntimeError("OpenRouter API key is required to generate catalogs")

        exclusion_map = self._normalise_exclusions(exclusions)
        if retry_limit is None:
            resolved_retry_limit = self._settings.generation_retry_limit
        else:
            try:
                resolved_retry_limit = int(retry_limit)
            except (TypeError, ValueError):
                resolved_retry_limit = self._settings.generation_retry_limit
        resolved_retry_limit = max(0, min(resolved_retry_limit, 10))
        tasks = [
            asyncio.create_task(
                self._generate_catalog_for_definition(
                    summary,
                    definition,
                    item_target=item_target,
                    seed=f"{seed}-{index:02d}",
                    api_key=resolved_key,
                    model=resolved_model,
                    exclusions=(exclusion_map or {}).get(definition.content_type),
                )
            )
            for index, definition in enumerate(STABLE_CATALOGS)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        movie_catalogs: list[Catalog] = []
        series_catalogs: list[Catalog] = []

        for definition, result in zip(STABLE_CATALOGS, results):
            if isinstance(result, Exception):
                logger.warning(
                    "Catalog generation failed for %s lane: %s",
                    definition.key,
                    result,
                )
                continue
            if result is None:
                continue
            if definition.content_type == "movie":
                movie_catalogs.append(result)
            else:
                series_catalogs.append(result)

        bundle = CatalogBundle(
            movie_catalogs=movie_catalogs, series_catalogs=series_catalogs
        )

        if exclusion_map:
            self._apply_exclusions(bundle, exclusion_map)
        if bundle.is_empty():
            raise RuntimeError("Model returned an empty catalog bundle")
        await self._ensure_item_targets(
            summary,
            seed=seed,
            bundle=bundle,
            item_limit=item_target,
            api_key=resolved_key,
            model=resolved_model,
            exclusions=exclusion_map,
            max_attempts=resolved_retry_limit,
        )
        self._enforce_session_uniqueness(bundle, exclusion_map)
        return bundle

    async def _generate_catalog_for_definition(
        self,
        summary: dict[str, Any],
        definition: StableCatalogDefinition,
        *,
        item_target: int,
        seed: str,
        api_key: str,
        model: str,
        exclusions: dict[str, Any] | None = None,
    ) -> Catalog | None:
        """Request catalog items for a single stable lane."""

        prompt = self._build_definition_prompt(
            summary,
            definition=definition,
            item_target=item_target,
            seed=seed,
            exclusions=exclusions,
        )

        payload = {
            "model": model,
            "temperature": 0.95,
            "top_p": 0.95,
            "max_output_tokens": self._estimate_definition_token_budget(item_target),
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/aiopicks/aiopicks",
            "X-Title": "AIOPicks Python",
        }

        response = await self._client.post("/chat/completions", json=payload, headers=headers)
        if response.status_code >= 400:
            raise RuntimeError(response.text)

        data = response.json()
        choices = data.get("choices", [])
        if not choices:
            raise RuntimeError("Model returned no choices")
        message = choices[0].get("message", {})
        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError("Model response missing content")

        parsed = extract_json_object(content)
        raw_items: list[dict[str, Any]] = []
        if isinstance(parsed, dict):
            candidate = parsed.get("items")
            if isinstance(candidate, list):
                raw_items = [entry for entry in candidate if isinstance(entry, dict)]
        elif isinstance(parsed, list):
            raw_items = [entry for entry in parsed if isinstance(entry, dict)]

        items: list[CatalogItem] = []
        for entry in raw_items:
            payload = {**entry, "type": definition.content_type}
            try:
                item = CatalogItem.model_validate(payload)
            except ValidationError:
                continue
            items.append(item)

        return Catalog(
            id=f"aiopicks-{definition.content_type}-{definition.key}",
            type=definition.content_type,
            title=definition.title,
            description=definition.description,
            seed=seed,
            items=items,
            generated_at=datetime.utcnow(),
        )

    def _build_definition_prompt(
        self,
        summary: dict[str, Any],
        *,
        definition: StableCatalogDefinition,
        item_target: int,
        seed: str,
        exclusions: dict[str, Any] | None = None,
    ) -> str:
        profile = summary.get("profile", {}) or {}
        movie_profile = profile.get("movies", {}) or {}
        series_profile = profile.get("series", {}) or {}

        content_label = "movie" if definition.content_type == "movie" else "series"
        content_label_plural = "movies" if content_label == "movie" else "series"

        avoid_titles = self._render_exclusion_titles(exclusions, limit=28)
        if avoid_titles:
            avoid_list = "; ".join(avoid_titles)
        else:
            avoid_list = "none supplied—use the history context to stay fresh."

        return CATALOG_REQUEST_TEMPLATE.format(
            generated_at=summary.get("generated_at", datetime.utcnow().isoformat()),
            lifetime_summary=summary.get(
                "lifetime_summary", "Lifetime stats unavailable."
            ),
            movie_taste_summary=movie_profile.get(
                "taste_summary", "No strong movie signals captured yet."
            ),
            series_taste_summary=series_profile.get(
                "taste_summary", "No strong series signals captured yet."
            ),
            recent_movies=movie_profile.get(
                "recent_highlights", "No recent standouts captured."
            ),
            recent_series=series_profile.get(
                "recent_highlights", "No recent standouts captured."
            ),
            title=definition.title,
            description=definition.description,
            content_label=content_label,
            content_label_plural=content_label_plural,
            content_type=definition.content_type,
            item_target=item_target,
            seed=seed,
            avoid_list=avoid_list,
        )

    def _estimate_definition_token_budget(self, item_target: int) -> int:
        """Estimate a token budget for a single catalog lane."""

        try:
            items = max(int(item_target), 1)
        except (TypeError, ValueError):
            items = 1
        estimated = 900 + items * 20
        return max(2_000, min(12_000, estimated))

    def _estimate_top_up_token_budget(self, total_missing: int) -> int:
        """Estimate token budget for targeted top-up prompts."""

        try:
            missing = max(int(total_missing), 1)
        except (TypeError, ValueError):
            missing = 1
        estimated = 600 + missing * 22
        return max(1_500, min(24_000, estimated))

    async def _ensure_item_targets(
        self,
        summary: dict[str, Any],
        *,
        seed: str,
        bundle: CatalogBundle,
        item_limit: int,
        api_key: str,
        model: str,
        exclusions: dict[str, dict[str, Any]] | None = None,
        max_attempts: int = 3,
    ) -> None:
        """Ensure every catalog reaches the configured item target."""

        async def _fill(content_type: str, catalogs: list[Catalog]) -> None:
            attempts = 0
            content_exclusions = (exclusions or {}).get(content_type)
            session_seen = self._build_session_seen(catalogs)
            requests = self._prepare_top_up_requests(
                catalogs,
                item_limit,
                exclusions=content_exclusions,
                session_seen=session_seen,
            )
            attempt_limit = max(0, max_attempts)
            if attempt_limit <= 0:
                return
            while requests and attempts < attempt_limit:
                additions = await self._top_up_catalogs(
                    summary,
                    seed=seed,
                    content_type=content_type,
                    requests=requests,
                    item_limit=item_limit,
                    api_key=api_key,
                    model=model,
                    exclusions=content_exclusions,
                    attempt=attempts,
                    attempt_limit=attempt_limit,
                )
                if additions:
                    self._merge_additions(
                        catalogs,
                        additions,
                        exclusions=content_exclusions,
                        session_seen=session_seen,
                    )
                    requests = self._prepare_top_up_requests(
                        catalogs,
                        item_limit,
                        exclusions=content_exclusions,
                        session_seen=session_seen,
                    )
                attempts += 1
            if requests:
                logger.warning(
                    "Model did not reach %s items for %s catalogs: %s",
                    item_limit,
                    content_type,
                    ", ".join(sorted(requests.keys())),
                )

        await asyncio.gather(
            _fill("movie", bundle.movie_catalogs),
            _fill("series", bundle.series_catalogs),
        )

    def _prepare_top_up_requests(
        self,
        catalogs: list[Catalog],
        item_limit: int,
        *,
        exclusions: dict[str, Any] | None = None,
        session_seen: dict[tuple[str, str, int | None], str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Normalise catalog items and describe missing counts for top-ups."""

        requests: dict[str, dict[str, Any]] = {}
        for catalog in catalogs:
            cleaned, summaries, missing = self._normalise_catalog(
                catalog,
                item_limit=item_limit,
                exclusions=exclusions,
                session_seen=session_seen,
            )
            if catalog.items != cleaned:
                catalog.items = cleaned
            if missing > 0:
                requests[catalog.id] = {
                    "catalog": catalog,
                    "missing": missing,
                    "summaries": summaries,
                }
        return requests

    def _merge_additions(
        self,
        catalogs: list[Catalog],
        additions: dict[str, list[CatalogItem]],
        *,
        exclusions: dict[str, Any] | None = None,
        session_seen: dict[tuple[str, str, int | None], str] | None = None,
    ) -> None:
        """Append new items to catalogs, avoiding duplicates."""

        catalog_map = {catalog.id: catalog for catalog in catalogs}
        excluded: set[str] = set()
        if exclusions:
            excluded = set(exclusions.get("fingerprints", set()))
        for catalog_id, items in additions.items():
            catalog = catalog_map.get(catalog_id)
            if catalog is None or not items:
                continue
            existing = {self._catalog_item_key(item) for item in catalog.items}
            for item in items:
                key = self._catalog_item_key(item)
                if key in existing:
                    continue
                if excluded and self._is_excluded(item, excluded):
                    continue
                if session_seen is not None:
                    owner = session_seen.get(key)
                    if owner is not None and owner != catalog_id:
                        continue
                existing.add(key)
                if session_seen is not None:
                    session_seen[key] = catalog_id
                catalog.items.append(item)

    async def _top_up_catalogs(
        self,
        summary: dict[str, Any],
        *,
        seed: str,
        content_type: str,
        requests: dict[str, dict[str, Any]],
        item_limit: int,
        api_key: str,
        model: str,
        exclusions: dict[str, Any] | None = None,
        attempt: int = 0,
        attempt_limit: int = 1,
    ) -> dict[str, list[CatalogItem]]:
        """Ask the model for additional catalog items."""

        if not requests:
            return {}

        profile = summary.get("profile", {})
        profile_snapshot = profile.get(
            "movies" if content_type == "movie" else "series", {}
        )
        prompt_lines = [
            "Continue curating {content_type} catalogs for a Stremio power user.".format(
                content_type=content_type
            ),
            "Use the random seed {seed} for inspiration.".format(seed=seed),
            (
                "Each catalog must end up with exactly {limit} unique picks. "
                "Only supply the missing items and keep every description under "
                "16 words."
            ).format(limit=item_limit),
            "Deliver left-field but still on-profile choices—no repeats from earlier suggestions in this session.",
        ]
        if attempt_limit > 0:
            prompt_lines.append(
                "Attempt {current} of {total}. Previous response left open slots—top them up without "
                "recycling anything already confirmed.".format(
                    current=attempt + 1, total=attempt_limit
                )
            )
        genres = profile_snapshot.get("top_genres")
        languages = profile_snapshot.get("top_languages")
        recent = profile_snapshot.get("top_titles")
        if genres or languages or recent:
            taste_bits = []
            if genres:
                taste_bits.append(f"genres {genres}")
            if languages:
                taste_bits.append(f"languages {languages}")
            if recent:
                taste_bits.append(f"recent favorites {recent}")
            prompt_lines.append(
                "Keep curations aligned with {content_type} taste: {details}.".format(
                    content_type=content_type, details=", ".join(taste_bits)
                )
            )

        prompt_lines.append(
            "Respond with JSON where each key is a catalog ID and the value is an "
            "array of the missing items."
        )
        avoided_titles = self._render_exclusion_titles(exclusions, limit=20)
        excluded: set[str] = set()
        if exclusions:
            excluded = set(exclusions.get("fingerprints", set()))
        if avoided_titles:
            prompt_lines.append(
                "Avoid anything they've already finished, including: "
                + "; ".join(avoided_titles)
                + "."
            )
        prompt_lines.append("Use this schema:")
        prompt_lines.append(
            "{{\n  \"{id}\": [\n    {{\n      \"name\": \"Title\",\n      \"type\": \"{ctype}\",\n      \"year\": 2024,\n      \"description\": \"short sentence\"\n    }}\n  ]\n}}".format(
                id=next(iter(requests.keys())), ctype=content_type
            )
        )

        for catalog_id, info in requests.items():
            catalog: Catalog = info["catalog"]
            summaries: list[str] = info.get("summaries", [])
            missing = info.get("missing", 0)
            prompt_lines.append("")
            prompt_lines.append(f"Catalog ID: {catalog_id}")
            prompt_lines.append(f"Title: {catalog.title}")
            if catalog.description:
                prompt_lines.append(f"Description: {catalog.description}")
            if summaries:
                prompt_lines.append(
                    "Existing picks: " + "; ".join(summaries)
                )
            else:
                prompt_lines.append("Existing picks: (none yet)")
            prompt_lines.append(
                f"Currently holding {len(summaries)} selections—need {missing} more to reach {item_limit}."
            )
            prompt_lines.append(
                f"Provide {missing} new unique {content_type} titles."
            )

        prompt = "\n".join(prompt_lines)

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/aiopicks/aiopicks",
            "X-Title": "AIOPicks Python",
        }
        total_missing = sum(info.get("missing", 0) for info in requests.values())
        payload = {
            "model": model,
            "temperature": 1.1,
            "top_p": 0.9,
            "max_output_tokens": self._estimate_top_up_token_budget(total_missing),
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }

        response = await self._client.post("/chat/completions", json=payload, headers=headers)
        if response.status_code >= 400:
            logger.error(
                "Top-up request failed (%s): %s", response.status_code, response.text
            )
            return {}

        data = response.json()
        choices = data.get("choices", [])
        if not choices:
            logger.warning("Top-up response missing choices")
            return {}
        message = choices[0].get("message", {})
        content = message.get("content")
        if not isinstance(content, str):
            logger.warning("Top-up response missing content")
            return {}

        parsed = extract_json_object(content)
        if not isinstance(parsed, dict):
            logger.warning("Top-up response was not a JSON object: %s", content)
            return {}

        additions: dict[str, list[CatalogItem]] = {}
        for catalog_id, raw_items in parsed.items():
            if catalog_id not in requests:
                continue
            if not isinstance(raw_items, list):
                continue
            needed = max(int(requests[catalog_id].get("missing", 0)), 0)
            collected: list[CatalogItem] = []
            for entry in raw_items:
                if not isinstance(entry, dict):
                    continue
                item_data = {**entry, "type": content_type}
                try:
                    item = CatalogItem.model_validate(item_data)
                except ValidationError:
                    continue
                if excluded and self._is_excluded(item, excluded):
                    continue
                collected.append(item)
                if len(collected) >= needed:
                    break
            if collected:
                additions[catalog_id] = collected
        return additions

    def _render_exclusion_titles(
        self, exclusions: dict[str, Any] | None, *, limit: int
    ) -> list[str]:
        """Render a readable list of completed titles from exclusion metadata."""

        if not exclusions or limit <= 0:
            return []

        titles: list[str] = []
        for title in exclusions.get("titles", []) or []:
            if isinstance(title, str):
                cleaned = title.strip()
                if cleaned and cleaned not in titles:
                    titles.append(cleaned)
            if len(titles) >= limit:
                return titles[:limit]

        if titles:
            return titles[:limit]

        rendered: list[str] = []
        seen: set[str] = set()
        for fingerprint in sorted(exclusions.get("fingerprints", []) or []):
            if not isinstance(fingerprint, str):
                continue
            parts = fingerprint.split(":")
            if len(parts) < 3:
                continue
            _, kind, *values = parts
            if kind not in {"title", "slug"}:
                continue
            if not values:
                continue
            name = values[0]
            if not isinstance(name, str) or not name.strip():
                continue
            if kind == "slug":
                base_name = name.replace("-", " ").replace("_", " ")
            else:
                base_name = name
            display = base_name.strip()
            if not display:
                continue
            display = display.title()
            year: str | None = None
            if len(values) >= 2:
                candidate = values[1]
                if isinstance(candidate, str) and candidate.isdigit():
                    year = candidate
            if year:
                label = f"{display} ({year})"
            else:
                label = display
            if label not in seen:
                seen.add(label)
                rendered.append(label)
            if len(rendered) >= limit:
                break
        return rendered

    def _normalise_catalog(
        self,
        catalog: Catalog,
        *,
        item_limit: int,
        exclusions: dict[str, Any] | None = None,
        session_seen: dict[tuple[str, str, int | None], str] | None = None,
    ) -> tuple[list[CatalogItem], list[str], int]:
        """Remove duplicates and enforce item limits for a catalog."""

        cleaned: list[CatalogItem] = []
        summaries: list[str] = []
        seen: set[tuple[str, str, int | None]] = set()
        excluded: set[str] = set()
        if exclusions:
            excluded = set(exclusions.get("fingerprints", set()))
        for item in catalog.items:
            title = (item.title or "").strip()
            if not title:
                continue
            key = self._catalog_item_key(item)
            if key in seen:
                continue
            if excluded and self._is_excluded(item, excluded):
                continue
            if session_seen is not None:
                owner = session_seen.get(key)
                if owner is not None and owner != catalog.id:
                    continue
            seen.add(key)
            cleaned.append(item)
            summaries.append(self._summarise_item(item))
            if session_seen is not None:
                session_seen[key] = catalog.id
        if len(cleaned) > item_limit:
            cleaned = cleaned[:item_limit]
            summaries = summaries[:item_limit]
        missing = max(item_limit - len(cleaned), 0)
        return cleaned, summaries, missing

    def _summarise_item(self, item: CatalogItem) -> str:
        title = (item.title or "").strip()
        year = item.year or "?"
        return f"{title} ({year})"

    def _catalog_item_key(
        self, item: CatalogItem
    ) -> tuple[str, str, int | None]:
        title = (item.title or "").strip().casefold()
        return (item.type, title, item.year)

    def _apply_exclusions(
        self,
        bundle: CatalogBundle,
        exclusions: dict[str, dict[str, Any]],
    ) -> None:
        for catalog in bundle.movie_catalogs:
            self._filter_catalog_items(catalog, exclusions.get("movie"))
        for catalog in bundle.series_catalogs:
            self._filter_catalog_items(catalog, exclusions.get("series"))

    def _filter_catalog_items(
        self,
        catalog: Catalog,
        exclusions: dict[str, Any] | None,
    ) -> None:
        if not exclusions:
            return
        excluded: set[str] = set(exclusions.get("fingerprints", set()))
        if not excluded:
            return
        filtered = [
            item
            for item in catalog.items
            if not self._is_excluded(item, excluded)
        ]
        if len(filtered) != len(catalog.items):
            catalog.items = filtered

    _TITLE_YEAR_RE = re.compile(r"^(?P<title>.+?)(?:\s*\((?P<year>\d{4})\))?$")

    def _normalise_exclusions(
        self, exclusions: dict[str, dict[str, Any]] | None
    ) -> dict[str, dict[str, Any]]:
        if not exclusions:
            return {}
        normalised: dict[str, dict[str, Any]] = {}
        for content_type, payload in exclusions.items():
            if content_type not in {"movie", "series"}:
                continue
            if not isinstance(payload, dict):
                continue
            fingerprints: set[str] = set()
            titles: list[str] = []
            for fp in payload.get("fingerprints", []) or []:
                if isinstance(fp, str) and fp:
                    fingerprints.add(fp)
            recent_titles = []
            for title in payload.get("recent_titles", []) or []:
                if isinstance(title, str):
                    cleaned = title.strip()
                    if cleaned:
                        recent_titles.append(cleaned)
                        titles.append(cleaned)
            if recent_titles:
                fingerprints.update(
                    self._fingerprints_from_recent_titles(
                        content_type, recent_titles
                    )
                )
            if fingerprints or titles:
                normalised[content_type] = {
                    "fingerprints": fingerprints,
                    "titles": titles[:40],
                }
        return normalised

    def _fingerprints_from_recent_titles(
        self, content_type: str, titles: list[str]
    ) -> set[str]:
        prefix = "movie" if content_type == "movie" else "series"
        fingerprints: set[str] = set()
        for entry in titles:
            match = self._TITLE_YEAR_RE.match(entry)
            if match is None:
                base_title = entry.strip()
                year: str | None = None
            else:
                base_title = (match.group("title") or "").strip()
                year = match.group("year")
            if not base_title:
                continue
            lowered = base_title.casefold()
            if lowered:
                fingerprints.add(f"{prefix}:title:{lowered}")
                if year and year.isdigit():
                    fingerprints.add(f"{prefix}:title:{lowered}:{year}")
            slug_title = slugify(lowered)
            if slug_title:
                fingerprints.add(f"{prefix}:slug:{slug_title}")
                if year and year.isdigit():
                    fingerprints.add(f"{prefix}:slug:{slug_title}:{year}")
        return fingerprints

    def _enforce_session_uniqueness(
        self,
        bundle: CatalogBundle,
        exclusions: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """Ensure the final bundle contains unique, unwatched items per content type."""

        exclusion_map = exclusions or {}

        def _clean_group(
            catalogs: list[Catalog], content_type: str
        ) -> None:
            if not catalogs:
                return
            session_seen: dict[tuple[str, str, int | None], str] = {}
            content_exclusions = exclusion_map.get(content_type)
            for catalog in catalogs:
                cleaned, _, _ = self._normalise_catalog(
                    catalog,
                    item_limit=len(catalog.items),
                    exclusions=content_exclusions,
                    session_seen=session_seen,
                )
                if catalog.items != cleaned:
                    catalog.items = cleaned

        _clean_group(bundle.movie_catalogs, "movie")
        _clean_group(bundle.series_catalogs, "series")

    def _build_session_seen(
        self, catalogs: list[Catalog]
    ) -> dict[tuple[str, str, int | None], str]:
        seen: dict[tuple[str, str, int | None], str] = {}
        for catalog in catalogs:
            for item in catalog.items:
                key = self._catalog_item_key(item)
                seen.setdefault(key, catalog.id)
        return seen

    def _is_excluded(self, item: CatalogItem, excluded: set[str]) -> bool:
        if not excluded:
            return False
        return any(fp in excluded for fp in self._item_fingerprints(item))

    def _item_fingerprints(self, item: CatalogItem) -> set[str]:
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
