"""Integration helpers for the OpenRouter API."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from pydantic import ValidationError

from ..config import Settings
from ..models import Catalog, CatalogBundle, CatalogItem
from ..utils import extract_json_object, slugify

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are AIOPicks, an AI that curates playful but trustworthy movie and series catalogs "
    "for the Stremio media center. You always respond with a single JSON object that matches "
    "the documented schema and never include commentary outside JSON."
)

USER_PROMPT_TEMPLATE = """
You are helping a power user discover new titles based on their Trakt history.

Trakt profile summary (generated at {generated_at} UTC):
- Total movies logged: {movie_total}
- Total series logged: {series_total}
- Movie taste snapshot: top genres {movie_genres}; top languages {movie_languages}
- Series taste snapshot: top genres {series_genres}; top languages {series_languages}
- Recently watched movies: {recent_movies}
- Recently watched series: {recent_series}

Instructions:
1. Generate {catalog_count} movie catalogs AND {catalog_count} series catalogs.
2. Use the random seed `{seed}` to introduce surprise—shuffle sequencing, bend genres, or surface overlooked regional scenes that still feel earned for this viewer.
3. Each catalog must include EXACTLY {items_per_catalog} strong picks with real titles and release years.
4. Keep each description to a single crisp sentence (max ~16 words) to conserve tokens, even when {items_per_catalog} is large.
5. Craft each catalog title as a personal nod to this viewer—reference their standout genres, languages, or specific films/series they love while signalling the fresh angle you’re taking on their taste.
6. Every title must include at least one concrete hook from their history (a cited title, city, language, character, actor, etc.) and steer clear of vague buzzwords or generic marketing phrases such as "action-packed", "epic journey", "gems", "discover", "explore", "must-watch", "binge", "series to dive into", or similar filler.
7. Keep the titles confident and conversational without using tentative phrasing like "you may" or "you might". Vary the structure so no two titles recycle the same cadence, and never begin a title with "Your" or another possessive pronoun.
8. Write titles in natural sentence case (capitalize the first word and proper nouns only), keeping the rest lower-case unless grammar requires otherwise.
9. Center each catalog on a vivid, specific micro-theme or narrative that ties back to their logged tastes while pushing them toward unexpected adjacent discoveries. Avoid broad buckets like "action adventures" or "fantasy journeys".
10. Treat the viewer's history as inspiration, not a shopping list—avoid repeating titles mentioned above unless a sequel or continuation is essential, and spotlight why each new pick connects to their tastes.
11. Lean into discovery: ensure at least 60% of every catalog consists of fresh-to-viewer surprises rather than comfort rewatches.
12. For each item include only its real title, type, release year, and a concise description. Do not invent IDs, posters, or runtimes—the server enriches entries with the configured metadata add-on (for example, Cinemeta).

Respond with JSON using this structure:
{{
  "movie_catalogs": [
    {{
      "id": "string",
      "title": "string",
      "description": "string",
      "seed": "{seed}",
      "items": [
        {{
          "name": "Movie title",
          "type": "movie",
          "year": 2024,
          "description": "short synopsis"
        }}
      ]
    }}
  ],
  "series_catalogs": [
    {{ ... same fields but type "series" ... }}
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
    ) -> CatalogBundle:
        """Generate new catalogs using the configured model."""

        catalog_count = summary.get("catalog_count", self._settings.catalog_count)
        item_target = summary.get(
            "catalog_item_count", self._settings.catalog_item_count
        )
        profile = summary.get("profile", {})

        resolved_model = model or self._settings.openrouter_model
        resolved_key = api_key or self._settings.openrouter_api_key
        if not resolved_key:
            raise RuntimeError("OpenRouter API key is required to generate catalogs")

        prompt = USER_PROMPT_TEMPLATE.format(
            generated_at=summary.get("generated_at"),
            movie_total=profile.get("movies", {}).get("total"),
            series_total=profile.get("series", {}).get("total"),
            movie_genres=profile.get("movies", {}).get("top_genres"),
            series_genres=profile.get("series", {}).get("top_genres"),
            movie_languages=profile.get("movies", {}).get("top_languages"),
            series_languages=profile.get("series", {}).get("top_languages"),
            recent_movies=profile.get("movies", {}).get("top_titles"),
            recent_series=profile.get("series", {}).get("top_titles"),
            catalog_count=catalog_count,
            items_per_catalog=item_target,
            seed=seed,
        )

        payload = {
            "model": resolved_model,
            "temperature": 1.1,
            "top_p": 0.9,
            "max_output_tokens": self._estimate_initial_token_budget(
                catalog_count, item_target
            ),
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }

        headers = {
            "Authorization": f"Bearer {resolved_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/aiopicks/aiopicks",
            "X-Title": "AIOPicks Python",
        }

        response = await self._client.post("/chat/completions", json=payload, headers=headers)
        if response.status_code >= 400:
            logger.error("OpenRouter request failed: %s", response.text)
            raise RuntimeError("OpenRouter request failed")

        data = response.json()
        choices = data.get("choices", [])
        if not choices:
            raise RuntimeError("Model returned no choices")
        message = choices[0].get("message", {})
        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError("Model response missing content")

        parsed = extract_json_object(content)
        exclusion_map = self._normalise_exclusions(exclusions)
        bundle = CatalogBundle.from_ai_response(parsed, seed=seed)
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
        )
        return bundle

    def _estimate_initial_token_budget(
        self, catalog_count: int, item_target: int
    ) -> int:
        """Estimate a generous token budget for the primary catalog request."""

        try:
            catalogs = max(int(catalog_count), 1)
        except (TypeError, ValueError):
            catalogs = 1
        try:
            items = max(int(item_target), 1)
        except (TypeError, ValueError):
            items = 1
        total_items = catalogs * items * 2  # movie + series catalogs
        estimated = 900 + total_items * 18
        return max(2_500, min(48_000, estimated))

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
    ) -> None:
        """Ensure every catalog reaches the configured item target."""

        async def _fill(content_type: str, catalogs: list[Catalog]) -> None:
            attempts = 0
            content_exclusions = (exclusions or {}).get(content_type)
            requests = self._prepare_top_up_requests(
                catalogs,
                item_limit,
                exclusions=content_exclusions,
            )
            while requests and attempts < 3:
                additions = await self._top_up_catalogs(
                    summary,
                    seed=seed,
                    content_type=content_type,
                    requests=requests,
                    item_limit=item_limit,
                    api_key=api_key,
                    model=model,
                    exclusions=content_exclusions,
                )
                if not additions:
                    break
                self._merge_additions(
                    catalogs,
                    additions,
                    exclusions=content_exclusions,
                )
                requests = self._prepare_top_up_requests(
                    catalogs,
                    item_limit,
                    exclusions=content_exclusions,
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
    ) -> dict[str, dict[str, Any]]:
        """Normalise catalog items and describe missing counts for top-ups."""

        requests: dict[str, dict[str, Any]] = {}
        for catalog in catalogs:
            cleaned, summaries, missing = self._normalise_catalog(
                catalog,
                item_limit=item_limit,
                exclusions=exclusions,
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
                existing.add(key)
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
        ]
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
        avoided_titles: list[str] = []
        excluded: set[str] = set()
        if exclusions:
            titles = [
                str(title)
                for title in exclusions.get("titles", [])
                if isinstance(title, str) and title
            ]
            avoided_titles = titles[:12]
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

    def _normalise_catalog(
        self,
        catalog: Catalog,
        *,
        item_limit: int,
        exclusions: dict[str, Any] | None = None,
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
            seen.add(key)
            cleaned.append(item)
            summaries.append(self._summarise_item(item))
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
            for title in payload.get("recent_titles", []) or []:
                if isinstance(title, str) and title:
                    titles.append(title)
            if fingerprints or titles:
                normalised[content_type] = {
                    "fingerprints": fingerprints,
                    "titles": titles[:24],
                }
        return normalised

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
