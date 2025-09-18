"""Integration helpers for the OpenRouter API."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from pydantic import ValidationError

from ..config import Settings
from ..models import Catalog, CatalogBundle, CatalogItem
from ..utils import extract_json_object

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
2. Use the random seed `{seed}` to introduce surprise (shuffle titles, invent novel themes).
3. Each catalog must include EXACTLY {items_per_catalog} strong picks with real titles and release years.
4. Keep each description to a single crisp sentence (max ~16 words) to conserve tokens, even when {items_per_catalog} is large.
5. Avoid repeating catalog titles across refreshes by choosing unexpected phrasing.
6. Balance comfort picks (known favorites) with 30% exploratory discoveries.
7. For each item include only its real title, type, release year, and a concise description. Do not invent IDs, posters, or runtimes—the server enriches entries with Cinemeta.

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
        bundle = CatalogBundle.from_ai_response(parsed, seed=seed)
        if bundle.is_empty():
            raise RuntimeError("Model returned an empty catalog bundle")
        await self._ensure_item_targets(
            summary,
            seed=seed,
            bundle=bundle,
            item_limit=item_target,
            api_key=resolved_key,
            model=resolved_model,
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
    ) -> None:
        """Ensure every catalog reaches the configured item target."""

        async def _fill(content_type: str, catalogs: list[Catalog]) -> None:
            attempts = 0
            requests = self._prepare_top_up_requests(catalogs, item_limit)
            while requests and attempts < 3:
                additions = await self._top_up_catalogs(
                    summary,
                    seed=seed,
                    content_type=content_type,
                    requests=requests,
                    item_limit=item_limit,
                    api_key=api_key,
                    model=model,
                )
                if not additions:
                    break
                self._merge_additions(catalogs, additions)
                requests = self._prepare_top_up_requests(catalogs, item_limit)
                attempts += 1
            if requests:
                logger.warning(
                    "Model did not reach %s items for %s catalogs: %s",
                    item_limit,
                    content_type,
                    ", ".join(sorted(requests.keys())),
                )

        await _fill("movie", bundle.movie_catalogs)
        await _fill("series", bundle.series_catalogs)

    def _prepare_top_up_requests(
        self, catalogs: list[Catalog], item_limit: int
    ) -> dict[str, dict[str, Any]]:
        """Normalise catalog items and describe missing counts for top-ups."""

        requests: dict[str, dict[str, Any]] = {}
        for catalog in catalogs:
            cleaned, summaries, missing = self._normalise_catalog(
                catalog, item_limit=item_limit
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
        self, catalogs: list[Catalog], additions: dict[str, list[CatalogItem]]
    ) -> None:
        """Append new items to catalogs, avoiding duplicates."""

        catalog_map = {catalog.id: catalog for catalog in catalogs}
        for catalog_id, items in additions.items():
            catalog = catalog_map.get(catalog_id)
            if catalog is None or not items:
                continue
            existing = {self._catalog_item_key(item) for item in catalog.items}
            for item in items:
                key = self._catalog_item_key(item)
                if key in existing:
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
                collected.append(item)
                if len(collected) >= needed:
                    break
            if collected:
                additions[catalog_id] = collected
        return additions

    def _normalise_catalog(
        self, catalog: Catalog, *, item_limit: int
    ) -> tuple[list[CatalogItem], list[str], int]:
        """Remove duplicates and enforce item limits for a catalog."""

        cleaned: list[CatalogItem] = []
        summaries: list[str] = []
        seen: set[tuple[str, str, int | None]] = set()
        for item in catalog.items:
            title = (item.title or "").strip()
            if not title:
                continue
            key = self._catalog_item_key(item)
            if key in seen:
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
