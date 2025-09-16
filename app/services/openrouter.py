"""Integration helpers for the OpenRouter API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from ..config import Settings
from ..models import CatalogBundle
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
3. Each catalog must include 6-10 strong picks with real-world metadata.
4. Avoid repeating catalog titles across refreshes by choosing unexpected phrasing.
5. Balance comfort picks (known favorites) with 30% exploratory discoveries.
6. Provide diverse posters/backgrounds when possible and include imdb or trakt IDs when you know them.

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
          "description": "short synopsis",
          "poster": "https://...",
          "background": "https://...",
          "year": 2024,
          "imdb_id": "tt...",
          "trakt_id": 12345,
          "tmdb_id": 67890,
          "runtime_minutes": 120,
          "genres": ["genre"],
          "maturity_rating": "PG-13",
          "weight": 0.0,
          "providers": ["Netflix", "Hulu"]
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
    ) -> CatalogBundle:
        """Generate new catalogs using the configured model."""

        catalog_count = summary.get("catalog_count", self._settings.catalog_count)
        profile = summary.get("profile", {})

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
            seed=seed,
        )

        payload = {
            "model": self._settings.openrouter_model,
            "temperature": 1.1,
            "top_p": 0.9,
            "max_output_tokens": 2_500,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }

        headers = {
            "Authorization": f"Bearer {self._settings.openrouter_api_key}",
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
        return bundle
