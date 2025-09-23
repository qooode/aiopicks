from __future__ import annotations

import asyncio
from datetime import datetime
from typing import cast

from app.config import Settings
from app.models import Catalog, CatalogBundle, CatalogItem
from app.services.openrouter import OpenRouterClient


class _DummyAsyncClient:
    async def post(self, *args, **kwargs):  # pragma: no cover - not used in tests
        raise AssertionError("Network access should not be triggered during tests")


def _make_client() -> OpenRouterClient:
    settings = Settings(_env_file=None)
    return OpenRouterClient(settings, cast(object, _DummyAsyncClient()))  # type: ignore[arg-type]


def test_openrouter_apply_exclusions_trims_items() -> None:
    """Catalog items matching watched fingerprints are removed up front."""

    client = _make_client()
    now = datetime.utcnow()
    watched = {"movie": {"fingerprints": {"movie:imdb:tt1234567"}, "titles": []}}
    bundle = CatalogBundle(
        movie_catalogs=[
            Catalog(
                id="aiopicks-movie-demo",
                type="movie",
                title="Demo",
                description=None,
                seed="seed",
                items=[
                    CatalogItem(
                        title="Seen Film",
                        type="movie",
                        imdb_id="tt1234567",
                        year=2020,
                    ),
                    CatalogItem(
                        title="Fresh Film",
                        type="movie",
                        imdb_id="tt7654321",
                        year=2021,
                    ),
                ],
                generated_at=now,
            )
        ],
        series_catalogs=[],
    )

    client._apply_exclusions(bundle, watched)

    catalog = bundle.movie_catalogs[0]
    assert len(catalog.items) == 1
    assert catalog.items[0].title == "Fresh Film"


def test_normalise_catalog_skips_excluded_items() -> None:
    """Normalisation drops watched items and reports missing slots."""

    client = _make_client()
    now = datetime.utcnow()
    catalog = Catalog(
        id="aiopicks-movie-demo",
        type="movie",
        title="Demo",
        description=None,
        seed="seed",
        items=[
            CatalogItem(
                title="Seen Film",
                type="movie",
                imdb_id="tt1234567",
                year=2020,
            ),
            CatalogItem(
                title="Fresh Film",
                type="movie",
                imdb_id="tt7654321",
                year=2021,
            ),
        ],
        generated_at=now,
    )

    cleaned, summaries, missing = client._normalise_catalog(
        catalog,
        item_limit=2,
        exclusions={"fingerprints": {"movie:imdb:tt1234567"}},
    )

    assert [item.title for item in cleaned] == ["Fresh Film"]
    assert summaries == ["Fresh Film (2021)"]
    assert missing == 1


def test_render_exclusion_titles_prefers_recent_titles() -> None:
    """Readable titles are returned when provided in exclusion payloads."""

    client = _make_client()
    exclusions = {
        "fingerprints": {"movie:title:some film:2020"},
        "titles": ["Seen Film (2020)", "  Extra Spaces  "],
    }

    titles = client._render_exclusion_titles(exclusions, limit=5)

    assert titles == ["Seen Film (2020)", "Extra Spaces"]


def test_render_exclusion_titles_falls_back_to_fingerprints() -> None:
    """Fingerprints with title metadata are converted into readable labels."""

    client = _make_client()
    exclusions = {
        "fingerprints": {
            "movie:title:another film:2021",
            "series:slug:some-show",
            "movie:title:another film:2021",  # duplicates ignored
        },
        "titles": [],
    }

    titles = client._render_exclusion_titles(exclusions, limit=5)

    assert titles == ["Another Film (2021)", "Some Show"]


def test_normalise_exclusions_builds_fingerprints_from_recent_titles() -> None:
    """Recent titles populate fallback fingerprints for duplicate blocking."""

    client = _make_client()
    exclusions = {
        "movie": {
            "recent_titles": ["Seen Film (2020)", " Stripped Title "]
        },
        "series": {
            "recent_titles": ["Known Show"]
        },
    }

    normalised = client._normalise_exclusions(exclusions)

    movie_fps = normalised["movie"]["fingerprints"]
    assert "movie:title:seen film" in movie_fps
    assert "movie:title:seen film:2020" in movie_fps
    assert "movie:slug:seen-film" in movie_fps
    assert "movie:slug:seen-film:2020" in movie_fps
    assert "movie:title:stripped title" in movie_fps

    series_fps = normalised["series"]["fingerprints"]
    assert "series:title:known show" in series_fps
    assert "series:slug:known-show" in series_fps


def test_ensure_item_targets_retries_when_additions_drop_out() -> None:
    """Empty top-up batches still consume attempts and trigger another request."""

    class _TrackingOpenRouterClient(OpenRouterClient):
        def __init__(self, responses: list[dict[str, list[CatalogItem]]]):
            super().__init__(Settings(_env_file=None), cast(object, _DummyAsyncClient()))  # type: ignore[arg-type]
            self._responses = list(responses)
            self.attempts: list[int] = []

        async def _top_up_catalogs(  # type: ignore[override]
            self,
            summary: dict[str, object],
            *,
            seed: str,
            content_type: str,
            requests: dict[str, dict[str, object]],
            item_limit: int,
            api_key: str,
            model: str,
            exclusions: dict[str, object] | None = None,
            attempt: int = 0,
            attempt_limit: int = 1,
        ) -> dict[str, list[CatalogItem]]:
            self.attempts.append(attempt)
            if self._responses:
                return self._responses.pop(0)
            return {}

    now = datetime.utcnow()
    catalog = Catalog(
        id="aiopicks-movie-demo",
        type="movie",
        title="Demo",
        description=None,
        seed="seed",
        items=[
            CatalogItem(title="Seen Film", type="movie", year=2020),
        ],
        generated_at=now,
    )
    bundle = CatalogBundle(movie_catalogs=[catalog], series_catalogs=[])

    responses = [
        {catalog.id: [CatalogItem(title="Seen Film", type="movie", year=2020)]},
        {catalog.id: [CatalogItem(title="Fresh Film", type="movie", year=2021)]},
    ]
    client = _TrackingOpenRouterClient(responses)

    async def runner() -> None:
        await client._ensure_item_targets(
            {},
            seed="seed",
            bundle=bundle,
            item_limit=2,
            api_key="test-key",
            model="test-model",
            exclusions=None,
            max_attempts=3,
        )

    asyncio.run(runner())

    assert [item.title for item in catalog.items] == ["Seen Film", "Fresh Film"]
    assert client.attempts == [0, 1]
