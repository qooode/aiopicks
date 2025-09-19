from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import cast

import httpx

from app.config import Settings
from app.database import Database
from app.db_models import CatalogRecord, Profile
from app.models import Catalog, CatalogItem
from app.services.metadata_addon import MetadataAddonClient, MetadataMatch
from app.services.catalog_generator import CatalogService
from app.services.catalog_generator import ProfileState, ProfileStatus
from app.services.openrouter import OpenRouterClient
from app.services.trakt import TraktClient
from app.services.catalog_generator import ManifestConfig


def test_default_profile_skipped_without_api_key(tmp_path) -> None:
    """The default profile should not be created when no API key is configured."""

    async def runner() -> None:
        database_path = tmp_path / "service.db"
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        await database.create_all()

        settings = Settings(_env_file=None)
        assert settings.openrouter_api_key is None

        service = CatalogService(
            settings,
            cast(TraktClient, object()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, object()),
            database.session_factory,
        )

        await service._ensure_default_profile()
        state = await service._load_profile_state("default")

        assert state is None

        await database.dispose()

    asyncio.run(runner())


def test_profile_status_payload_flags() -> None:
    """Ready flag reflects catalog availability and refresh state."""

    base_state = ProfileState(
        id="example",
        openrouter_api_key="test",
        openrouter_model="model",
        trakt_client_id=None,
        trakt_access_token=None,
        catalog_count=4,
        catalog_item_count=12,
        refresh_interval_seconds=3600,
        response_cache_seconds=600,
        trakt_history_limit=1_000,
        next_refresh_at=datetime.utcnow() + timedelta(seconds=1800),
        last_refreshed_at=datetime.utcnow(),
    )

    ready_status = ProfileStatus(
        state=base_state,
        has_catalogs=True,
        needs_refresh=False,
        refreshing=False,
    )
    payload = ready_status.to_payload()
    assert payload["ready"] is True
    assert payload["hasCatalogs"] is True
    assert payload["refreshing"] is False

    refreshing_status = ProfileStatus(
        state=base_state,
        has_catalogs=True,
        needs_refresh=True,
        refreshing=True,
    )
    refreshing_payload = refreshing_status.to_payload()
    assert refreshing_payload["ready"] is False
    assert refreshing_payload["needsRefresh"] is True


def test_trakt_snapshot_normalisation() -> None:
    """Rich Trakt stats are flattened for UI consumption."""

    stats = {
        "movies": {"watched": 297, "plays": 345, "minutes": 4000},
        "shows": {"watched": 119, "collected": 42},
        "episodes": {"watched": 1889, "plays": 2151, "minutes": 88000},
    }

    snapshot = CatalogService._build_trakt_history_snapshot(stats)

    assert snapshot["movies"] == {"watched": 297, "plays": 345, "minutes": 4000}
    assert snapshot["shows"] == {"watched": 119}
    assert snapshot["episodes"] == {
        "watched": 1889,
        "plays": 2151,
        "minutes": 88000,
    }
    assert snapshot["totalMinutes"] == 4000 + 88000


def test_trakt_watched_extraction_handles_missing_sections() -> None:
    """Missing sections or malformed values do not raise errors."""

    stats = {"movies": {"watched": 12}, "shows": {}}

    assert CatalogService._extract_trakt_watched(stats, "movies") == 12
    assert CatalogService._extract_trakt_watched(stats, "shows") is None
    assert CatalogService._extract_trakt_watched(stats, "episodes") is None


def test_profile_id_inferred_from_catalog_id() -> None:
    """Catalog IDs embed the profile namespace for lookups."""

    settings = Settings(_env_file=None)
    service = CatalogService(
        settings,
        cast(TraktClient, object()),
        cast(OpenRouterClient, object()),
        cast(MetadataAddonClient, object()),
        cast(Database, object()),  # session factory not needed for this test
    )

    scoped_id = "user-123abc__aiopicks-movie-epic-adventures"
    assert service.profile_id_from_catalog_id(scoped_id) == "user-123abc"
    assert service.profile_id_from_catalog_id("aiopicks-movie-epic-adventures") is None


def test_profile_id_uses_trakt_slug() -> None:
    """Trakt logins derive stable profile identifiers from the user slug."""

    class DummyTrakt:
        async def fetch_user(self, *, client_id=None, access_token=None):  # noqa: D401
            return {
                "ids": {"slug": "Example_User"},
                "username": "Example_User",
                "name": "Example User",
            }

    settings = Settings(_env_file=None)
    service = CatalogService(
        settings,
        cast(TraktClient, DummyTrakt()),
        cast(OpenRouterClient, object()),
        cast(MetadataAddonClient, object()),
        cast(Database, object()),
    )

    config = ManifestConfig.model_validate(
        {"traktAccessToken": "token-123", "traktClientId": "client"}
    )

    profile_id = asyncio.run(service.determine_profile_id(config))

    assert profile_id == "trakt-example-user"


def test_profile_id_ignores_default_hint_when_trakt_present() -> None:
    """Explicit default profile IDs are overridden when Trakt identity is known."""

    class DummyTrakt:
        async def fetch_user(self, *, client_id=None, access_token=None):  # noqa: D401
            return {
                "ids": {"slug": "Example_User"},
                "username": "Example_User",
                "name": "Example User",
            }

    settings = Settings(_env_file=None)
    service = CatalogService(
        settings,
        cast(TraktClient, DummyTrakt()),
        cast(OpenRouterClient, object()),
        cast(MetadataAddonClient, object()),
        cast(Database, object()),
    )

    config = ManifestConfig.model_validate(
        {
            "profileId": "default",
            "traktAccessToken": "token-123",
            "traktClientId": "client",
        }
    )

    profile_id = asyncio.run(service.determine_profile_id(config))

    assert profile_id == "trakt-example-user"


def test_profile_id_hashes_trakt_token_when_slug_missing() -> None:
    """Fallback hashes the access token to keep IDs unique when slug lookup fails."""

    class DummyTrakt:
        async def fetch_user(self, *, client_id=None, access_token=None):  # noqa: D401
            return {}

    token = "token-456"
    expected = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]

    settings = Settings(_env_file=None)
    service = CatalogService(
        settings,
        cast(TraktClient, DummyTrakt()),
        cast(OpenRouterClient, object()),
        cast(MetadataAddonClient, object()),
        cast(Database, object()),
    )

    config = ManifestConfig.model_validate({"traktAccessToken": token})
    profile_id = asyncio.run(service.determine_profile_id(config))

    assert profile_id == f"trakt-{expected}"


def test_resolve_profile_persists_trakt_display_name(tmp_path) -> None:
    """Resolved profiles store the human-friendly Trakt display name."""

    class DummyTrakt:
        async def fetch_user(self, *, client_id=None, access_token=None):  # noqa: D401
            return {
                "ids": {"slug": "Example_User"},
                "username": "Example_User",
                "name": "Example User",
            }

    async def runner() -> None:
        database_path = tmp_path / "display.db"
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        await database.create_all()

        settings = Settings(_env_file=None, OPENROUTER_API_KEY="test-key")
        service = CatalogService(
            settings,
            cast(TraktClient, DummyTrakt()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, object()),
            database.session_factory,
        )

        config = ManifestConfig.model_validate(
            {"traktAccessToken": "token-abc", "traktClientId": "client"}
        )

        context = await service.resolve_profile(config)
        assert context.state.id == "trakt-example-user"

        async with database.session_factory() as session:
            profile = await session.get(Profile, context.state.id)
            assert profile is not None
            assert profile.display_name == "Example User"

        await database.dispose()

    asyncio.run(runner())


def test_catalog_lookup_falls_back_to_any_profile(tmp_path) -> None:
    """Catalog retrieval works even when the request lacks profile context."""

    async def runner() -> None:
        database_path = tmp_path / "fallback.db"
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        await database.create_all()

        settings = Settings(_env_file=None)
        service = CatalogService(
            settings,
            cast(TraktClient, object()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, object()),
            database.session_factory,
        )

        now = datetime.utcnow()
        profile = Profile(
            id="user-abcdef",
            openrouter_api_key="key",
            openrouter_model="model",
            catalog_count=1,
            catalog_item_count=8,
            refresh_interval_seconds=3600,
            response_cache_seconds=3600,
            next_refresh_at=now,
            created_at=now,
            updated_at=now,
        )

        item = CatalogItem(
            title="Sample Movie",
            type="movie",
            overview="A test entry",
            imdb_id="tt1234567",
        )
        catalog = Catalog(
            id="aiopicks-movie-test-catalog",
            type="movie",
            title="Test Catalog",
            description=None,
            seed="abcd",
            items=[item],
            generated_at=now,
        )

        async with database.session_factory() as session:
            session.add(profile)
            await session.flush()
            record = CatalogRecord(
                profile_id="user-abcdef",
                content_type="movie",
                catalog_id=catalog.id,
                title=catalog.title,
                description=catalog.description,
                seed=catalog.seed,
                position=0,
                payload=catalog.model_dump(mode="json"),
                generated_at=catalog.generated_at,
                expires_at=catalog.generated_at + timedelta(seconds=3600),
                created_at=now,
                updated_at=now,
            )
            session.add(record)
            await session.commit()
            record_id = record.id

        catalog_id = "user-abcdef__aiopicks-movie-test-catalog"
        config = ManifestConfig()
        payload = await service.get_catalog_payload(
            config, "movie", catalog_id
        )
        assert payload["catalogName"] == "Test Catalog"
        assert payload["metas"] == [
            {
                "id": "tt1234567",
                "type": "movie",
                "name": "Sample Movie",
                "description": "A test entry",
                "imdbId": "tt1234567",
                "imdb_id": "tt1234567",
            }
        ]

        async with database.session_factory() as session:
            stored = await session.get(CatalogRecord, record_id)
            assert stored is not None
            assert stored.catalog_id == catalog_id

        await database.dispose()

    asyncio.run(runner())


def test_metadata_addon_url_persisted(tmp_path) -> None:
    """Metadata add-on URLs supplied in config are stored on the profile."""

    async def runner() -> None:
        database_path = tmp_path / "metadata.db"
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        await database.create_all()

        settings = Settings(_env_file=None)
        service = CatalogService(
            settings,
            cast(TraktClient, object()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, object()),
            database.session_factory,
        )

        config = ManifestConfig.model_validate(
            {
                "openrouterKey": "sk-test",
                "metadataAddon": "https://example-addon.strem.fun/manifest.json",
            }
        )
        context = await service._resolve_profile(config)

        assert context.state.metadata_addon_url == "https://example-addon.strem.fun/manifest.json"

        loaded = await service._load_profile_state(context.state.id)
        assert loaded is not None
        assert loaded.metadata_addon_url == "https://example-addon.strem.fun/manifest.json"

        await database.dispose()

    asyncio.run(runner())


def test_history_limit_persisted(tmp_path) -> None:
    """Custom history limits should be stored and surfaced in profile state."""

    async def runner() -> None:
        database_path = tmp_path / "history.db"
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        await database.create_all()

        settings = Settings(_env_file=None)
        service = CatalogService(
            settings,
            cast(TraktClient, object()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, object()),
            database.session_factory,
        )

        config = ManifestConfig.model_validate(
            {
                "openrouterKey": "sk-history",
                "traktHistoryLimit": 1500,
            }
        )
        context = await service._resolve_profile(config)

        assert context.state.trakt_history_limit == 1500

        loaded = await service._load_profile_state(context.state.id)
        assert loaded is not None
        assert loaded.trakt_history_limit == 1500

        await database.dispose()

    asyncio.run(runner())


def test_watched_index_collects_identifiers() -> None:
    """Completed titles produce stable fingerprints for exclusion logic."""

    service = CatalogService.__new__(CatalogService)
    movie_history = [
        {
            "movie": {
                "title": "Seen Film",
                "year": 2020,
                "ids": {
                    "imdb": "tt1234567",
                    "trakt": 101,
                    "tmdb": 202,
                    "slug": "seen-film",
                },
            }
        }
    ]
    show_history = [
        {
            "show": {
                "title": "Seen Show",
                "year": 2018,
                "ids": {
                    "imdb": "tt7654321",
                    "trakt": 303,
                },
            }
        }
    ]

    index = service._build_watched_index(movie_history, show_history)
    movie_index = index["movie"]
    series_index = index["series"]

    assert "movie:imdb:tt1234567" in movie_index.fingerprints
    assert "movie:trakt:101" in movie_index.fingerprints
    assert movie_index.recent_titles == ["Seen Film (2020)"]
    assert "series:imdb:tt7654321" in series_index.fingerprints
    assert "series:trakt:303" in series_index.fingerprints


def test_serialise_watched_index_filters_empty_entries() -> None:
    """Serialisation drops empty content types and trims title samples."""

    service = CatalogService.__new__(CatalogService)
    index = {
        "movie": service._index_history_items(
            [
                {
                    "movie": {
                        "title": "Another Film",
                        "year": 2021,
                        "ids": {"imdb": "tt9999999"},
                    }
                }
            ],
            key="movie",
        ),
        "series": service._index_history_items([], key="show"),
    }

    payload = service._serialise_watched_index(index)
    assert set(payload.keys()) == {"movie"}
    assert set(payload["movie"]["fingerprints"]) == {
        "movie:imdb:tt9999999",
        "movie:title:another film",
        "movie:title:another film:2021",
    }
    assert payload["movie"]["recent_titles"] == ["Another Film (2021)"]


def test_metadata_lookup_retries_on_402() -> None:
    """HTTP 402 responses trigger a short retry before giving up."""

    async def runner() -> None:
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(402, request=request)
            return httpx.Response(
                200,
                request=request,
                json={
                    "metas": [
                        {
                            "name": "Hinamatsuri",
                            "type": "series",
                            "id": "tt8076356",
                            "releaseInfo": "2018",
                            "poster": "https://example.com/poster.jpg",
                        }
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            metadata_client = MetadataAddonClient(client, "https://example.com")
            match = await metadata_client.lookup(
                "Hinamatsuri", content_type="series", year=2018
            )

        assert match is not None
        assert match.id == "tt8076356"
        assert call_count == 2

    asyncio.run(runner())


def test_catalog_items_removed_when_metadata_missing() -> None:
    """Items without metadata add-on results are dropped from catalogs."""

    async def runner() -> None:
        class DummyMetadataAddon:
            default_base_url = "https://example.com"

            def __init__(self) -> None:
                self.calls: list[tuple[str, str, int | None]] = []

            async def lookup(
                self,
                title: str,
                *,
                content_type: str,
                year: int | None = None,
                base_url: str | None = None,
            ) -> MetadataMatch | None:
                self.calls.append((title, content_type, year))
                return None

        metadata_client = DummyMetadataAddon()
        settings = Settings(_env_file=None)
        service = CatalogService(
            settings,
            cast(TraktClient, object()),
            cast(OpenRouterClient, object()),
            cast(MetadataAddonClient, metadata_client),
            cast(Database, object()),
        )

        catalog = Catalog(
            id="aiopicks-movie-test",
            type="movie",
            title="Test Catalog",
            items=[
                CatalogItem(title="Needs Help", type="movie"),
                CatalogItem(
                    title="Already Good",
                    type="movie",
                    imdb_id="tt0111161",
                    poster="https://example.com/poster.jpg",
                ),
            ],
            generated_at=datetime.utcnow(),
        )

        catalogs = {"movie": {catalog.id: catalog}, "series": {}}
        await service._enrich_catalogs_with_metadata(
            catalogs, metadata_addon_url=None
        )

        updated_items = catalogs["movie"][catalog.id].items
        assert [item.title for item in updated_items] == ["Already Good"]
        assert metadata_client.calls == [("Needs Help", "movie", None)]

    asyncio.run(runner())

