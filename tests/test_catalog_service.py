from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import cast

from app.config import Settings
from app.database import Database
from app.db_models import CatalogRecord, Profile
from app.models import Catalog, CatalogItem
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
        refresh_interval_seconds=3600,
        response_cache_seconds=600,
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


def test_profile_id_inferred_from_catalog_id() -> None:
    """Catalog IDs embed the profile namespace for lookups."""

    settings = Settings(_env_file=None)
    service = CatalogService(
        settings,
        cast(TraktClient, object()),
        cast(OpenRouterClient, object()),
        cast(Database, object()),  # session factory not needed for this test
    )

    scoped_id = "user-123abc__aiopicks-movie-epic-adventures"
    assert service.profile_id_from_catalog_id(scoped_id) == "user-123abc"
    assert service.profile_id_from_catalog_id("aiopicks-movie-epic-adventures") is None


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
            database.session_factory,
        )

        now = datetime.utcnow()
        profile = Profile(
            id="user-abcdef",
            openrouter_api_key="key",
            openrouter_model="model",
            catalog_count=1,
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
            }
        ]

        async with database.session_factory() as session:
            stored = await session.get(CatalogRecord, record_id)
            assert stored is not None
            assert stored.catalog_id == catalog_id

        await database.dispose()

    asyncio.run(runner())

