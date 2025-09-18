from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import cast

from app.config import Settings
from app.database import Database
from app.services.catalog_generator import CatalogService
from app.services.catalog_generator import ProfileState, ProfileStatus
from app.services.openrouter import OpenRouterClient
from app.services.trakt import TraktClient


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
