from __future__ import annotations

import asyncio
from typing import cast

from app.config import Settings
from app.database import Database
from app.services.catalog_generator import CatalogService
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
