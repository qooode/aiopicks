from __future__ import annotations

import asyncio

from sqlalchemy import create_engine, inspect, text

from app.database import Database


def _initialise_legacy_schema(database_path: str) -> None:
    """Create a legacy profiles table lacking the new catalog_keys column."""

    engine = create_engine(f"sqlite:///{database_path}")
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE profiles (
                        id VARCHAR(64) PRIMARY KEY,
                        display_name VARCHAR(120),
                        openrouter_api_key TEXT,
                        openrouter_model VARCHAR(200),
                        trakt_client_id VARCHAR(200),
                        trakt_access_token VARCHAR(200),
                        trakt_history_limit INTEGER,
                        trakt_movie_history_count INTEGER,
                        trakt_show_history_count INTEGER,
                        trakt_history_refreshed_at DATETIME,
                        trakt_history_snapshot JSON,
                        catalog_count INTEGER,
                        catalog_item_count INTEGER,
                        generation_retry_limit INTEGER,
                        refresh_interval_seconds INTEGER,
                        response_cache_seconds INTEGER,
                        metadata_addon_url VARCHAR(512),
                        next_refresh_at DATETIME,
                        last_refreshed_at DATETIME,
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                    """
                )
            )
    finally:
        engine.dispose()


def test_create_all_adds_catalog_keys_column(tmp_path) -> None:
    """Schema migrations should backfill the catalog_keys column."""

    database_path = tmp_path / "legacy.db"
    _initialise_legacy_schema(str(database_path))

    database = Database(f"sqlite+aiosqlite:///{database_path}")
    asyncio.run(database.create_all())
    asyncio.run(database.dispose())

    inspector_engine = create_engine(f"sqlite:///{database_path}")
    try:
        inspector = inspect(inspector_engine)
        columns = {column["name"] for column in inspector.get_columns("profiles")}
    finally:
        inspector_engine.dispose()

    assert "catalog_keys" in columns
