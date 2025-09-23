"""Database utilities for the AIOPicks service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy import MetaData, inspect, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base with consistent naming conventions."""

    metadata = MetaData()


class Database:
    """Thin wrapper managing the SQLAlchemy async engine and sessions."""

    def __init__(self, database_url: str):
        self._engine: AsyncEngine = create_async_engine(database_url, future=True)
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    async def create_all(self) -> None:
        """Create database tables if they do not yet exist."""

        async with self._engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            await connection.run_sync(self._apply_schema_migrations)

    @staticmethod
    def _apply_schema_migrations(sync_connection) -> None:
        """Ensure newly introduced columns are available on existing tables."""

        inspector = inspect(sync_connection)
        table_names = inspector.get_table_names()
        if "profiles" not in table_names:
            return

        existing_columns = {
            column["name"] for column in inspector.get_columns("profiles")
        }

        def _ensure_column(name: str, ddl: str, init_sql: str | None = None) -> None:
            if name in existing_columns:
                return
            sync_connection.execute(text(ddl))
            if init_sql:
                sync_connection.execute(text(init_sql))
            existing_columns.add(name)

        _ensure_column(
            "trakt_history_limit",
            "ALTER TABLE profiles ADD COLUMN trakt_history_limit INTEGER DEFAULT 1000",
            "UPDATE profiles SET trakt_history_limit = 1000 WHERE trakt_history_limit IS NULL",
        )
        _ensure_column(
            "trakt_movie_history_count",
            "ALTER TABLE profiles ADD COLUMN trakt_movie_history_count INTEGER DEFAULT 0",
            (
                "UPDATE profiles SET trakt_movie_history_count = 0 "
                "WHERE trakt_movie_history_count IS NULL"
            ),
        )
        _ensure_column(
            "trakt_show_history_count",
            "ALTER TABLE profiles ADD COLUMN trakt_show_history_count INTEGER DEFAULT 0",
            (
                "UPDATE profiles SET trakt_show_history_count = 0 "
                "WHERE trakt_show_history_count IS NULL"
            ),
        )
        _ensure_column(
            "trakt_history_refreshed_at",
            "ALTER TABLE profiles ADD COLUMN trakt_history_refreshed_at DATETIME",
        )
        _ensure_column(
            "trakt_history_snapshot",
            "ALTER TABLE profiles ADD COLUMN trakt_history_snapshot JSON",
        )
        _ensure_column(
            "catalog_keys",
            "ALTER TABLE profiles ADD COLUMN catalog_keys JSON",
            "UPDATE profiles SET catalog_keys = '[]' WHERE catalog_keys IS NULL",
        )

    async def dispose(self) -> None:
        """Dispose of the underlying database engine."""

        await self._engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Provide a transactional scope around a series of operations."""

        async with self.session_factory() as session:
            yield session
