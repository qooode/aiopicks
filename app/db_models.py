"""SQLAlchemy ORM models backing the persistent state."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base
from .stable_catalogs import STABLE_CATALOG_COUNT


class Profile(Base):
    """Represents a persisted user configuration profile."""

    __tablename__ = "profiles"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    display_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    openrouter_api_key: Mapped[str] = mapped_column(Text)
    openrouter_model: Mapped[str] = mapped_column(String(200))
    trakt_client_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    trakt_access_token: Mapped[str | None] = mapped_column(String(200), nullable=True)
    trakt_history_limit: Mapped[int] = mapped_column(Integer, default=1_000)
    trakt_movie_history_count: Mapped[int] = mapped_column(Integer, default=0)
    trakt_show_history_count: Mapped[int] = mapped_column(Integer, default=0)
    trakt_history_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    trakt_history_snapshot: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    catalog_keys: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    catalog_count: Mapped[int] = mapped_column(Integer, default=STABLE_CATALOG_COUNT)
    catalog_item_count: Mapped[int] = mapped_column(Integer, default=8)
    combine_for_you_catalogs: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    generation_retry_limit: Mapped[int] = mapped_column(Integer, default=3)
    refresh_interval_seconds: Mapped[int] = mapped_column(Integer, default=43_200)
    response_cache_seconds: Mapped[int] = mapped_column(Integer, default=1_800)
    metadata_addon_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    next_refresh_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    catalogs: Mapped[list["CatalogRecord"]] = relationship(
        back_populates="profile", cascade="all, delete-orphan"
    )


class CatalogRecord(Base):
    """Persisted catalog payloads tied to a profile."""

    __tablename__ = "catalogs"
    __table_args__ = (
        UniqueConstraint("profile_id", "catalog_id", name="uq_catalog_profile"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("profiles.id", ondelete="CASCADE")
    )
    content_type: Mapped[str] = mapped_column(String(16))
    catalog_id: Mapped[str] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    seed: Mapped[str | None] = mapped_column(String(32), nullable=True)
    position: Mapped[int] = mapped_column(Integer, default=0)
    payload: Mapped[dict[str, object]] = mapped_column(JSON)
    generated_at: Mapped[datetime] = mapped_column(DateTime)
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    profile: Mapped[Profile] = relationship(back_populates="catalogs")
