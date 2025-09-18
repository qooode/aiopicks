"""Pydantic models describing catalog payloads."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl

from .utils import ensure_unique_meta_id, slugify

ContentType = Literal["movie", "series"]


class CatalogItem(BaseModel):
    """Represents a single media entry returned to Stremio."""

    model_config = ConfigDict(populate_by_name=True)

    title: str = Field(
        validation_alias=AliasChoices("title", "name"),
        serialization_alias="name",
    )
    type: ContentType
    overview: str | None = Field(default=None, alias="description")
    poster: HttpUrl | None = None
    background: HttpUrl | None = None
    year: int | None = None
    trakt_id: int | None = None
    imdb_id: str | None = None
    tmdb_id: int | None = None
    weight: float | None = None
    runtime_minutes: int | None = None
    genres: list[str] = Field(default_factory=list)
    maturity_rating: str | None = None
    providers: list[str] = Field(default_factory=list)

    def build_meta_id(self, catalog_id: str, index: int) -> str:
        """Return the unique identifier used for catalog/meta lookups."""

        base_id = self.imdb_id or (
            f"trakt:{self.trakt_id}" if self.trakt_id else ""
        )
        if not base_id and self.tmdb_id:
            base_id = f"tmdb:{self.tmdb_id}"
        return ensure_unique_meta_id(base_id, f"{catalog_id}-{self.title}", index)

    def to_catalog_stub(self, catalog_id: str, index: int) -> dict[str, object]:
        """Return a minimal meta entry for catalog listings."""

        return {
            "id": self.build_meta_id(catalog_id, index),
            "type": self.type,
        }


class Catalog(BaseModel):
    """Collection of items grouped by the AI."""

    id: str
    type: ContentType
    title: str
    description: str | None = None
    seed: str | None = None
    items: list[CatalogItem] = Field(default_factory=list)
    generated_at: datetime

    @classmethod
    def from_ai_payload(
        cls,
        data: dict[str, object],
        *,
        content_type: ContentType,
        fallback_seed: str,
    ) -> "Catalog":
        title = str(data.get("title") or data.get("name") or "Surprise Picks")
        description = data.get("description") or data.get("summary")
        seed = str(data.get("seed") or fallback_seed)
        raw_items = data.get("items") or []
        items: list[CatalogItem] = []

        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            item_data = {**entry}
            item_data.setdefault("type", content_type)
            item = CatalogItem.model_validate(item_data)
            items.append(item)

        catalog_id = str(data.get("id") or slugify(title))
        catalog_slug = slugify(catalog_id)
        if not catalog_slug:
            catalog_slug = slugify(title)
        final_id = f"aiopicks-{content_type}-{catalog_slug}"

        return cls(
            id=final_id,
            type=content_type,
            title=title,
            description=str(description) if description else None,
            seed=seed,
            items=items,
            generated_at=datetime.utcnow(),
        )

    def to_manifest_entry(self) -> dict[str, object]:
        """Return a manifest catalog entry."""

        return {
            "type": self.type,
            "id": self.id,
            "name": self.title,
            "extra": [],
        }

    def to_catalog_response(self) -> dict[str, object]:
        """Return the Stremio catalog payload."""

        metas = [
            item.to_catalog_stub(self.id, index)
            for index, item in enumerate(self.items)
        ]
        return {
            "metas": metas,
            "catalogName": self.title,
            "catalogDescription": self.description,
        }


class CatalogBundle(BaseModel):
    """A pair of movie and series catalogs returned by the AI."""

    movie_catalogs: list[Catalog] = Field(default_factory=list)
    series_catalogs: list[Catalog] = Field(default_factory=list)

    @classmethod
    def from_ai_response(
        cls,
        data: dict[str, object],
        *,
        seed: str,
    ) -> "CatalogBundle":
        movie_payload = data.get("movie_catalogs") or data.get("movies") or []
        series_payload = data.get("series_catalogs") or data.get("shows") or []

        movies = [
            Catalog.from_ai_payload(entry, content_type="movie", fallback_seed=seed)
            for entry in movie_payload
            if isinstance(entry, dict)
        ]
        series = [
            Catalog.from_ai_payload(entry, content_type="series", fallback_seed=seed)
            for entry in series_payload
            if isinstance(entry, dict)
        ]
        return cls(movie_catalogs=movies, series_catalogs=series)

    def is_empty(self) -> bool:
        return not (self.movie_catalogs or self.series_catalogs)
