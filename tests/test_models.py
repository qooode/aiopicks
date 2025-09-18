from app.models import Catalog, CatalogBundle, CatalogItem


def test_catalog_from_ai_payload_generates_ids():
    catalog = Catalog.from_ai_payload(
        {
            "title": "Cozy Time Capsules",
            "description": "Stories to unwind with",
            "items": [
                {
                    "imdbId": "tt0359950",
                    "type": "movie",
                    "description": "A daydreamer's journey",
                    "poster": "https://example.com/poster.jpg",
                    "year": 2013,
                    "name": "The Secret Life of Walter Mitty",
                },
                {
                    "id": "tt0063350",
                    "type": "movie",
                    "imdbId": "tt0063350",
                },
            ],
        },
        content_type="movie",
        fallback_seed="abcd",
    )

    assert catalog.id.startswith("aiopicks-movie")
    stub = catalog.items[0].to_catalog_stub(catalog.id, 0)
    assert stub == {
        "id": "tt0359950",
        "type": "movie",
        "name": "The Secret Life of Walter Mitty",
        "imdbId": "tt0359950",
        "imdb_id": "tt0359950",
    }

    fallback_stub = catalog.items[1].to_catalog_stub(catalog.id, 1)
    assert fallback_stub["id"] == "tt0063350"
    assert fallback_stub["name"] == "tt0063350"


def test_catalog_bundle_from_ai_response_handles_missing_sections():
    bundle = CatalogBundle.from_ai_response(
        {
            "movies": [
                {
                    "title": "Chill Friday",
                    "items": [
                        {
                            "name": "Arrival",
                            "type": "movie",
                            "poster": "https://example.com/arrival.jpg",
                        }
                    ],
                }
            ]
        },
        seed="abcd",
    )

    assert len(bundle.movie_catalogs) == 1
    assert bundle.movie_catalogs[0].title == "Chill Friday"
    assert bundle.series_catalogs == []


def test_catalog_item_uses_tmdb_id_when_other_ids_missing() -> None:
    item = CatalogItem(title="Example", type="movie", tmdb_id=12345)
    stub = item.to_catalog_stub("aiopicks-movie-demo", 0)
    assert stub["id"] == "tmdb:12345"
    assert stub["type"] == "movie"
    assert stub["tmdbId"] == 12345
    assert stub["tmdb_id"] == 12345
    assert stub["name"] == "Example"


def test_catalog_item_uses_identifier_as_fallback_name() -> None:
    item = CatalogItem(type="movie", imdb_id="tt7654321")
    stub = item.to_catalog_stub("aiopicks-movie-demo", 0)
    assert stub["name"] == "tt7654321"
    assert item.display_name() == "tt7654321"


def test_catalog_item_accepts_camel_case_ids() -> None:
    item = CatalogItem.model_validate(
        {"type": "movie", "name": "Sample", "traktId": 42, "tmdbId": 123}
    )
    assert item.trakt_id == 42
    assert item.tmdb_id == 123
