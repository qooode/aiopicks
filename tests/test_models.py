from app.models import Catalog, CatalogBundle, CatalogItem


def test_catalog_from_ai_payload_generates_ids():
    catalog = Catalog.from_ai_payload(
        {
            "title": "Cozy Time Capsules",
            "description": "Stories to unwind with",
            "items": [
                {
                    "name": "The Secret Life of Walter Mitty",
                    "type": "movie",
                    "description": "A daydreamer's journey",
                    "poster": "https://example.com/poster.jpg",
                    "year": 2013,
                    "imdb_id": "tt0359950",
                }
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
        "imdbId": "tt0359950",
    }


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
