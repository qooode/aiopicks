from app.models import Catalog, CatalogBundle


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
    assert catalog.items[0].to_meta(catalog.id, 0)["id"] == "tt0359950"


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
