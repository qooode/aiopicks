from app.utils import ensure_unique_meta_id, extract_json_object, slugify


def test_slugify_basic():
    assert slugify("Late Night Thrills!") == "late-night-thrills"


def test_extract_json_object_from_markdown():
    payload = """
    Here is your payload:
    ```json
    {"movie_catalogs": []}
    ```
    """
    assert extract_json_object(payload) == {"movie_catalogs": []}


def test_ensure_unique_meta_id_with_fallback():
    meta_id = ensure_unique_meta_id("", "Some Title", 3)
    assert meta_id.startswith("some-title")
