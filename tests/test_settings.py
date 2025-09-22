"""Configuration settings behaviour tests."""

from __future__ import annotations

import pytest

from app.config import Settings, DEFAULT_CATALOG_KEYS


def test_catalog_keys_subset_selection() -> None:
    """Settings should respect custom catalog key selections."""

    settings = Settings(_env_file=None, CATALOG_KEYS="movies-for-you,actors-you-love")

    assert settings.catalog_keys == ("movies-for-you", "actors-you-love")
    assert settings.catalog_count == 2
    assert [definition.key for definition in settings.catalog_definitions] == [
        "movies-for-you",
        "actors-you-love",
    ]


def test_catalog_keys_accepts_case_insensitive_values() -> None:
    """Catalog keys should be parsed case-insensitively."""

    settings = Settings(_env_file=None, CATALOG_KEYS=["Movies-For-You", "HIDDEN-GEMS"])

    assert settings.catalog_keys == ("movies-for-you", "hidden-gems")
    assert settings.catalog_count == 2


def test_catalog_keys_blank_defaults() -> None:
    """Blank catalog keys should fall back to the full stable set."""

    settings = Settings(_env_file=None, CATALOG_KEYS="")

    assert settings.catalog_keys == DEFAULT_CATALOG_KEYS
    assert settings.catalog_count == len(DEFAULT_CATALOG_KEYS)


def test_catalog_keys_invalid_raises() -> None:
    """Unknown catalog keys should raise a validation error."""

    with pytest.raises(ValueError, match="Unknown catalog keys configured"):
        Settings(_env_file=None, CATALOG_KEYS="does-not-exist")


def test_catalog_count_must_match_keys() -> None:
    """Explicit catalog counts must match the selected keys."""

    with pytest.raises(ValueError, match="must match the number of configured catalog keys"):
        Settings(
            _env_file=None,
            CATALOG_KEYS="movies-for-you",
            CATALOG_COUNT=2,
        )
