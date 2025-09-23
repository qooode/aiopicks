"""Tests for the metadata add-on helper utilities."""

import pytest

from app.services.metadata_addon import MetadataAddonClient


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "https://provider.example.com/manifest.json",
            "https://provider.example.com",
        ),
        (
            "https://addons.example.com/custom/manifest.json?token=abc",
            "https://addons.example.com/custom",
        ),
        (
            "https://addons.example.com/custom/manifest.json/",
            "https://addons.example.com/custom",
        ),
        (
            "https://addons.example.com/custom/",
            "https://addons.example.com/custom",
        ),
        (
            "https://provider.example.com/manifest.js…",
            "https://provider.example.com",
        ),
        (
            "https://provider.example.com/manifest.js%E2%80%A6",
            "https://provider.example.com",
        ),
        (
            "https://example.com/addons/aiopicks",
            "https://example.com/addons/aiopicks",
        ),
    ],
)
def test_normalize_base_url_handles_common_variations(raw: str, expected: str) -> None:
    """Various manifest URL formats normalize to the service base URL."""

    assert MetadataAddonClient._normalize_base_url(raw) == expected


def test_normalize_base_url_rejects_empty_values() -> None:
    """Empty strings or ``None`` are treated as missing URLs."""

    assert MetadataAddonClient._normalize_base_url(None) is None
    assert MetadataAddonClient._normalize_base_url("   ") is None
