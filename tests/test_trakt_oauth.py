"""Tests for the Trakt OAuth helper endpoints."""

from __future__ import annotations

from contextlib import contextmanager
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from app.config import settings
from app.main import CatalogService, create_app


@contextmanager
def _test_client(monkeypatch):
    monkeypatch.setattr(settings, "trakt_client_id", "client")
    monkeypatch.setattr(settings, "trakt_client_secret", "secret")

    async def _noop_start(self):  # type: ignore[override]
        return None

    monkeypatch.setattr(CatalogService, "start", _noop_start)

    app = create_app()
    with TestClient(app) as client:
        yield client


def _extract_redirect(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    redirect_values = params.get("redirect_uri")
    assert redirect_values, "redirect_uri missing from login URL"
    return redirect_values[0]


def test_trakt_login_url_respects_forwarded_proto_and_host(monkeypatch) -> None:
    with _test_client(monkeypatch) as client:
        response = client.post(
            "/api/trakt/login-url",
            headers={
                "origin": "https://app.example",
                "x-forwarded-proto": "https",
                "x-forwarded-host": "app.example",
                "x-forwarded-port": "443",
            },
        )
        assert response.status_code == 200
        redirect_uri = _extract_redirect(response.json()["url"])
        assert redirect_uri == "https://app.example/api/trakt/callback"


def test_trakt_login_url_honours_forwarded_prefix(monkeypatch) -> None:
    with _test_client(monkeypatch) as client:
        response = client.post(
            "/api/trakt/login-url",
            headers={
                "origin": "https://example.com",
                "x-forwarded-proto": "https",
                "x-forwarded-host": "example.com",
                "x-forwarded-prefix": "/addon",
            },
        )
        assert response.status_code == 200
        redirect_uri = _extract_redirect(response.json()["url"])
        assert redirect_uri == "https://example.com/addon/api/trakt/callback"
