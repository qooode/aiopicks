from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.main import register_routes
from app.services.catalog_generator import CatalogService, ManifestConfig


class DummyCatalogService(CatalogService):
    """Minimal CatalogService stub for manifest testing."""

    def __init__(self) -> None:  # pragma: no cover - nothing to initialise
        # Deliberately skip super().__init__ to avoid touching external systems.
        self.last_config: ManifestConfig | None = None

    async def list_manifest_catalogs(  # type: ignore[override]
        self, config: ManifestConfig
    ) -> tuple[SimpleNamespace, list[dict[str, object]]]:
        self.last_config = config
        state = SimpleNamespace(openrouter_model="test-model")
        return state, []

    def profile_id_from_catalog_id(self, catalog_id: str) -> str | None:  # pragma: no cover - not used
        return None


def test_manifest_advertises_only_catalog_resource() -> None:
    """The manifest should list only catalog resources so Stremio won't request meta."""

    app = FastAPI()
    register_routes(app)
    app.state.catalog_service = DummyCatalogService()

    with TestClient(app) as client:
        response = client.get("/manifest.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload["resources"] == ["catalog"]


def test_manifest_allows_path_overrides() -> None:
    app = FastAPI()
    register_routes(app)
    service = DummyCatalogService()
    app.state.catalog_service = service

    with TestClient(app) as client:
        response = client.get(
            "/manifest/catalogCount/5/catalogItems/9/manifest.json"
        )

    assert response.status_code == 200
    assert service.last_config is not None
    assert service.last_config.catalog_count == 5
    assert service.last_config.catalog_item_count == 9


def test_manifest_rejects_malformed_path_overrides() -> None:
    app = FastAPI()
    register_routes(app)
    app.state.catalog_service = DummyCatalogService()

    with TestClient(app) as client:
        response = client.get("/manifest/catalogCount/6/catalogItems/manifest.json")

    assert response.status_code == 400


def test_manifest_rejects_query_overrides() -> None:
    app = FastAPI()
    register_routes(app)
    app.state.catalog_service = DummyCatalogService()

    with TestClient(app) as client:
        response = client.get(
            "/manifest.json",
            params={"catalogCount": "3"},
        )

    assert response.status_code == 400
