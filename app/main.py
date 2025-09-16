"""Entry point for the FastAPI-powered Stremio addon."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import settings
from .services.catalog_generator import CatalogService
from .services.openrouter import OpenRouterClient
from .services.trakt import TraktClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app: FastAPI


@asynccontextmanager
async def lifespan(_: FastAPI):
    exit_stack = AsyncExitStack()
    trakt_client = await exit_stack.enter_async_context(
        httpx.AsyncClient(
            base_url=str(settings.trakt_api_url),
            timeout=httpx.Timeout(20.0, connect=10.0),
        )
    )
    openrouter_client = await exit_stack.enter_async_context(
        httpx.AsyncClient(
            base_url=str(settings.openrouter_api_url),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )
    )

    trakt = TraktClient(settings, trakt_client)
    openrouter = OpenRouterClient(settings, openrouter_client)
    catalog_service = CatalogService(settings, trakt, openrouter)

    app.state.catalog_service = catalog_service
    await catalog_service.start()

    try:
        yield
    finally:  # pragma: no cover - teardown path exercised at runtime
        await catalog_service.stop()
        await exit_stack.aclose()


def create_app() -> FastAPI:
    fastapi_app = FastAPI(
        title=settings.app_name,
        description="AI-personalized catalogs for Stremio powered by OpenRouter",
        version="1.0.0",
        lifespan=lifespan,
    )

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"]
    )

    register_routes(fastapi_app)
    return fastapi_app


def get_catalog_service(app: FastAPI) -> CatalogService:
    service = getattr(app.state, "catalog_service", None)
    if not isinstance(service, CatalogService):
        raise RuntimeError("Catalog service not initialised")
    return service


def register_routes(fastapi_app: FastAPI) -> None:
    @fastapi_app.get("/healthz")
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @fastapi_app.get("/manifest.json")
    async def manifest() -> dict[str, Any]:
        service = get_catalog_service(fastapi_app)
        catalogs = await service.list_manifest_catalogs()
        return {
            "id": "com.aiopicks.python",
            "version": "1.0.0",
            "name": f"{settings.app_name} (Gemini 2.5 Flash Lite)",
            "description": (
                "Dynamic, AI-randomized catalogs powered by OpenRouter's Google Gemini 2.5 "
                "Flash Lite model and your Trakt history."
            ),
            "catalogs": catalogs,
            "resources": ["catalog", "meta"],
            "types": ["movie", "series"],
            "idPrefixes": ["aiopicks", "tt", "trakt"],
        }

    @fastapi_app.get("/catalog/{content_type}/{catalog_id}.json")
    async def catalog(content_type: str, catalog_id: str) -> JSONResponse:
        if content_type not in {"movie", "series"}:
            raise HTTPException(status_code=400, detail="Unsupported content type")
        service = get_catalog_service(fastapi_app)
        try:
            payload = await service.get_catalog_payload(content_type, catalog_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(payload)

    @fastapi_app.get("/meta/{content_type}/{meta_id}.json")
    async def meta(content_type: str, meta_id: str) -> JSONResponse:
        if content_type not in {"movie", "series"}:
            raise HTTPException(status_code=400, detail="Unsupported content type")
        service = get_catalog_service(fastapi_app)
        try:
            meta_payload = await service.find_meta(content_type, meta_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse({"meta": meta_payload})


app = create_app()


if __name__ == "__main__":  # pragma: no cover - manual execution
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.environment == "development",
    )
