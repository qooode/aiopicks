"""Entry point for the FastAPI-powered Stremio addon."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import settings
from .services.catalog_generator import CatalogService
from .services.openrouter import OpenRouterClient
from .services.trakt import TraktClient
from .web import render_config_page

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app: FastAPI


class DeviceCodeRequest(BaseModel):
    """Payload for requesting a Trakt device code."""

    client_id: str = Field(min_length=5, max_length=128)


class DeviceTokenRequest(BaseModel):
    """Payload for polling a Trakt device token."""

    client_id: str = Field(min_length=5, max_length=128)
    client_secret: str = Field(min_length=5, max_length=160)
    device_code: str = Field(min_length=5, max_length=160)


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
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
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

    @fastapi_app.get("/config", response_class=HTMLResponse)
    async def config_page() -> HTMLResponse:
        return HTMLResponse(render_config_page(settings))

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

    @fastapi_app.post("/api/trakt/device-code")
    async def trakt_device_code(payload: DeviceCodeRequest) -> dict[str, Any]:
        try:
            response = await _post_trakt_oauth("/oauth/device/code", payload.model_dump())
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "network_error",
                    "description": "Unable to reach Trakt. Please try again shortly.",
                },
            ) from exc

        data = _response_json(response)
        if response.status_code >= 400:
            raise HTTPException(
                status_code=response.status_code,
                detail=_format_trakt_error(
                    data, "Trakt rejected the device code request."
                ),
            )

        verification = (
            data.get("verification_url")
            or data.get("verification_uri")
            or data.get("verification_uri_complete")
            or "https://trakt.tv/activate"
        )
        expires = _coerce_int(data.get("expires_in"), default=600)
        interval = _coerce_int(data.get("interval"), default=5)

        return {
            "device_code": str(data.get("device_code") or ""),
            "user_code": str(data.get("user_code") or ""),
            "verification_url": str(verification),
            "expires_in": expires,
            "interval": interval,
        }

    @fastapi_app.post("/api/trakt/device-token")
    async def trakt_device_token(payload: DeviceTokenRequest) -> dict[str, Any]:
        body = payload.model_dump()
        try:
            response = await _post_trakt_oauth("/oauth/device/token", body)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "network_error",
                    "description": "Unable to reach Trakt. Please try again shortly.",
                },
            ) from exc

        data = _response_json(response)
        if response.status_code >= 400:
            raise HTTPException(
                status_code=response.status_code,
                detail=_format_trakt_error(
                    data, "Trakt rejected the token request."
                ),
            )

        payload_map = {
            "access_token": data.get("access_token"),
            "refresh_token": data.get("refresh_token"),
            "expires_in": _coerce_int(data.get("expires_in")),
            "scope": data.get("scope"),
            "token_type": data.get("token_type"),
            "created_at": _coerce_int(data.get("created_at")),
        }
        return {key: value for key, value in payload_map.items() if value not in {None, ""}}


def _coerce_int(value: Any, *, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def _post_trakt_oauth(path: str, payload: dict[str, Any]) -> httpx.Response:
    headers = {
        "trakt-api-version": "2",
        "Content-Type": "application/json",
    }
    client_id = payload.get("client_id")
    if isinstance(client_id, str) and client_id:
        headers["trakt-api-key"] = client_id
    async with httpx.AsyncClient(
        base_url=str(settings.trakt_api_url),
        timeout=httpx.Timeout(20.0, connect=10.0),
    ) as client:
        return await client.post(path, json=payload, headers=headers)


def _response_json(response: httpx.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _format_trakt_error(data: dict[str, Any], fallback: str) -> dict[str, str]:
    if not data:
        return {"error": "trakt_error", "description": fallback}
    error = str(
        data.get("error")
        or data.get("error_description")
        or data.get("message")
        or "trakt_error"
    )
    description = str(
        data.get("error_description")
        or data.get("message")
        or data.get("hint")
        or data.get("description")
        or fallback
    )
    return {"error": error, "description": description}


app = create_app()


if __name__ == "__main__":  # pragma: no cover - manual execution
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.environment == "development",
    )
