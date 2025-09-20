"""Entry point for the FastAPI-powered Stremio addon."""

from __future__ import annotations
import json
import logging
import secrets
import time
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, Mapping
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError

from .config import settings
from .database import Database
from .services.metadata_addon import MetadataAddonClient
from .services.catalog_generator import CatalogService
from .services.catalog_generator import ManifestConfig
from .services.openrouter import OpenRouterClient
from .services.trakt import TraktClient
from .web import render_config_page

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
    metadata_client_kwargs: dict[str, Any] = {
        "timeout": httpx.Timeout(15.0, connect=5.0)
    }
    if settings.metadata_addon_url:
        metadata_client_kwargs["base_url"] = str(settings.metadata_addon_url)
    metadata_http_client = await exit_stack.enter_async_context(
        httpx.AsyncClient(**metadata_client_kwargs)
    )
    database = Database(settings.database_url)
    await database.create_all()

    trakt = TraktClient(settings, trakt_client)
    openrouter = OpenRouterClient(settings, openrouter_client)
    default_metadata_addon = (
        str(settings.metadata_addon_url)
        if settings.metadata_addon_url is not None
        else None
    )
    metadata_client = MetadataAddonClient(
        metadata_http_client, default_metadata_addon
    )
    catalog_service = CatalogService(
        settings, trakt, openrouter, metadata_client, database.session_factory
    )

    app.state.catalog_service = catalog_service
    app.state.database = database
    await catalog_service.start()

    try:
        yield
    finally:  # pragma: no cover - teardown path exercised at runtime
        await catalog_service.stop()
        await database.dispose()
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

    fastapi_app.state.trakt_oauth_states: dict[str, dict[str, Any]] = {}

    register_routes(fastapi_app)
    return fastapi_app


def get_catalog_service(app: FastAPI) -> CatalogService:
    service = getattr(app.state, "catalog_service", None)
    if not isinstance(service, CatalogService):
        raise RuntimeError("Catalog service not initialised")
    return service


def register_routes(fastapi_app: FastAPI) -> None:
    async def _manifest_endpoint(
        request: Request,
        *,
        profile_id: str | None = None,
        extra_params: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        service = get_catalog_service(fastapi_app)
        try:
            query_params = dict(request.query_params)
            if query_params:
                raise HTTPException(
                    status_code=400,
                    detail="Query parameters are not supported for manifest overrides.",
                )
            payload: dict[str, str] = {}
            if extra_params:
                payload.update(extra_params)
            config = ManifestConfig.from_request(payload, profile_id=profile_id)
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.errors()) from exc

        try:
            profile_state, catalogs = await service.list_manifest_catalogs(config)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        manifest_suffix = getattr(profile_state, "id", None)
        manifest_id = "com.aiopicks.python"
        if isinstance(manifest_suffix, str) and manifest_suffix.strip():
            manifest_id = f"{manifest_id}.{manifest_suffix.strip()}"
        manifest_name = (config.manifest_name or "").strip()
        if not manifest_name:
            manifest_name = settings.app_name
        return {
            "id": manifest_id,
            "version": "1.0.0",
            "name": manifest_name,
            "description": "Dynamic catalogs tailored to your Trakt history.",
            "catalogs": catalogs,
            "resources": ["catalog"],
            "types": ["movie", "series"],
            "idPrefixes": ["aiopicks", "tt", "trakt"],
        }

    async def _catalog_endpoint(
        request: Request,
        content_type: str,
        catalog_id: str,
        *,
        profile_id: str | None = None,
    ) -> JSONResponse:
        if content_type not in {"movie", "series"}:
            raise HTTPException(status_code=400, detail="Unsupported content type")
        service = get_catalog_service(fastapi_app)
        if profile_id is None:
            inferred = service.profile_id_from_catalog_id(catalog_id)
            if inferred:
                profile_id = inferred
        try:
            config = ManifestConfig.from_request(
                request.query_params, profile_id=profile_id
            )
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.errors()) from exc
        try:
            payload = await service.get_catalog_payload(
                config, content_type, catalog_id
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload)

    @fastapi_app.get("/healthz")
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @fastapi_app.get("/config", response_class=HTMLResponse)
    async def config_page(request: Request) -> HTMLResponse:
        callback_origin, _ = _resolve_trakt_redirect(request)
        return HTMLResponse(
            render_config_page(settings, callback_origin=callback_origin)
        )

    def _parse_path_overrides(raw_segments: str) -> dict[str, str]:
        if not raw_segments:
            return {}
        segments = [segment for segment in raw_segments.split("/") if segment]
        if not segments:
            return {}
        if len(segments) % 2 != 0:
            raise ValueError("Path overrides must use key/value pairs")
        overrides: dict[str, str] = {}
        for index in range(0, len(segments), 2):
            key = segments[index]
            value = segments[index + 1]
            if not key:
                raise ValueError("Override keys may not be empty")
            overrides[key] = value
        return overrides

    @fastapi_app.get("/manifest.json")
    async def manifest(request: Request) -> dict[str, Any]:
        return await _manifest_endpoint(request)

    @fastapi_app.get("/manifest/{path_params:path}/manifest.json")
    async def manifest_with_path_overrides(
        request: Request, path_params: str
    ) -> dict[str, Any]:
        try:
            overrides = _parse_path_overrides(path_params)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return await _manifest_endpoint(request, extra_params=overrides)

    @fastapi_app.get("/profiles/{profile_id}/manifest.json")
    async def manifest_with_profile(
        request: Request, profile_id: str
    ) -> dict[str, Any]:
        return await _manifest_endpoint(request, profile_id=profile_id)

    @fastapi_app.get("/catalog/{content_type}/{catalog_id}.json")
    async def catalog(
        request: Request, content_type: str, catalog_id: str
    ) -> JSONResponse:
        return await _catalog_endpoint(request, content_type, catalog_id)

    @fastapi_app.get(
        "/profiles/{profile_id}/catalog/{content_type}/{catalog_id}.json"
    )
    async def catalog_with_profile(
        request: Request, profile_id: str, content_type: str, catalog_id: str
    ) -> JSONResponse:
        return await _catalog_endpoint(
            request, content_type, catalog_id, profile_id=profile_id
        )

    @fastapi_app.post("/api/profile/prepare")
    async def prepare_profile_endpoint(request: Request) -> JSONResponse:
        service = get_catalog_service(fastapi_app)
        try:
            payload = await request.json()
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")

        def _coerce_bool(value: object) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in {"1", "true", "yes", "on"}
            if isinstance(value, (int, float)):
                return bool(value)
            return False

        try:
            config = ManifestConfig.model_validate(payload)
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.errors()) from exc

        force_flag = _coerce_bool(payload.get("force"))
        wait_for_completion = _coerce_bool(payload.get("waitForCompletion", False))
        context = await service.resolve_profile(config)
        state = context.state
        status = await service.get_profile_status(state.id)
        if status is None:
            raise HTTPException(status_code=404, detail="Profile could not be resolved")

        should_refresh = force_flag or context.force_refresh or status.needs_refresh
        if should_refresh:
            if wait_for_completion:
                await service.ensure_catalogs(state, force=True, wait=True)
            else:
                service.request_refresh(state, force=True)
            status = await service.get_profile_status(state.id)
            if status is None:
                raise HTTPException(status_code=404, detail="Profile not found after refresh")

        return JSONResponse(status.to_payload())

    @fastapi_app.get("/api/profile/status")
    async def profile_status_endpoint(request: Request) -> JSONResponse:
        service = get_catalog_service(fastapi_app)
        try:
            config = ManifestConfig.from_query(request.query_params)
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.errors()) from exc

        def _has_overrides(cfg: ManifestConfig) -> bool:
            return any(
                value is not None
                for value in (
                    cfg.openrouter_key,
                    cfg.openrouter_model,
                    cfg.catalog_count,
                    cfg.catalog_item_count,
                    cfg.refresh_interval,
                    cfg.response_cache,
                    cfg.trakt_history_limit,
                    cfg.trakt_client_id,
                    cfg.trakt_access_token,
                    cfg.metadata_addon_url,
                )
            )

        profile_id = await service.determine_profile_id(config)
        status = await service.get_profile_status(profile_id)

        def _requires_resolution(existing_status) -> bool:
            if existing_status is None:
                return _has_overrides(config)

            state = existing_status.state
            if (
                config.trakt_access_token is not None
                and state.trakt_access_token != config.trakt_access_token
            ):
                return True
            if (
                config.trakt_client_id is not None
                and state.trakt_client_id != config.trakt_client_id
            ):
                return True
            if (
                config.trakt_history_limit is not None
                and state.trakt_history_limit != config.trakt_history_limit
            ):
                return True
            if (
                config.openrouter_key is not None
                and state.openrouter_api_key != config.openrouter_key
            ):
                return True
            if (
                config.openrouter_model is not None
                and state.openrouter_model != config.openrouter_model
            ):
                return True
            if (
                config.catalog_count is not None
                and state.catalog_count != config.catalog_count
            ):
                return True
            if (
                config.catalog_item_count is not None
                and state.catalog_item_count != config.catalog_item_count
            ):
                return True
            if (
                config.refresh_interval is not None
                and state.refresh_interval_seconds != config.refresh_interval
            ):
                return True
            if (
                config.response_cache is not None
                and state.response_cache_seconds != config.response_cache
            ):
                return True
            if config.metadata_addon_url is not None:
                metadata_url = str(config.metadata_addon_url)
                if (state.metadata_addon_url or None) != metadata_url:
                    return True
            return False

        if _requires_resolution(status):
            try:
                context = await service.resolve_profile(config)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            profile_id = context.state.id
            status = await service.get_profile_status(profile_id)

        if status is None:
            raise HTTPException(status_code=404, detail="Profile not found")

        return JSONResponse(status.to_payload())

    @fastapi_app.post("/api/trakt/login-url")
    async def trakt_login_url(request: Request) -> dict[str, str]:
        if not (settings.trakt_client_id and settings.trakt_client_secret):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "trakt_credentials_missing",
                    "description": (
                        "Trakt client ID and secret must be configured on the server "
                        "to enable sign in."
                    ),
                },
            )

        _prune_expired_states(fastapi_app)
        state = secrets.token_urlsafe(32)
        default_origin, redirect_uri = _resolve_trakt_redirect(request)
        origin_header = _normalize_origin_header(request.headers.get("origin"))
        referer_origin = _origin_from_url(request.headers.get("referer"))
        origin = origin_header or referer_origin or default_origin
        fastapi_app.state.trakt_oauth_states[state] = {
            "origin": origin,
            "redirect_uri": redirect_uri,
            "expires_at": time.time() + 600,
        }

        query = urlencode(
            {
                "response_type": "code",
                "client_id": settings.trakt_client_id,
                "redirect_uri": redirect_uri,
                "state": state,
            }
        )
        return {"url": f"{settings.trakt_authorize_url}?{query}"}

    @fastapi_app.get(
        "/api/trakt/callback",
        response_class=HTMLResponse,
        name="trakt_oauth_callback",
    )
    async def trakt_oauth_callback(
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        error_description: str | None = None,
    ) -> HTMLResponse:
        _prune_expired_states(fastapi_app)
        default_origin, default_redirect = _resolve_trakt_redirect(request)

        if not state:
            payload = {
                "status": "error",
                "error": "missing_state",
                "error_description": "State parameter was not returned by Trakt.",
            }
            return HTMLResponse(
                _render_oauth_popup(default_origin, payload),
                status_code=400,
            )

        state_data = fastapi_app.state.trakt_oauth_states.pop(state, None)
        origin = (state_data or {}).get("origin") or default_origin
        redirect_uri = (state_data or {}).get("redirect_uri") or default_redirect

        if not state_data or state_data.get("expires_at", 0) < time.time():
            payload = {
                "status": "error",
                "error": "state_expired",
                "error_description": "The sign-in session has expired. Please try again.",
            }
            return HTMLResponse(
                _render_oauth_popup(origin, payload),
                status_code=400,
            )

        if error:
            payload = {
                "status": "error",
                "error": error,
                "error_description": error_description
                or "Trakt reported an error during sign in.",
            }
            return HTMLResponse(_render_oauth_popup(origin, payload), status_code=400)

        if not code:
            payload = {
                "status": "error",
                "error": "missing_code",
                "error_description": "Trakt did not provide an authorisation code.",
            }
            return HTMLResponse(
                _render_oauth_popup(origin, payload),
                status_code=400,
            )

        body = {
            "code": code,
            "client_id": settings.trakt_client_id,
            "client_secret": settings.trakt_client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        try:
            response = await _post_trakt_oauth("/oauth/token", body)
        except httpx.HTTPError:
            payload = {
                "status": "error",
                "error": "network_error",
                "error_description": "Unable to reach Trakt. Please try again shortly.",
            }
            return HTMLResponse(_render_oauth_popup(origin, payload), status_code=502)

        data = _response_json(response)
        if response.status_code >= 400:
            formatted = _format_trakt_error(
                data, "Trakt rejected the authorisation request."
            )
            payload = {
                "status": "error",
                "error": formatted.get("error", "trakt_error"),
                "error_description": formatted.get(
                    "description", "Trakt rejected the authorisation request."
                ),
            }
            return HTMLResponse(
                _render_oauth_popup(origin, payload),
                status_code=response.status_code,
            )

        token_payload = {
            "access_token": data.get("access_token"),
            "refresh_token": data.get("refresh_token"),
            "expires_in": _coerce_int(data.get("expires_in")),
            "scope": data.get("scope"),
            "token_type": data.get("token_type"),
            "created_at": _coerce_int(data.get("created_at")),
        }
        payload = {
            "status": "success",
            "tokens": {
                key: value
                for key, value in token_payload.items()
                if value not in {None, ""}
            },
        }
        return HTMLResponse(_render_oauth_popup(origin, payload))


def _prune_expired_states(fastapi_app: FastAPI) -> None:
    store = getattr(fastapi_app.state, "trakt_oauth_states", {})
    now = time.time()
    expired = [key for key, info in store.items() if info.get("expires_at", 0) <= now]
    for key in expired:
        store.pop(key, None)


def _resolve_trakt_redirect(request: Request) -> tuple[str, str]:
    if settings.trakt_redirect_uri:
        parsed = urlparse(str(settings.trakt_redirect_uri))
        origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        return origin, str(settings.trakt_redirect_uri)

    origin, base = _resolve_external_base(request)
    path = request.app.url_path_for("trakt_oauth_callback")
    return origin, f"{base}{path}"


def _resolve_external_base(request: Request) -> tuple[str, str]:
    headers = request.headers
    scheme = _first_forwarded_value(headers.get("x-forwarded-proto")) or request.url.scheme

    host = _first_forwarded_value(headers.get("x-forwarded-host"))
    if not host:
        host_header = headers.get("host")
        host = _first_forwarded_value(host_header) if host_header else None
    if not host:
        host = request.url.netloc

    port = _first_forwarded_value(headers.get("x-forwarded-port"))
    if port and ":" not in host:
        default_port = "443" if scheme == "https" else "80"
        if port != default_port:
            host = f"{host}:{port}"

    origin = f"{scheme}://{host}".rstrip("/")

    prefix = (
        _first_forwarded_value(headers.get("x-forwarded-prefix"))
        or request.scope.get("root_path")
        or ""
    )
    if prefix and not prefix.startswith("/"):
        prefix = f"/{prefix}"
    prefix = prefix.rstrip("/")

    base = f"{origin}{prefix}" if prefix else origin
    return origin, base


def _first_forwarded_value(header_value: str | None) -> str | None:
    if not header_value:
        return None
    return header_value.split(",", 1)[0].strip()


def _normalize_origin_header(origin_value: str | None) -> str | None:
    if not origin_value:
        return None
    origin_value = origin_value.strip()
    if not origin_value or origin_value.lower() == "null":
        return None
    return origin_value.rstrip("/")


def _origin_from_url(url_value: str | None) -> str | None:
    if not url_value:
        return None
    try:
        parsed = urlparse(url_value)
    except ValueError:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _render_oauth_popup(target_origin: str, payload: dict[str, Any]) -> str:
    message = {"source": "trakt-oauth", **payload}
    status = str(payload.get("status") or "").lower()
    tokens = message.get("tokens")
    if status == "success" and isinstance(tokens, dict):
        message.setdefault("type", "TRAKT_AUTH_SUCCESS")
        for key in ("access_token", "refresh_token", "expires_in", "scope", "token_type"):
            value = tokens.get(key)
            if value not in {None, ""}:
                message.setdefault(key, value)
    elif status:
        message.setdefault("type", "TRAKT_AUTH_ERROR")

    json_payload = json.dumps(message).replace("</", "<\\/")
    origin = target_origin or "*"
    return f"""
<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <title>Trakt Sign In</title>
</head>
<body>
    <p>You can close this window and return to the configuration tab.</p>
    <script>
        (function() {{
            const payload = {json_payload};
            const targetOrigin = {json.dumps(origin)};
            let notified = false;
            try {{
                if (window.opener && !window.opener.closed) {{
                    window.opener.postMessage(payload, targetOrigin);
                    notified = true;
                }}
            }} catch (err) {{
                console.error('Unable to notify opener via postMessage', err);
            }}
            if (!notified && window.opener && !window.opener.closed && targetOrigin !== '*') {{
                try {{
                    window.opener.postMessage(payload, '*');
                    notified = true;
                }} catch (err) {{
                    console.error('Unable to notify opener via wildcard postMessage', err);
                }}
            }}
            try {{
                if ('BroadcastChannel' in window) {{
                    const channel = new BroadcastChannel('aiopicks.trakt-oauth');
                    channel.postMessage(payload);
                    channel.close();
                    notified = true;
                }}
            }} catch (err) {{
                console.error('Unable to broadcast Trakt OAuth payload', err);
            }}
            if (!notified) {{
                console.warn('Trakt OAuth payload could not be delivered to the opener context.');
            }}
            setTimeout(() => {{
                window.close();
            }}, 150);
        }})();
    </script>
</body>
</html>
"""


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
