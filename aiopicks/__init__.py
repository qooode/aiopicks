"""Compatibility shim exposing the main FastAPI app."""

from __future__ import annotations

from app.main import app, create_app

__all__ = ["app", "create_app"]
