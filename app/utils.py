"""Utility helpers for the AIOPicks service."""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any


JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
BARE_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def slugify(value: str) -> str:
    """Return a URL-friendly slug."""

    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value)
    value = value.strip("-")
    value = re.sub(r"-+", "-", value)
    return value.lower() or "catalog"


def extract_json_object(content: str) -> dict[str, Any]:
    """Extract and parse the first JSON object from the model response."""

    match = JSON_BLOCK_RE.search(content)
    if match:
        payload = match.group(1)
    else:
        match = BARE_JSON_RE.search(content)
        if not match:
            raise ValueError("No JSON object found in response")
        payload = match.group(0)

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive branch
        raise ValueError("Invalid JSON payload produced by the model") from exc


def ensure_unique_meta_id(base_id: str, fallback: str, index: int) -> str:
    """Generate a deterministic unique meta identifier."""

    if base_id:
        return base_id
    slug = slugify(fallback)
    if not slug:
        slug = "meta"
    return f"{slug}-{index}"
