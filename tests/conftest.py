"""Pytest configuration and test helpers."""

from __future__ import annotations

import sys
from pathlib import Path


# Ensure the application package is importable when running tests without an
# editable install. This mirrors the expected runtime layout where ``app`` sits
# at the project root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


