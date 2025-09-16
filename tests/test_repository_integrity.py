"""Repository-level integrity checks."""

from __future__ import annotations

import re
from pathlib import Path

CONFLICT_PATTERN = re.compile(r"^(<<<<<<<|=======|>>>>>>>)", re.MULTILINE)
IGNORED_PARTS = {".git", "__pycache__", ".mypy_cache", ".pytest_cache", ".venv"}


def test_repository_has_no_merge_conflict_markers() -> None:
    """Ensure no files in the repo still contain git conflict markers."""

    repo_root = Path(__file__).resolve().parents[1]
    offending_files: list[Path] = []

    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in IGNORED_PARTS for part in path.parts):
            continue

        try:
            contents = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            contents = path.read_text(encoding="utf-8", errors="ignore")

        if CONFLICT_PATTERN.search(contents):
            offending_files.append(path.relative_to(repo_root))

    assert not offending_files, (
        "The following files still contain git conflict markers: "
        + ", ".join(str(path) for path in offending_files)
    )
