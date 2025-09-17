"""Module executed when running ``python -m aiopicks``."""

from __future__ import annotations

import uvicorn

from app.config import settings


def main() -> None:
    """Start the uvicorn server using the configured settings."""

    uvicorn.run(
        "app.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.environment == "development",
    )


if __name__ == "__main__":  # pragma: no cover - runtime entrypoint
    main()
