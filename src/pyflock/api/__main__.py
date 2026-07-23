"""Entry point: ``python -m pyflock.api`` (runs uvicorn)."""

from __future__ import annotations

import uvicorn

from pyflock.config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "pyflock.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
