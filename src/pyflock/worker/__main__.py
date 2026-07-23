"""Entry point: ``python -m pyflock.worker``."""

from __future__ import annotations

import logging

from pyflock.core.db import create_all
from pyflock.worker.agent import WorkerAgent


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    create_all()
    WorkerAgent().run()


if __name__ == "__main__":
    main()
