"""Entry point: ``python -m pyflock.scheduler``."""

from __future__ import annotations

import logging

from pyflock.core.db import create_all
from pyflock.scheduler.reaper import Reaper


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    create_all()
    Reaper().run()


if __name__ == "__main__":
    main()
