"""Permet python3 -m abis_logs_ingestor depuis le dossier parent (ou via run.py)."""
from __future__ import annotations

from bootstrap import install_package

install_package()

from abis_logs_ingestor.handler import main  # noqa: E402

main()
