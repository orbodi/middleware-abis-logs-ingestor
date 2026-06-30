#!/usr/bin/env python3
"""
Point d'entrée CLI — à lancer depuis le dossier du projet.

    python3 run.py
    python3 run.py --no-cleanup
    python3 run.py /chemin/vers/auditlog.log
"""
from __future__ import annotations

from bootstrap import install_package

install_package()

from abis_logs_ingestor.handler import main  # noqa: E402

if __name__ == "__main__":
    main()
