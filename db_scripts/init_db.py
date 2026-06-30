"""
Initialisation manuelle de la base de données.

    python db_scripts/init_db.py

Equivalent a INIT_DB=true dans .env puis lancement du handler.
"""

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from db_config import db_config_from_env, initialize_database  # noqa: E402
from storage import load_env  # noqa: E402


def main() -> None:
    env = load_env(project_root / ".env")
    config = db_config_from_env(env)
    initialize_database(project_root, config)


if __name__ == "__main__":
    main()
