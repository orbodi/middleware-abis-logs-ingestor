#!/usr/bin/env python3
"""Nettoyage ROOT_DIR (age + espace disque). Usage: python run_cleanup.py"""
from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from cleanup import cleanup_config_from_env, run_cleanup  # noqa: E402
from storage import ensure_storage_layout, layout_from_env, load_env  # noqa: E402


def main() -> int:
    env = load_env(project_root / ".env")
    layout = layout_from_env(env, project_root)
    ensure_storage_layout(layout)
    config = cleanup_config_from_env(env)

    stats = run_cleanup(layout, config)
    age = stats.get("age") or {}
    disk = stats.get("disk") or {}

    age_deleted = sum(s.get("deleted", 0) for s in age.values() if isinstance(s, dict))
    print(f"\nCleanup: age={age_deleted}, disk={disk.get('deleted', 0)}")
    if disk.get("triggered"):
        print(
            f"Disk: {disk.get('available_gb_before')} -> "
            f"{disk.get('available_gb_after')} GB free"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
