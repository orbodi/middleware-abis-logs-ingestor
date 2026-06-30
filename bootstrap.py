"""Monte le package abis_logs_ingestor quel que soit le nom du dossier projet."""
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

PKG = "abis_logs_ingestor"


def install_package(project_root: Path | None = None) -> str:
    root = project_root or Path(__file__).resolve().parent
    root_str = str(root)

    parent = str(root.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    existing = sys.modules.get(PKG)
    if existing is not None and list(getattr(existing, "__path__", [])) == [root_str]:
        return PKG

    pkg = ModuleType(PKG)
    pkg.__path__ = [root_str]  # type: ignore[attr-defined]
    sys.modules[PKG] = pkg

    return PKG
