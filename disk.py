"""Mesure de l'espace disque disponible."""
from __future__ import annotations

import os
import shutil
from pathlib import Path


def get_available_gb(path: Path) -> float:
    """Espace libre en Go sur le volume contenant path."""
    try:
        stat = os.statvfs(path)
        return (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
    except AttributeError:
        _, _, free = shutil.disk_usage(path)
        return free / (1024 ** 3)
