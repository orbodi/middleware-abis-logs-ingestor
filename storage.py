"""Arborescence ROOT_DIR et création automatique des dossiers."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

_DEFAULT_DIR_MODE = 0o755


def load_env(env_path: Path) -> Dict[str, str]:
    """Charge un fichier .env simple, puis fusionne avec os.environ."""
    env: Dict[str, str] = {}
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    merged = {**env}
    merged.update(os.environ)
    return merged


def _ensure_dir(path: Path, mode: int = _DEFAULT_DIR_MODE) -> None:
    """Crée un répertoire (et ses parents) et applique les permissions si possible."""
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        try:
            os.chmod(path, mode)
        except OSError:
            pass


@dataclass
class StorageLayout:
    """Chemins dérivés de ROOT_DIR."""

    root_dir: Path
    inputs_dir: Path
    processing_dir: Path
    json_data_dir: Path
    archive_dir: Path
    archive_inputs_dir: Path
    archive_json_dir: Path
    errors_dir: Path
    logs_dir: Path
    backups_dir: Path

    @property
    def storage_root(self) -> Path:
        """Alias rétrocompatible."""
        return self.root_dir


def layout_from_env(env: Dict[str, str], project_root: Path) -> StorageLayout:
    """
    Dérive l'arborescence à partir de ROOT_DIR (ou STORAGE_DIR en fallback).

    Arborescence créée sous ROOT_DIR :
        inputs/              ← .log.gz (entrée, ex. depuis logspipe-relay)
        inputs/processing_data/  ← .log à parser
        json_data/
        archives/inputs/
        archives/json_data/
        errors/
        logs/
        backups/
    """
    root_env = env.get("ROOT_DIR", "").strip() or env.get("STORAGE_DIR", "storage").strip()
    root_dir = Path(root_env)
    if not root_dir.is_absolute():
        root_dir = project_root / root_dir

    archive_dir = root_dir / "archives"

    return StorageLayout(
        root_dir=root_dir,
        inputs_dir=root_dir / "inputs",
        processing_dir=root_dir / "inputs" / "processing_data",
        json_data_dir=root_dir / "json_data",
        archive_dir=archive_dir,
        archive_inputs_dir=archive_dir / "inputs",
        archive_json_dir=archive_dir / "json_data",
        errors_dir=root_dir / "errors",
        logs_dir=root_dir / "logs",
        backups_dir=root_dir / "backups",
    )


def ensure_storage_layout(layout: StorageLayout) -> None:
    """Crée ROOT_DIR et tous les sous-dossiers nécessaires."""
    for path in (
        layout.root_dir,
        layout.inputs_dir,
        layout.processing_dir,
        layout.json_data_dir,
        layout.archive_dir,
        layout.archive_inputs_dir,
        layout.archive_json_dir,
        layout.errors_dir,
        layout.logs_dir,
        layout.backups_dir,
    ):
        _ensure_dir(path)
