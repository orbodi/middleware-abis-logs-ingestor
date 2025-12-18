from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List
from datetime import datetime
import re

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..repository import events_model_to_database, json_to_event_model


def _load_env(env_path: Path) -> Dict[str, str]:
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


def _build_db_url(env: Dict[str, str]) -> str:
    """Construit une URL SQLAlchemy PostgreSQL à partir de DB_DSN ou des champs séparés."""
    if env.get("DB_DSN"):
        return env["DB_DSN"]

    host = env.get("DB_HOST", "localhost")
    port = env.get("DB_PORT", "5432")
    name = env.get("DB_NAME", "abis")
    user = env.get("DB_USER", "postgres")
    password = env.get("DB_PASSWORD", "")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


_AUDITLOG_DATE_RE = re.compile(r"auditlog-(\d{4}-\d{2}-\d{2})_")


def _extract_date_folder_from_json(name: str) -> str:
    """
    Extrait une date (YYYYMMDD) à partir d'un nom de fichier JSON de type
    `auditlog-2025-11-10_07.0.json`.
    Si aucune date n'est trouvée, retourne la date du jour.
    """
    m = _AUDITLOG_DATE_RE.search(name)
    if m:
        return m.group(1).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def _open_session(project_root: Path) -> Session:
    env = _load_env(project_root / ".env")
    url = _build_db_url(env)
    engine = create_engine(url, future=True)
    return Session(engine)


def _iter_events_from_file(json_path: Path):
    """Charge un fichier JSON d'événements et le transforme en modèles Event."""
    content = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(content, list):
        raise ValueError(f"{json_path} ne contient pas un tableau JSON d'événements.")
    source_file = str(json_path)
    for obj in content:
        if not isinstance(obj, dict):
            continue
        yield json_to_event_model(obj, source_file=source_file)


def persist_json_file_to_db(json_path: Path, session: Session) -> int:
    """
    Persiste un fichier JSON d'événements (généré par le handler) dans la base.

    Returns:
        Nombre d'événements insérés.
    """
    events = list(_iter_events_from_file(json_path))
    if not events:
        return 0
    ids = events_model_to_database(events, session)
    return len(ids)


def persist_all_json_to_db() -> None:
    """
    Parcourt le dossier storage/json_data et persiste tous les fichiers *.json
    dans la base `abis.events`.

    Cette fonction peut être appelée depuis le handler après la génération
    des fichiers JSON, ou bien exécutée comme script autonome.
    """
    project_root = Path(__file__).resolve().parent.parent

    # Charger la configuration storage depuis .env
    env = _load_env(project_root / ".env")

    storage_root = Path(env.get("STORAGE_DIR", "storage"))
    if not storage_root.is_absolute():
        storage_root = project_root / storage_root

    json_data_dir = storage_root / env.get("JSON_DATA_SUBDIR", "json_data")

    # Dossier d'archives pour les JSON déjà persistés
    archive_dir = storage_root / env.get("ARCHIVES_SUBDIR", "archives")
    archive_json_dir = archive_dir / "json_data"

    json_data_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_json_dir.mkdir(parents=True, exist_ok=True)

    session = _open_session(project_root)
    try:
        total = 0
        for json_path in sorted(json_data_dir.glob("*.json")):
            inserted = persist_json_file_to_db(json_path, session)
            print(f"✓ {inserted} événements insérés depuis {json_path}")
            total += inserted
            # Déterminer le sous-dossier daté à partir du nom du fichier JSON
            date_folder = _extract_date_folder_from_json(json_path.name)
            archive_json_date_dir = archive_json_dir / date_folder
            archive_json_date_dir.mkdir(parents=True, exist_ok=True)
            # Archiver le fichier JSON traité dans un sous-dossier daté
            target = archive_json_date_dir / json_path.name
            json_path.replace(target)
        print(f"Total événements insérés : {total}")
    finally:
        session.close()


if __name__ == "__main__":
    persist_all_json_to_db()


