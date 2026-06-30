from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import re

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..repository import events_model_to_database, json_to_event_model
from ..retry import DbRetryConfig, db_retry_config_from_env, run_with_db_retry
from ..storage import ensure_storage_layout, layout_from_env, load_env
from ..db_config import (
    DbConfig,
    build_sqlalchemy_url,
    configure_orm_schema,
    db_config_from_env,
    maybe_initialize_database,
)

_AUDITLOG_DATE_RE = re.compile(r"auditlog-(\d{4}-\d{2}-\d{2})_")


def _extract_date_folder_from_json(name: str) -> str:
    """Extrait YYYYMMDD depuis un nom auditlog-YYYY-MM-DD_*.json."""
    m = _AUDITLOG_DATE_RE.search(name)
    if m:
        return m.group(1).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def _open_session(db_config: DbConfig) -> Session:
    configure_orm_schema(db_config.schema)
    url = build_sqlalchemy_url(db_config)
    engine = create_engine(url, future=True, pool_pre_ping=True)
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


def _persist_json_file_with_retry(
    json_path: Path,
    db_config: DbConfig,
    retry_config: DbRetryConfig,
) -> int:
    """Persiste un fichier JSON avec retry et session dédiée par tentative."""

    def _attempt() -> int:
        session = _open_session(db_config)
        try:
            return persist_json_file_to_db(json_path, session)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return run_with_db_retry(
        _attempt,
        retry_config,
        operation_name=f"persistance {json_path.name}",
    )


def persist_all_json_to_db(*, skip_init: bool = False) -> None:
    """
    Parcourt le dossier json_data/ et persiste tous les fichiers *.json
    dans la base (schema configurable via DB_SCHEMA), avec retry sur erreurs transitoires.

    Les fichiers en échec définitif restent dans json_data/ (non archivés).
    """
    project_root = Path(__file__).resolve().parent.parent
    env = load_env(project_root / ".env")
    if skip_init:
        db_config = db_config_from_env(env)
        configure_orm_schema(db_config.schema)
    else:
        db_config = maybe_initialize_database(project_root, env)
    layout = layout_from_env(env, project_root)
    ensure_storage_layout(layout)
    retry_config = db_retry_config_from_env(env)

    json_data_dir = layout.json_data_dir
    archive_json_dir = layout.archive_json_dir

    total = 0
    failed = 0

    for json_path in sorted(json_data_dir.glob("*.json")):
        try:
            inserted = _persist_json_file_with_retry(json_path, db_config, retry_config)
            print(f"✓ {inserted} événements insérés depuis {json_path}")
            total += inserted

            date_folder = _extract_date_folder_from_json(json_path.name)
            archive_json_date_dir = archive_json_dir / date_folder
            archive_json_date_dir.mkdir(parents=True, exist_ok=True)
            target = archive_json_date_dir / json_path.name
            json_path.replace(target)
        except Exception as exc:
            failed += 1
            print(
                f"✗ Échec persistance {json_path.name} après {retry_config.max_retries} "
                f"tentative(s): {exc}",
                flush=True,
            )

    print(f"Total événements insérés : {total}")
    if failed:
        print(f"Fichiers JSON non archivés (échec DB) : {failed}")


if __name__ == "__main__":
    persist_all_json_to_db()
