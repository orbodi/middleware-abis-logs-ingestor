from __future__ import annotations

import gzip
import shutil
from pathlib import Path
from typing import List
import re


_AUDITLOG_DATE_RE = re.compile(r"auditlog-(\d{4}-\d{2}-\d{2})_")


def _extract_date_folder(name: str) -> str:
    """
    Extrait une date (YYYYMMDD) à partir d'un nom de fichier de type
    `auditlog-2025-11-10_07.0.log.gz`. Si aucune date n'est trouvée,
    utilise la date du jour.
    """
    from datetime import datetime

    m = _AUDITLOG_DATE_RE.search(name)
    if m:
        return m.group(1).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def decompress_audit_gz_in_inputs(inputs_dir: Path, backups_root: Path | None = None) -> List[Path]:
    """
    Parcourt le dossier `inputs_dir`, trouve les fichiers
    de type `auditlog-*.log.gz`, les décompresse et place
    les fichiers `.log` résultants dans le sous-dossier
    `processing_data` de `inputs_dir`.

    Exemple :
        inputs/
          auditlog-2025-11-10_07.0.log.gz  →  inputs/processing_data/auditlog-2025-11-10_07.0.log

    Args:
        inputs_dir: chemin du dossier `inputs` (racine des fichiers à traiter).

    Returns:
        Liste des chemins des fichiers `.log` créés dans `processing_data`.
    """
    inputs_dir = inputs_dir.resolve()
    processing_dir = inputs_dir / "processing_data"
    processing_dir.mkdir(parents=True, exist_ok=True)

    created_logs: List[Path] = []

    # On prend tous les fichiers *.log.gz présents dans inputs_dir
    for gz_path in sorted(inputs_dir.glob("*.log.gz")):
        # Nom du fichier .log à l'intérieur (on retire seulement le suffixe .gz)
        log_name = gz_path.name[:-3]  # supprime le suffixe ".gz"
        target_log_path = processing_dir / log_name

        # Décompresser uniquement si le .log n'existe pas déjà
        if not target_log_path.exists():
            with gzip.open(gz_path, "rb") as f_in, open(target_log_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            created_logs.append(target_log_path)

        # Sauvegarder le fichier .gz dans un dossier de backup daté, si demandé
        if backups_root is not None:
            date_folder = _extract_date_folder(gz_path.name)
            backup_dir = backups_root / date_folder
            backup_dir.mkdir(parents=True, exist_ok=True)
            target_gz = backup_dir / gz_path.name
            if not target_gz.exists():
                gz_path.replace(target_gz)

    return created_logs


