from __future__ import annotations

import gzip
import shutil
from pathlib import Path
from typing import List


def decompress_audit_gz_in_inputs(inputs_dir: Path) -> List[Path]:
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

    for gz_path in sorted(inputs_dir.glob("auditlog-*.log.gz")):
        # Nom du fichier .log à l'intérieur
        log_name = gz_path.name[:-3]  # supprime le suffixe ".gz"
        target_log_path = processing_dir / log_name

        # Décompresser uniquement si le .log n'existe pas déjà
        if not target_log_path.exists():
            with gzip.open(gz_path, "rb") as f_in, open(target_log_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            created_logs.append(target_log_path)

    return created_logs


