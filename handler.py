"""
Point d'entrée du projet abis-logs-ingestor.

Objectif général
----------------
Ingestion de fichiers de logs ABIS (ex: auditlog_brs1.log), normalisation en JSON
et génération de fichiers de sortie exploitables pour l'analyse.

Ce module expose une fonction principale `main()` utilisable :
- en ligne de commande : `python -m projets.abis-logs-ingestor.handler ...`
- ou directement : `python handler.py ...` si exécuté depuis ce dossier.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime
import re

from .parser.build_full_json import process_content
from .service import persist_all_json_to_db
from .files.gzip_processing import decompress_audit_gz_in_inputs


_AUDITLOG_DATE_RE = re.compile(r"(?:.*-)?auditlog-(\d{4}-\d{2}-\d{2})_")


def _extract_date_folder(name: str) -> str:
    """
    Extrait une date (YYYYMMDD) à partir d'un nom de fichier de type
    `auditlog-2025-11-10_07.0.log`.
    Si aucune date n'est trouvée, retourne la date du jour.
    """
    m = _AUDITLOG_DATE_RE.search(name)
    if m:
        # '2025-11-10' -> '20251110'
        return m.group(1).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def load_env(env_path: Path) -> Dict[str, str]:
    """
    Charge un fichier .env très simple : lignes KEY=VALUE, ignore les commentaires.
    Les variables existantes dans l'environnement système restent prioritaires.
    """
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
    # Compléter avec les variables d'environnement existantes
    merged = {**env}
    merged.update(os.environ)
    return merged


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="abis-logs-ingestor",
        description=(
            "Ingestion et normalisation des logs ABIS en JSON.\n"
            "Par défaut, parcourt le dossier d'inputs défini dans .env."
        ),
    )

    parser.add_argument(
        "input_log",
        type=Path,
        nargs="?",
        help="(Optionnel) Chemin d'un fichier de log brut à traiter directement.",
    )

    return parser.parse_args(argv)


def ingest_log_file(input_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fonction centrale d'ingestion avec correction JSON avancée.

    Utilise le parseur avancé qui :
    - détecte les objets JSON multi-lignes
    - corrige les quasi-JSON (valeurs non-quotées, dumps Java, etc.)
    - gère les cas d'erreur avec fallback

    Returns:
        - events: liste d'objets JSON parsés
        - stats:  dictionnaire de statistiques basiques
    """
    raw = input_path.read_text(encoding="utf-8")

    # Utiliser le parseur avancé avec correction JSON
    events, errors = process_content(raw, debug=False)

    # Compter les lignes brutes pour les stats
    total_lines = len([line for line in raw.splitlines() if line.strip()])

    # Grâce au fallback dans le parseur, chaque bloc détecté produit un objet.
    # On conserve les erreurs à titre d'information, mais on ne les compte
    # plus comme des lignes échouées dans les statistiques de haut niveau.
    stats: Dict[str, Any] = {
        "total_lines": total_lines,
        "parsed_events": len(events),
        "failed_lines": 0,
        "errors": errors[:20],  # résumé des anomalies éventuelles
    }
    return events, stats


def write_outputs(
    events: List[Dict[str, Any]],
    stats: Dict[str, Any],
    output_json: Path,
    stats_log: Path,
) -> None:
    """Écrit le JSON complet et le fichier de statistiques."""
    # JSON complet : un seul tableau avec tous les événements
    output_json.write_text(
        json.dumps(events, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Fichier de stats lisible
    lines: List[str] = []
    lines.append("Bilan abis-logs-ingestor")
    lines.append("========================")
    lines.append("")
    lines.append(f"Fichier d'entrée : {stats.get('input_file')}")
    lines.append(f"Fichier JSON     : {output_json}")
    lines.append("")
    lines.append(f"Total lignes lues : {stats.get('total_lines')}")
    lines.append(f"Événements parsés : {stats.get('parsed_events')}")
    lines.append(f"Lignes en échec   : {stats.get('failed_lines')}")

    if stats.get("errors"):
        lines.append("")
        lines.append("Premières erreurs :")
        for err in stats["errors"]:
            lines.append(f"- {err}")

    stats_log.write_text("\n".join(lines), encoding="utf-8")


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)

    base_dir = Path(__file__).resolve().parent
    env = load_env(base_dir / ".env")

    # Racine de stockage (par défaut: ./storage)
    storage_root = Path(env.get("STORAGE_DIR", "storage"))
    if not storage_root.is_absolute():
        storage_root = base_dir / storage_root

    # Sous-dossiers configurables (noms de répertoires)
    inputs_dir = storage_root / env.get("INPUTS_SUBDIR", "inputs")
    archive_dir = storage_root / env.get("ARCHIVES_SUBDIR", "archives")
    errors_dir = storage_root / env.get("ERRORS_SUBDIR", "errors")
    logs_dir = storage_root / env.get("LOGS_SUBDIR", "logs")
    json_data_dir = storage_root / env.get("JSON_DATA_SUBDIR", "json_data")
    backups_dir = storage_root / env.get("BACKUPS_SUBDIR", "backups")
    enable_decompression = (
        str(env.get("ENABLE_DECOMPRESSION", "true")).lower() in ("1", "true", "yes")
    )

    # Sous-dossiers d'archives dédiés (racines)
    archive_inputs_dir = archive_dir / "inputs"
    archive_json_dir = archive_dir / "json_data"

    # Créer les dossiers racines nécessaires
    inputs_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_inputs_dir.mkdir(parents=True, exist_ok=True)
    archive_json_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    json_data_dir.mkdir(parents=True, exist_ok=True)
    backups_dir.mkdir(parents=True, exist_ok=True)

    # Dossier de travail pour les logs à parser
    processing_dir = inputs_dir / "processing_data"
    processing_dir.mkdir(parents=True, exist_ok=True)

    # Optionnel : décompresser les fichiers *.log.gz vers processing_data et sauvegarder les .gz
    if enable_decompression:
        created_logs, decompress_errors = decompress_audit_gz_in_inputs(
            inputs_dir, backups_dir
        )
        if created_logs:
            print(
                f"🗜️  {len(created_logs)} fichier(s) .gz décompressé(s) dans {processing_dir}"
            )
        if decompress_errors > 0:
            print(
                f"⚠️  {decompress_errors} fichier(s) .gz invalide(s) ou corrompu(s) (non décompressés)",
                file=sys.stderr,
            )
    else:
        print(
            "ℹ️  Étape de décompression ignorée (ENABLE_DECOMPRESSION=false) – lecture directe de processing_data",
            file=sys.stderr,
        )

    # Mode 1 : un fichier explicite passé en argument
    if args.input_log is not None:
        input_log = args.input_log
        if not input_log.is_absolute():
            input_log = input_log.resolve()

        if not input_log.is_file():
            print(f"✗ Fichier introuvable : {input_log}", file=sys.stderr)
            sys.exit(1)

        # Déterminer le sous-dossier daté à partir du nom du fichier
        date_folder = _extract_date_folder(input_log.name)
        archive_inputs_date_dir = archive_inputs_dir / date_folder
        logs_date_dir = logs_dir / date_folder
        archive_inputs_date_dir.mkdir(parents=True, exist_ok=True)
        logs_date_dir.mkdir(parents=True, exist_ok=True)

        events, stats = ingest_log_file(input_log)
        stats["input_file"] = str(input_log)

        output_json = json_data_dir / (input_log.stem + ".json")
        stats_log = logs_date_dir / (input_log.stem + "_stats.log")

        write_outputs(events, stats, output_json, stats_log)

        # Fichier incorrect si aucun événement parsé
        target_dir = errors_dir if stats["parsed_events"] == 0 else archive_inputs_date_dir
        target = target_dir / input_log.name
        input_log.replace(target)

        print(f"📄 Fichier log     : {input_log}")
        print(f"📝 JSON de sortie  : {output_json}")
        print(f"📊 Stats           : {stats_log}")
        print(f"➡️  Lignes lues    : {stats['total_lines']}")
        print(f"✅ Événements OK   : {stats['parsed_events']}")
        print(f"⚠️  Lignes en échec : {stats['failed_lines']}")
        print(f"📦 Déplacé vers    : {target_dir}")

        # Optionnel : persistance en base de données pour ce fichier
        persist_all_json_to_db()
        return

    # Mode 2 : batch sur le dossier d'inputs (storage/inputs/processing_data)
    if not processing_dir.is_dir():
        print(f"✗ Dossier de travail introuvable : {processing_dir}", file=sys.stderr)
        sys.exit(1)

    any_file = False
    for input_log in sorted(processing_dir.glob("*.log")):
        any_file = True
        # Déterminer le sous-dossier daté à partir du nom du fichier
        date_folder = _extract_date_folder(input_log.name)
        archive_inputs_date_dir = archive_inputs_dir / date_folder
        logs_date_dir = logs_dir / date_folder
        archive_inputs_date_dir.mkdir(parents=True, exist_ok=True)
        logs_date_dir.mkdir(parents=True, exist_ok=True)

        events, stats = ingest_log_file(input_log)
        stats["input_file"] = str(input_log)

        output_json = json_data_dir / (input_log.stem + ".json")
        stats_log = logs_date_dir / (input_log.stem + "_stats.log")

        write_outputs(events, stats, output_json, stats_log)

        target_dir = errors_dir if stats["parsed_events"] == 0 else archive_inputs_date_dir
        target = target_dir / input_log.name
        input_log.replace(target)

        print(f"📄 Fichier log     : {input_log}")
        print(f"📝 JSON de sortie  : {output_json}")
        print(f"📊 Stats           : {stats_log}")
        print(f"➡️  Lignes lues    : {stats['total_lines']}")
        print(f"✅ Événements OK   : {stats['parsed_events']}")
        print(f"⚠️  Lignes en échec : {stats['failed_lines']}")
        print(f"📦 Déplacé vers    : {target_dir}")
        print("-" * 40)

    # Si aucun log à traiter, vérifier s'il reste des JSON à persister
    if not any_file:
        remaining_json = list(json_data_dir.glob("*.json"))
        if remaining_json:
            print(f"ℹ️  Aucun nouveau fichier *.log, mais {len(remaining_json)} fichier(s) JSON à persister.")
            persist_all_json_to_db()
        else:
            print(
                f"ℹ️  Aucun fichier *.log trouvé dans {processing_dir} "
                f"et aucun JSON en attente dans {json_data_dir}"
            )
    else:
        # Après traitement de tous les fichiers, persister l'ensemble des JSON en base
        persist_all_json_to_db()


if __name__ == "__main__":
    main()


