"""
Package `abis_logs_ingestor`
============================

Outils d'ingestion et de normalisation des logs ABIS en JSON.

Points d'entrée principaux :
- `main()` : CLI principale (voir `handler.py`)
- `ingest_log_file()` : fonction de haut niveau pour parser un fichier de log.
"""

from .handler import ingest_log_file, main

__all__ = ["ingest_log_file", "main"]


