"""
Script d'initialisation de la base de données pour abis_logs_ingestor.

Ce script :
- lit la configuration de connexion dans un fichier .env,
- applique le fichier SQL `schema.sql` pour créer le schéma et les tables.

Prérequis : installer le driver PostgreSQL, par exemple :

    pip install psycopg2-binary

Variables d'environnement attendues (dans .env) :
- DB_DSN               (optionnel, ex: postgresql://user:pwd@host:5432/dbname)
ou bien :
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import psycopg2


def load_env(env_path: Path) -> Dict[str, str]:
    """Charge un fichier .env très simple, puis fusionne avec os.environ."""
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


def build_dsn(env: Dict[str, str]) -> str:
    """Construit une DSN PostgreSQL à partir de DB_DSN ou des variables séparées."""
    if "DB_DSN" in env and env["DB_DSN"]:
        return env["DB_DSN"]

    host = env.get("DB_HOST", "localhost")
    port = env.get("DB_PORT", "5432")
    name = env.get("DB_NAME", "abis")
    user = env.get("DB_USER", "postgres")
    password = env.get("DB_PASSWORD", "")

    return f"dbname={name} user={user} password={password} host={host} port={port}"


def apply_schema(conn, schema_path: Path) -> None:
    """Exécute le script SQL de schéma sur la base cible."""
    sql = schema_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main() -> None:
    here = Path(__file__).resolve().parent
    project_root = here.parent

    # On lit la configuration dans ../.env (copie de env.sample à adapter)
    env = load_env(project_root / ".env")
    dsn = build_dsn(env)

    print(f"Connexion à la base de données avec DSN: {dsn!r}")
    conn = psycopg2.connect(dsn)
    try:
        schema_path = here / "schema.sql"
        print(f"Application du schéma depuis {schema_path}")
        apply_schema(conn, schema_path)
        print("✓ Schéma appliqué avec succès.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()


