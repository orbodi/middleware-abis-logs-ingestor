"""Configuration base de données et initialisation du schéma."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import psycopg2

_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class DbConfig:
    host: str
    port: str
    name: str
    user: str
    password: str
    schema: str
    init_db: bool
    dsn: str | None = None

    @property
    def qualified_table(self) -> str:
        return f"{self.schema}.events"


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def validate_schema_name(schema: str) -> str:
    schema = schema.strip()
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError(
            f"Nom de schéma invalide {schema!r} — autorise : lettres, chiffres, underscore"
        )
    return schema


def db_config_from_env(env: Dict[str, str]) -> DbConfig:
    schema = validate_schema_name(env.get("DB_SCHEMA", "abis"))
    return DbConfig(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        name=env.get("DB_NAME", "abis"),
        user=env.get("DB_USER", "postgres"),
        password=env.get("DB_PASSWORD", ""),
        schema=schema,
        init_db=_parse_bool(env.get("INIT_DB"), default=False),
        dsn=env.get("DB_DSN") or None,
    )


def build_psycopg2_dsn(config: DbConfig) -> str:
    if config.dsn:
        dsn = config.dsn
        if dsn.startswith("postgresql+psycopg2://"):
            dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
        if dsn.startswith("postgresql://"):
            return dsn.replace("postgresql://", "", 1)
        return dsn

    return (
        f"dbname={config.name} user={config.user} password={config.password} "
        f"host={config.host} port={config.port}"
    )


def build_sqlalchemy_url(config: DbConfig) -> str:
    if config.dsn:
        dsn = config.dsn
        if dsn.startswith("postgresql://") and "+psycopg2" not in dsn:
            return dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
        return dsn

    return (
        f"postgresql+psycopg2://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.name}"
    )


def load_schema_sql(project_root: Path, schema: str) -> str:
    schema_path = project_root / "db_scripts" / "schema.sql"
    sql_template = schema_path.read_text(encoding="utf-8")
    return sql_template.replace("__DB_SCHEMA__", validate_schema_name(schema))


def initialize_database(project_root: Path, config: DbConfig) -> None:
    """Applique schema.sql (CREATE SCHEMA / TABLE / VIEWS). Idempotent."""
    dsn = build_psycopg2_dsn(config)
    sql = load_schema_sql(project_root, config.schema)

    print(f"[db] Connexion a {config.host}:{config.port}/{config.name}")
    print(f"[db] Initialisation du schéma {config.schema!r}")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"[db] Schema {config.schema!r} pret.")
    finally:
        conn.close()


def maybe_initialize_database(project_root: Path, env: Dict[str, str]) -> DbConfig:
    """Initialise la BD si INIT_DB=true, retourne la config."""
    config = db_config_from_env(env)
    if config.init_db:
        initialize_database(project_root, config)
    return config


def configure_orm_schema(schema: str) -> str:
    """Applique le schéma PostgreSQL au modele ORM."""
    from .models.event_orm import EventORM

    schema = validate_schema_name(schema)
    EventORM.__table__.schema = schema
    return schema
