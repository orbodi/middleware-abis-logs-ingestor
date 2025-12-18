## abis_logs_ingestor

Ingestion et valorisation de logs ABIS (MOSIP) en Python.

Ce projet :
- **parse** les fichiers de logs `auditlog-*.log` (et `*.log.gz`) contenant des quasi‑JSON,
- **corrige** les JSON mal formés,
- **génère** des fichiers JSON propres,
- **persiste** les événements dans une base PostgreSQL (schéma `abis`, table `events`),
- **archive** automatiquement les inputs / JSON / logs d'exécution par date.

---

### Structure principale

- `projets/abis_logs_ingestor/`
  - `handler.py` : point d'entrée principal (`python -m abis_logs_ingestor.handler`)
  - `parser/` : logique de reconstruction et correction JSON (`build_full_json.py`, `fix_json_v2.py`)
  - `files/` : utilitaires fichiers (ex. `gzip_processing.py` pour les `.gz`)
  - `models/` : modèles Python et ORM (`events.py`, `event_orm.py`)
  - `repository/` : persistance en base (`events_repository.py`)
  - `service/` : orchestration de la persistance (`persist_json_events.py`)
  - `db_scripts/` : scripts SQL / init DB (`schema.sql`, `init_db.py`)
  - `storage/` : dossier de travail (inputs, archives, logs, json_data, errors)

---

### Prérequis

- Python 3.11+ (recommandé)
- PostgreSQL accessible

Installe les dépendances :

```bash
pip install -r requirements.txt
```

---

### Configuration

Copier l'exemple d'environnement et l'adapter :

```bash
cd projets/abis_logs_ingestor
cp env.sample .env
```

Variables importantes dans `.env` :

- **Répertoires de travail**
  - `STORAGE_DIR` : dossier racine de travail (ex. `storage`)
  - `INPUTS_SUBDIR`, `ARCHIVES_SUBDIR`, `JSON_DATA_SUBDIR`, `ERRORS_SUBDIR`, `LOGS_SUBDIR`
- **Base de données**
  - soit `DB_DSN` complet (ex. `postgresql+psycopg2://user:pwd@host:5432/db`)
  - soit `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Initialiser le schéma en base (depuis `projets/abis_logs_ingestor`) :

```bash
python db_scripts/init_db.py
```

---

### Utilisation

1. Déposer les fichiers dans `storage/inputs` :
   - logs bruts `auditlog-YYYY-MM-DD_HH.0.log`
   - ou archives `auditlog-YYYY-MM-DD_HH.0.log.gz` (elles seront décompressées dans `inputs/processing_data`)

2. Lancer l'ingestion (depuis `projets/abis_logs_ingestor`) :

```bash
python -m abis_logs_ingestor.handler
```

Le handler :
- décompresse les `.gz` vers `inputs/processing_data`,
- parse chaque `.log`,
- écrit les JSON dans `storage/json_data`,
- archive les logs et JSON par date dans `storage/archives/...`,
- persiste les JSON en base (table `abis.events`).

Les journaux d'exécution sont écrits dans `storage/logs/<YYYYMMDD>/..._stats.log`.

---

### Points importants

- Tous les blocs d'événements sont **toujours représentés** en JSON :
  - soit en JSON strict,
  - soit en objet "plat" fallback avec `_raw` et `_parse_error`.
- Les structures complexes (ex. `REQUEST_MESSAGE`, `BRS_REQUEST`, `RESPONSE_PUBLISHED_TO_MOSIP_QUEUE`) sont stockées en **JSONB** pour exploitation analytique.
- La colonne `created_at` est remplie automatiquement à l'insertion.

---

### Requêtes d'exemple

Des vues pratiques sont définies dans `schema.sql` :

- `abis.v_insert_requests` : requêtes d'insertion ABIS (`mosip.abis.insert`)
- `abis.v_identify_requests` : requêtes d'identification ABIS (`mosip.abis.identify`)

Exemples :

```sql
SELECT COUNT(*) FROM abis.v_insert_requests;
SELECT COUNT(*) FROM abis.v_identify_requests;
```


