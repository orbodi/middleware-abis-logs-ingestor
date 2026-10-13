#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relivre à l'ingestor ABIS les morceaux de logs absents de la base.

Contexte
--------
Le relais (logspipe-relay) déposait les logs des serveurs BRS sous le même nom dans le
dossier de l'ingestor : pour chaque morceau (auditlog-AAAA-MM-JJ_HH.N), le dernier serveur
livré écrasait les autres. Depuis le 28/09/2026, plus rien n'est livré du tout.

Ce script compare les .gz conservés par le relais (processed/<serveur>/) avec la base
(colonnes host + source_file) et ne relivre que les couples (serveur, morceau) absents,
avec un préfixe serveur (brs1-auditlog-...) pour qu'ils ne s'écrasent plus.
Un morceau déjà présent en base n'est jamais relivré : pas de doublon.

Par défaut le script SIMULE (bilan par jour, rien n'est modifié).
Ajouter --appliquer pour livrer par lots et lancer l'ingestor après chaque lot.

Exemples (sur bislalog)
-----------------------
  python3 recuperer_morceaux.py --du 2026-09-13                                   # bilan seulement
  python3 recuperer_morceaux.py --du 2026-09-28 --au 2026-09-28 --appliquer --purger-archives
  python3 recuperer_morceaux.py --du 2026-09-13 --appliquer --purger-archives

Compatible Python 3.8. Dépendance : psycopg2 (déjà utilisée par l'ingestor).
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import os
import re
import shutil
import struct
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

try:
    import psycopg2
except ImportError:  # pragma: no cover
    sys.exit("psycopg2 est requis (il est déjà installé pour l'ingestor).")

# auditlog-AAAA-MM-JJ_HH.N, avec ou sans préfixe serveur, quelle que soit l'extension
CLE_RE = re.compile(r"(auditlog-(\d{4}-\d{2}-\d{2})_(\d{2})\.(\d+))")
GZ_RE = re.compile(r"^auditlog-(\d{4}-\d{2}-\d{2})_(\d{2})\.(\d+)\.log\.gz$")
HOST_RE = re.compile(rb'"HOST"\s*:\s*"([^"]+)"')
SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
GO = 1024 ** 3


class Morceau(NamedTuple):
    serveur: str      # nom normalisé (brs1), comparé à split_part(host, '.', 1)
    dossier: str      # nom du dossier dans processed/ (utilisé pour le préfixe)
    cle: str          # auditlog-AAAA-MM-JJ_HH.N
    jour: str         # AAAA-MM-JJ
    heure: int
    index: int
    chemin: Path
    taille_gz: int
    taille_log: int   # estimation de la taille décompressée


# --------------------------------------------------------------------------- outils


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def go(n: float) -> str:
    return f"{n / GO:.1f} Go"


def charger_env(path: Path) -> Dict[str, str]:
    """Même lecture du .env que l'ingestor (storage.load_env)."""
    env: Dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    merged = dict(env)
    merged.update(os.environ)
    return merged


def taille_decompressee(path: Path, taille_gz: int) -> int:
    """Champ ISIZE du gzip (modulo 4 Go) : estimation, suffisante pour dimensionner les lots."""
    try:
        with open(path, "rb") as fh:
            fh.seek(-4, os.SEEK_END)
            isize = struct.unpack("<I", fh.read(4))[0]
    except OSError:
        return taille_gz * 8
    return max(isize, taille_gz)


def memoire_disponible() -> Optional[int]:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def host_du_fichier(path: Path) -> Optional[str]:
    """Lit le premier HOST d'un .gz (début du fichier seulement)."""
    try:
        with gzip.open(path, "rb") as fh:
            m = HOST_RE.search(fh.read(256 * 1024))
            return m.group(1).decode("utf-8", "replace") if m else None
    except OSError:
        return None


def normaliser_serveur(host: str) -> str:
    return host.split(".", 1)[0].strip().lower()


# --------------------------------------------------------------------------- base


def connexion(env: Dict[str, str]):
    dsn = env.get("DB_DSN", "").strip()
    if dsn:
        return psycopg2.connect(dsn.replace("postgresql+psycopg2://", "postgresql://", 1))
    return psycopg2.connect(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        dbname=env.get("DB_NAME", "abis"),
        user=env.get("DB_USER", "postgres"),
        password=env.get("DB_PASSWORD", ""),
    )


def morceaux_en_base(conn, schema: str, du: dt.date, au: dt.date) -> Set[Tuple[str, str]]:
    """Couples (serveur, morceau) présents en base, sur la période élargie de 2 jours."""
    sql = (
        f"SELECT DISTINCT split_part(host, '.', 1), source_file FROM {schema}.events "
        "WHERE event_timestamp >= %s AND event_timestamp < %s"
    )
    debut = dt.datetime.combine(du - dt.timedelta(days=2), dt.time.min)
    fin = dt.datetime.combine(au + dt.timedelta(days=3), dt.time.min)
    present: Set[Tuple[str, str]] = set()
    with conn.cursor() as cur:
        cur.execute(sql, (debut, fin))
        for host, source in cur.fetchall():
            m = CLE_RE.search(os.path.basename(source or ""))
            if host and m:
                present.add((host.strip().lower(), m.group(1)))
    conn.rollback()  # fin de transaction en lecture
    return present


# --------------------------------------------------------------------------- contrôles


def controles_prealables(ingestor_dir: Path, conn, schema: str) -> Tuple[List[str], List[str]]:
    bloquants: List[str] = []
    avertissements: List[str] = []

    dbc = ingestor_dir / "db_config.py"
    if dbc.is_file() and "from models.event_orm" in dbc.read_text(encoding="utf-8"):
        bloquants.append(
            "db_config.py n'a pas le correctif d'import (from .models.event_orm) : "
            "l'ingestor plantera juste avant l'insertion en base."
        )

    repo = ingestor_dir / "repository" / "events_repository.py"
    repo_txt = repo.read_text(encoding="utf-8") if repo.is_file() else ""
    if "_FRACTION_RE" not in repo_txt and sys.version_info < (3, 11):
        avertissements.append(
            "events_repository.py n'a pas le correctif des horodatages : en Python "
            f"{sys.version_info.major}.{sys.version_info.minor}, response_time restera vide."
        )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'events' AND column_name = 'payload'",
            (schema,),
        )
        row = cur.fetchone()
    conn.rollback()
    if row and row[0] == "NO" and "payload=None" in repo_txt:
        bloquants.append(
            f"La colonne {schema}.events.payload est NOT NULL alors que l'ingestor écrit "
            f"payload=None : toutes les insertions échoueraient. Exécuter d'abord :\n"
            f"      ALTER TABLE {schema}.events ALTER COLUMN payload DROP NOT NULL;"
        )
    return bloquants, avertissements


def verifier_hosts(par_serveur: Dict[str, List[Morceau]]) -> List[str]:
    """Vérifie, sur un fichier par dossier, que le HOST correspond bien au nom du dossier."""
    alertes = []
    for dossier, morceaux in sorted(par_serveur.items()):
        if not morceaux:
            continue
        host = host_du_fichier(morceaux[0].chemin)
        if host is None:
            alertes.append(f"{dossier} : HOST illisible dans {morceaux[0].chemin.name}")
        elif normaliser_serveur(host) != morceaux[0].serveur:
            alertes.append(
                f"{dossier} : le fichier {morceaux[0].chemin.name} contient HOST={host}, "
                f"différent du nom du dossier"
            )
    return alertes


# --------------------------------------------------------------------------- inventaire


def lister_processed(
    processed: Path, serveurs: Optional[Set[str]], du: str, au: str
) -> Dict[str, List[Morceau]]:
    res: Dict[str, List[Morceau]] = defaultdict(list)
    for sdir in sorted(p for p in processed.iterdir() if p.is_dir()):
        if serveurs and sdir.name.lower() not in serveurs:
            continue
        for f in sdir.iterdir():
            m = GZ_RE.match(f.name)
            if not m or not (du <= m.group(1) <= au):
                continue
            taille_gz = f.stat().st_size
            res[sdir.name].append(
                Morceau(
                    serveur=normaliser_serveur(sdir.name),
                    dossier=sdir.name,
                    cle=f"auditlog-{m.group(1)}_{m.group(2)}.{m.group(3)}",
                    jour=m.group(1),
                    heure=int(m.group(2)),
                    index=int(m.group(3)),
                    chemin=f,
                    taille_gz=taille_gz,
                    taille_log=taille_decompressee(f, taille_gz),
                )
            )
    return res


def decouper_en_lots(morceaux: List[Morceau], max_fichiers: int, max_octets: int) -> List[List[Morceau]]:
    """Lots chronologiques, jamais à cheval sur deux jours."""
    lots: List[List[Morceau]] = []
    courant: List[Morceau] = []
    taille = 0
    for m in morceaux:
        nouveau_jour = courant and courant[-1].jour != m.jour
        trop = courant and (len(courant) >= max_fichiers or taille + m.taille_log > max_octets)
        if nouveau_jour or trop:
            lots.append(courant)
            courant, taille = [], 0
        courant.append(m)
        taille += m.taille_log
    if courant:
        lots.append(courant)
    return lots


def afficher_bilan(tous: List[Morceau], manquants: List[Morceau], serveurs: List[str]) -> None:
    total = defaultdict(lambda: defaultdict(int))
    manque = defaultdict(lambda: defaultdict(int))
    volume = defaultdict(int)
    for m in tous:
        total[m.jour][m.serveur] += 1
    for m in manquants:
        manque[m.jour][m.serveur] += 1
        volume[m.jour] += m.taille_log
    entete = "Jour        " + "".join(f"{s + ' manq/total':>18}" for s in serveurs) + "   à livrer   décompressé"
    print(entete)
    print("-" * len(entete))
    for jour in sorted(total):
        cols = "".join(f"{manque[jour][s]:>9}/{total[jour][s]:<8}" for s in serveurs)
        n = sum(manque[jour].values())
        print(f"{jour}  {cols}  {n:>8}   {go(volume[jour]):>10}")
    print("-" * len(entete))
    print(
        f"TOTAL : {len(manquants)} morceaux à livrer sur {len(tous)} "
        f"({go(sum(m.taille_log for m in manquants))} décompressés)"
    )


# --------------------------------------------------------------------------- application


def lancer_ingestor(commande: str, cwd: Path, journal: Path) -> int:
    journal.parent.mkdir(parents=True, exist_ok=True)
    with open(journal, "a", encoding="utf-8") as out:
        out.write(f"\n===== {dt.datetime.now():%Y-%m-%d %H:%M:%S} : {commande}\n")
        out.flush()
        proc = subprocess.run(commande, shell=True, cwd=str(cwd), stdout=out, stderr=subprocess.STDOUT)
    return proc.returncode


def noms_livraison(m: Morceau, inputs: Path, json_dir: Path) -> Tuple[Path, Path, Path]:
    """(.log.gz déposé, .log décompressé par l'ingestor, .json produit par l'ingestor)."""
    base = f"{m.dossier}-{m.cle}"
    return inputs / f"{base}.log.gz", inputs / f"{base}.log", json_dir / f"{base}.json"


def fichiers_en_attente(inputs: Path, json_dir: Path, dossiers: Set[str]) -> Tuple[List[str], List[str]]:
    """Fichiers déjà en attente côté ingestor : (les nôtres, les autres)."""
    prefixes = tuple(f"{d}-auditlog-" for d in dossiers)
    nos, autres = [], []
    candidats = []
    if inputs.is_dir():
        candidats += [p for p in inputs.iterdir() if p.is_file() and (p.name.endswith(".log") or p.name.endswith(".log.gz"))]
        legacy = inputs / "processing_data"
        if legacy.is_dir():
            candidats += [p for p in legacy.iterdir() if p.is_file() and p.name.endswith(".log")]
    if json_dir.is_dir():
        candidats += [p for p in json_dir.iterdir() if p.is_file() and p.name.endswith(".json")]
    for p in candidats:
        (nos if p.name.startswith(prefixes) and p.parent.name != "processing_data" else autres).append(str(p))
    return nos, autres


def appliquer(args, lots, conn, schema, ingestor_dir, root_dir, journal) -> int:
    inputs = root_dir / "inputs"
    json_dir = root_dir / "json_data"
    errors_dir = root_dir / "errors"
    inputs.mkdir(parents=True, exist_ok=True)

    commande = args.commande
    if not commande:
        verrou = "flock /tmp/abis-ingestor.lock " if shutil.which("flock") else ""
        commande = f"{verrou}{sys.executable} -m {ingestor_dir.name}.handler --no-cleanup"

    livres_ok = 0
    for i, lot in enumerate(lots, 1):
        besoin = sum(m.taille_log for m in lot)
        libre = shutil.disk_usage(str(inputs)).free
        if libre - 2 * besoin < args.marge_disque_go * GO:
            log(
                f"ARRÊT : espace insuffisant pour le lot {i} ({go(libre)} libres, besoin estimé "
                f"{go(2 * besoin)} + marge {args.marge_disque_go} Go)."
            )
            return 2
        plus_gros = max(m.taille_log for m in lot)
        dispo = memoire_disponible()
        if dispo and plus_gros * 6 > dispo:
            log(
                f"Attention mémoire : le plus gros morceau du lot fait {go(plus_gros)} décompressé, "
                f"{go(dispo)} de RAM disponible. L'ingestor charge tout le fichier en mémoire."
            )

        nouveaux = 0
        for m in lot:
            gz, log_file, json_file = noms_livraison(m, inputs, json_dir)
            if (errors_dir / log_file.name).exists():
                log(f"  déjà tenté, aucun événement lisible (dans errors/) : {log_file.name}")
                continue
            if json_file.exists():
                log(f"  JSON déjà en attente, il sera inséré par l'ingestor : {json_file.name}")
                continue
            if gz.exists() or log_file.exists():
                log(f"  déjà en attente dans inputs/ : {gz.name}")
                continue
            try:
                os.link(str(m.chemin), str(gz))
            except OSError:
                shutil.copy2(str(m.chemin), str(gz))
            nouveaux += 1

        log(f"Lot {i}/{len(lots)} — {lot[0].jour} — {nouveaux} morceau(x) déposé(s), {go(besoin)} décompressés : ingestor…")
        t0 = time.time()
        rc = lancer_ingestor(commande, ingestor_dir.parent, journal)
        duree = time.time() - t0

        present = morceaux_en_base(
            conn, schema,
            dt.date.fromisoformat(lot[0].jour), dt.date.fromisoformat(lot[-1].jour),
        )
        absents = [m for m in lot if (m.serveur, m.cle) not in present]
        livres_ok += len(lot) - len(absents)

        # Un morceau inséré ne doit plus rien laisser dans inputs/ : si l'ingestor y a laissé le .gz
        # (sauvegarde déjà existante dans backups/), il le réingérerait au passage suivant.
        nettoyes = 0
        liberes = 0
        for m in lot:
            if (m.serveur, m.cle) in present:
                for reste in noms_livraison(m, inputs, json_dir)[:2]:
                    if reste.exists():
                        reste.unlink()
                        nettoyes += 1
                if args.purger_archives:
                    # Le .log décompressé et le JSON archivés par l'ingestor ne servent plus :
                    # le morceau est en base et son .gz d'origine reste dans processed/.
                    dossier_date = m.jour.replace("-", "")
                    base = f"{m.dossier}-{m.cle}"
                    for archive in (
                        root_dir / "archives" / "inputs" / dossier_date / f"{base}.log",
                        root_dir / "archives" / "json_data" / dossier_date / f"{base}.json",
                    ):
                        if archive.exists():
                            liberes += archive.stat().st_size
                            archive.unlink()

        restes, json_restants, en_erreur = [], [], []
        for m in absents:
            gz, log_file, json_file = noms_livraison(m, inputs, json_dir)
            if gz.exists() or log_file.exists():
                restes.append(log_file.name)
            if json_file.exists():
                json_restants.append(json_file.name)
            if (errors_dir / log_file.name).exists():
                en_erreur.append(log_file.name)

        log(
            f"  code retour {rc}, {duree:.0f} s — en base : {len(lot) - len(absents)}/{len(lot)}"
            + (f", vides (dans errors/) : {len(en_erreur)}" if en_erreur else "")
            + (f", restes retirés de inputs/ : {nettoyes}" if nettoyes else "")
            + (f", archives purgées : {go(liberes)}" if args.purger_archives else "")
        )

        if rc != 0 or restes or json_restants or len(absents) != len(en_erreur):
            log("ARRÊT : le lot ne s'est pas terminé proprement.")
            if rc in (-9, 137):
                log("  L'ingestor a été tué (code 137/-9) : très probablement un manque de mémoire.")
            if restes:
                log(f"  Encore dans inputs/ : {', '.join(restes[:5])}{' …' if len(restes) > 5 else ''}")
            if json_restants:
                log(f"  JSON non insérés dans json_data/ : {', '.join(json_restants[:5])}")
            if absents:
                log(f"  Absents de la base : {', '.join(m.dossier + '-' + m.cle for m in absents[:5])}")
            log(f"  Détails : {journal}")
            return 1

    log(f"Terminé : {livres_ok} morceau(x) livrés et présents en base.")
    return 0


# --------------------------------------------------------------------------- main


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Relivre à l'ingestor ABIS les morceaux absents de la base (simulation par défaut).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--du", required=True, help="premier jour (AAAA-MM-JJ, date du nom de fichier)")
    p.add_argument("--au", default=dt.date.today().isoformat(), help="dernier jour inclus (défaut : aujourd'hui)")
    p.add_argument("--processed", type=Path, default=Path("/data/logspipe-relay/data/data/processed"))
    p.add_argument("--ingestor", type=Path, default=Path("/data/middleware-abis-logs-ingestor"),
                   help="dossier de l'ingestor (son .env donne la base et ROOT_DIR)")
    p.add_argument("--serveurs", help="liste séparée par des virgules (défaut : tous les dossiers)")
    p.add_argument("--lot-max-fichiers", type=int, default=10)
    p.add_argument("--lot-max-go", type=float, default=20.0, help="taille décompressée max d'un lot")
    p.add_argument("--marge-disque-go", type=float, default=30.0)
    p.add_argument("--commande", help="commande de lancement de l'ingestor (défaut : python3 -m <dossier>.handler --no-cleanup)")
    p.add_argument("--journal", type=Path, help="fichier de sortie de l'ingestor (défaut : <ingestor>/logs/recuperation.log)")
    p.add_argument("--purger-archives", action="store_true",
                   help="supprimer le .log et le JSON archivés par l'ingestor dès que le morceau est en base "
                        "(recommandé : sinon chaque jour rattrapé occupe son volume décompressé pendant 5 jours)")
    p.add_argument("--appliquer", action="store_true", help="livrer réellement et lancer l'ingestor")
    args = p.parse_args(argv)

    du_d = dt.date.fromisoformat(args.du)
    au_d = dt.date.fromisoformat(args.au)
    ingestor_dir = args.ingestor.resolve()
    env = charger_env(ingestor_dir / ".env")
    schema = env.get("DB_SCHEMA", "abis").strip()
    if not SCHEMA_RE.match(schema):
        print(f"DB_SCHEMA invalide : {schema!r}", file=sys.stderr)
        return 2
    root_env = env.get("ROOT_DIR", "").strip() or env.get("STORAGE_DIR", "storage").strip()
    root_dir = Path(root_env) if Path(root_env).is_absolute() else ingestor_dir / root_env
    journal = args.journal or ingestor_dir / "logs" / "recuperation.log"
    serveurs = {s.strip().lower() for s in args.serveurs.split(",")} if args.serveurs else None

    log(f"Période {du_d} → {au_d} | processed : {args.processed} | ingestor ROOT_DIR : {root_dir} | schéma : {schema}")
    conn = connexion(env)

    bloquants, avertissements = controles_prealables(ingestor_dir, conn, schema)
    for a in avertissements:
        log(f"Attention : {a}")

    par_serveur = lister_processed(args.processed, serveurs, du_d.isoformat(), au_d.isoformat())
    for alerte in verifier_hosts(par_serveur):
        log(f"Attention : {alerte}")
    tous = sorted(
        (m for lst in par_serveur.values() for m in lst),
        key=lambda m: (m.jour, m.heure, m.index, m.serveur),
    )
    if not tous:
        log("Aucun fichier dans processed/ pour cette période.")
        return 0

    present = morceaux_en_base(conn, schema, du_d, au_d)
    manquants = [m for m in tous if (m.serveur, m.cle) not in present]
    noms_serveurs = sorted({m.serveur for m in tous})
    log(f"{len(present)} couple(s) (serveur, morceau) déjà en base sur la période élargie.")
    print()
    afficher_bilan(tous, manquants, noms_serveurs)
    print()

    if not manquants:
        log("Rien à livrer.")
        return 0
    lots = decouper_en_lots(manquants, args.lot_max_fichiers, int(args.lot_max_go * GO))
    log(f"{len(lots)} lot(s) prévus (max {args.lot_max_fichiers} fichiers / {args.lot_max_go} Go décompressés).")

    nos, autres = fichiers_en_attente(root_dir / "inputs", root_dir / "json_data", set(par_serveur))
    if nos:
        log(f"{len(nos)} fichier(s) d'une exécution précédente du script en attente : ils seront traités.")
    if autres:
        bloquants.append(
            f"{len(autres)} fichier(s) sans préfixe serveur attendent déjà côté ingestor "
            "(on ne peut pas savoir de quel serveur ils viennent, ils risqueraient de créer des doublons). "
            "Les traiter ou les déplacer avant la récupération :\n      "
            + "\n      ".join(autres[:5]) + ("\n      …" if len(autres) > 5 else "")
        )

    if bloquants:
        for b in bloquants:
            log(f"BLOQUANT : {b}")
        return 2
    if not args.appliquer:
        log("Simulation uniquement. Relancer avec --appliquer pour livrer.")
        return 0
    return appliquer(args, lots, conn, schema, ingestor_dir, root_dir, journal)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("Interrompu. Les morceaux déjà livrés restent dans inputs/ et seront traités au prochain passage.")
        sys.exit(130)
