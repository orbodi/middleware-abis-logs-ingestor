"""
Parseur avancé pour fichiers de logs ABIS multi-lignes.

Ce module fournit :
- `extract_json_objects`: reconstitution des blocs JSON multi-lignes
- `process_content`: applique `fix_json_v2` et retourne une liste d'objets JSON
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict, List, Tuple

from .fix_json_v2 import fix_json


def extract_json_objects(content: str) -> List[Tuple[int, str]]:
    """
    Extrait les objets JSON d'un fichier multi-lignes
    en reconstituant les blocs `{ ... }` qui s'étalent sur plusieurs lignes.

    Returns:
        Liste de (line_start, json_string)
    """
    objects: List[Tuple[int, str]] = []
    current_obj: List[str] = []
    current_start_line = 0
    brace_depth = 0
    in_string = False
    escape_next = False

    lines = content.split("\n")

    for line_num, line in enumerate(lines, 1):
        # Parcourir caractère par caractère pour compter les accolades
        for char in line:
            if escape_next:
                escape_next = False
                continue

            if char == "\\":
                escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string

            if not in_string:
                if char == "{":
                    if brace_depth == 0:
                        current_start_line = line_num
                        current_obj = []
                    brace_depth += 1
                elif char == "}":
                    brace_depth -= 1

        # Ajouter la ligne à l'objet en cours
        if brace_depth > 0 or (brace_depth == 0 and line.strip().endswith("}")):
            current_obj.append(line)

        # Si on a fermé toutes les accolades, on a un objet complet
        if brace_depth == 0 and current_obj:
            json_str = "\n".join(current_obj)
            if json_str.strip():
                objects.append((current_start_line, json_str))
            current_obj = []

    return objects


def _fallback_flat_object(start_line: int, json_str: str, error: Exception) -> Dict[str, Any]:
    """
    Dernier recours : parser le bloc à plat avec une regex, pour garantir
    qu'on renvoie au moins un dictionnaire exploitable.
    """
    obj: Dict[str, Any] = {
        "_start_line": start_line,
        "_raw": json_str,
        "_parse_error": str(error),
    }

    # On travaille sur une seule ligne pour simplifier la regex
    line = " ".join(json_str.splitlines())
    line_for_kv = line  # ligne utilisée pour l'extraction des paires clé/valeur simples

    # Tentative de récupération correcte de l'objet imbriqué
    # RESPONSE_PUBLISHED_TO_MOSIP_QUEUE si présent.
    key_rsp = '"RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"'
    idx = line.find(key_rsp)
    if idx != -1:
        colon = line.find(":", idx)
        if colon != -1:
            brace_start = line.find("{", colon)
            if brace_start != -1:
                depth = 0
                in_string = False
                escape_next = False
                brace_end = -1
                for i, ch in enumerate(line[brace_start:], start=brace_start):
                    if escape_next:
                        escape_next = False
                        continue
                    if ch == "\\":
                        escape_next = True
                        continue
                    if ch == '"' and not escape_next:
                        in_string = not in_string
                        continue
                    if not in_string:
                        if ch == "{":
                            depth += 1
                        elif ch == "}":
                            depth -= 1
                            if depth == 0:
                                brace_end = i
                                break
                if brace_end != -1:
                    sub = line[brace_start : brace_end + 1]
                    try:
                        obj["RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"] = json.loads(sub)
                    except Exception:
                        # En dernier recours, conserver la chaîne brute
                        obj["RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"] = sub
                    # Supprimer complètement cette clé de la ligne pour éviter
                    # que la regex générale ne la capture
                    # On cherche la fin de la clé (après l'objet) pour supprimer toute la partie
                    # Format: "RESPONSE_PUBLISHED_TO_MOSIP_QUEUE":{...},
                    # On doit trouver la virgule ou l'accolade fermante après l'objet
                    after_obj = brace_end + 1
                    # Chercher la virgule ou l'accolade fermante qui suit
                    while after_obj < len(line) and line[after_obj] in " \t":
                        after_obj += 1
                    if after_obj < len(line) and line[after_obj] == ",":
                        # Supprimer de la clé jusqu'à la virgule incluse
                        line_for_kv = line[:idx] + line[after_obj + 1 :]
                    elif after_obj < len(line) and line[after_obj] == "}":
                        # Supprimer de la clé jusqu'à avant l'accolade (c'est la fin de l'objet parent)
                        line_for_kv = line[:idx] + line[after_obj:]
                    else:
                        # Pas de séparateur trouvé, supprimer jusqu'à la fin
                        line_for_kv = line[:idx]
                else:
                    # Objet non fermé, on prend tout ce qui reste après le :
                    obj["RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"] = line[colon + 1 :].strip()
                    # Supprimer cette clé de la ligne
                    line_for_kv = line[:idx]
            else:
                # Pas de { après le :, prendre le reste comme string
                obj["RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"] = line[colon + 1 :].strip()
                # Supprimer cette clé de la ligne
                # Chercher la virgule ou } après la valeur
                after_colon = colon + 1
                while after_colon < len(line) and line[after_colon] in " \t":
                    after_colon += 1
                value_end = after_colon
                while value_end < len(line) and line[value_end] not in ",}":
                    value_end += 1
                if value_end < len(line) and line[value_end] == ",":
                    line_for_kv = line[:idx] + line[value_end + 1 :]
                elif value_end < len(line):
                    line_for_kv = line[:idx] + line[value_end:]
                else:
                    line_for_kv = line[:idx]
        else:
            # Pas de : après la clé
            obj["RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"] = line[idx + len(key_rsp) :].strip()
            # Supprimer cette clé de la ligne
            line_for_kv = line[:idx]

    # Appliquer la regex générale sur la ligne nettoyée (sans RESPONSE_PUBLISHED_TO_MOSIP_QUEUE)
    for m in re.finditer(r'"([^"\\]+)"\s*:\s*([^,}]+)', line_for_kv):
        key = m.group(1)
        # Ignorer si on a déjà traité cette clé
        if key == "RESPONSE_PUBLISHED_TO_MOSIP_QUEUE":
            continue
        value = m.group(2).strip()

        # Nettoyer les guillemets de début/fin
        if value.startswith('"') and value.endswith('"'):
            obj[key] = value[1:-1]
            continue

        lower = value.lower()
        if lower == "true":
            obj[key] = True
        elif lower == "false":
            obj[key] = False
        elif lower == "null":
            obj[key] = None
        else:
            try:
                if "." in value:
                    obj[key] = float(value)
                else:
                    obj[key] = int(value)
            except ValueError:
                obj[key] = value

    return obj


def process_content(
    content: str, debug: bool = False
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Traite un contenu brut de log et retourne les objets JSON parsés.

    - Détecte les blocs `{ ... }` multi-lignes.
    - Tente un `json.loads` direct, puis avec `fix_json`.
    - En cas d'échec, construit un objet "plat" de secours.
    """
    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    json_objects = extract_json_objects(content)

    if debug:
        print(
            f"🔍 {len(json_objects)} objet(s) JSON détecté(s) dans le fichier",
            file=sys.stderr,
        )

    for start_line, json_str in json_objects:
        if not json_str.strip():
            continue

        # 1) Essayer de parser directement
        try:
            parsed = json.loads(json_str)
            results.append(parsed)
            continue
        except json.JSONDecodeError as e_raw:
            last_error = e_raw

        # 2) Essayer avec fix_json_v2
        try:
            fixed = fix_json(json_str)
            parsed = json.loads(fixed)
            results.append(parsed)
            continue
        except json.JSONDecodeError as e_fixed:
            last_error = e_fixed

        # 3) Dernier recours : objet plat de secours
        fallback_obj = _fallback_flat_object(start_line, json_str, last_error)
        results.append(fallback_obj)
        errors.append(f"Ligne {start_line}: JSONDecodeError - {last_error}")

    return results, errors


__all__ = ["extract_json_objects", "process_content"]


