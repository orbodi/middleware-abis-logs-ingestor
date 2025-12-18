import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _is_primitive(value: str) -> bool:
    """Return True if value already looks like a JSON primitive (number/bool/null)."""
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?|true|false|null", value))


def _fix_empty_values(text: str) -> str:
    """Replace empty values (`": ,` or `":}`) with an empty string."""
    return re.sub(r'":\s*(?=[,}])', '":""', text)


def _quote_specific_fields(text: str) -> str:
    """
    Quote explicitement certains champs connus qui sont presque toujours
    des chaînes métier dans les logs (OPERATION, REQUEST_ID, REQUEST_TIME, URLs, etc.).
    """

    string_fields = [
        "OPERATION",
        "REFERENCE_ID",
        "ID",
        "REQUEST_ID",
        "REQUEST_TIME",
        "RESPONSE_TIME",  # important pour les timestamps comme 2025-12-03T15:00:00.992407636
        "mosip.version",
        "REFERENCE_URL",
        "BRS_URL",
        "CBEFF_URL",
    ]

    fixed = text
    for field in string_fields:
        pattern = re.compile(
            rf'("{re.escape(field)}"\s*:\s*)([^,\}}\n]*?)(\s*[,}}])'
        )

        def repl(match: re.Match) -> str:
            prefix = match.group(1)
            value = match.group(2).strip()
            ending = match.group(3)

            if not value:
                return f'{prefix}""{ending}'

            # Déjà JSON (string, objet, array) ou primitive -> ne pas toucher
            if value[0] in '{["' or _is_primitive(value):
                return match.group(0)

            return f'{prefix}"{value}"{ending}'

        fixed = pattern.sub(repl, fixed)

    return fixed


def _fix_java_like_values(text: str) -> str:
    """
    Détecte les valeurs de type Java (ex: idemia.brs.mosip.data.MosipBaseData@abcd1234
    ou [B@abcdef) après certains champs et les convertit en chaînes JSON.
    """

    # Cas général pour REQUEST_MESSAGE (peut être un objet JSON ou un dump Java)
    def repl_request_message(match: re.Match) -> str:
        prefix = match.group(1)
        value = match.group(2).strip()
        ending = match.group(3)
        # Si déjà un objet/array/chaîne JSON, on ne touche pas
        if value.startswith("{") or value.startswith("[") or value.startswith('"'):
            return match.group(0)
        # Sinon, tout mettre en chaîne brute
        return f'{prefix}"{value}"{ending}'

    text = re.sub(
        r'("REQUEST_MESSAGE"\s*:\s*)([^,\}}\n]*?)(\s*[,}}])', repl_request_message, text
    )

    # Cas spécifique CBEFF_DATA pour les valeurs [B@xxxx
    def repl_cbeff(match: re.Match) -> str:
        prefix = match.group(1)
        value = match.group(2).strip()
        ending = match.group(3)
        # Si c'est un dump de tableau Java [B@...., le convertir en chaîne
        if re.fullmatch(r"\[B@[0-9A-Fa-f]+\]?", value):
            return f'{prefix}"{value}"{ending}'
        # Si c'est déjà un tableau/objet/chaîne JSON ou primitive, laisser tel quel
        if value and (value[0] in "{[" or value[0] == '"' or _is_primitive(value)):
            return match.group(0)
        # Sinon, traiter comme chaîne générique
        return f'{prefix}"{value}"{ending}'

    text = re.sub(
        r'("CBEFF_DATA"\s*:\s*)([^,\}}\n]*?)(\s*[,}}])', repl_cbeff, text
    )

    return text


def _fix_map_like(segment: str) -> str:
    """
    Convert Java/Map-like content (key=value, bare lists) into JSON.
    The segment is assumed to start with { and contain no nested {} (handled by caller).
    """
    # key= -> "key":
    segment = re.sub(r"([{\[,]\s*)([A-Za-z0-9_.-]+)=", r'\1"\2":', segment)

    # Quote bare tokens inside arrays: [FINGER, IRIS] -> ["FINGER","IRIS"]
    def quote_array(match: re.Match) -> str:
        val = match.group(2)
        if _is_primitive(val):
            return match.group(0)
        return f'{match.group(1)}"{val}"{match.group(3)}'

    segment = re.sub(r"([\[,]\s*)([^,\]\s{}]+)(\s*[,]])", quote_array, segment)

    # Quote bare values after a colon (except primitives)
    def quote_value(match: re.Match) -> str:
        val = match.group(2)
        if _is_primitive(val):
            return match.group(0)
        return f'{match.group(1)}"{val}"{match.group(3)}'

    segment = re.sub(r"(:\s*)([^,\}\s\[][^,\}\s]*)(\s*[,}])", quote_value, segment)
    return segment


def _fix_map_like_blocks(text: str) -> str:
    """Apply _fix_map_like to all single-level {...} blocks (non-nested)."""
    return re.sub(r"\{[^{}]*\}", lambda m: _fix_map_like(m.group(0)), text)


def _fix_global_arrays(text: str) -> str:
    """
    Quote bare identifiers inside simple arrays:
    [FINGER, IRIS] -> ["FINGER","IRIS"]
    [123, true] remains unchanged for primitives.
    """

    def repl(match: re.Match) -> str:
        inside = match.group(1)
        # Ne pas toucher aux tableaux qui contiennent déjà des objets JSON
        if "{" in inside or "}" in inside:
            return match.group(0)

        parts = [p.strip() for p in inside.split(",")]
        out_parts = []
        for p in parts:
            if not p:
                continue
            if _is_primitive(p) or (p.startswith('"') and p.endswith('"')):
                out_parts.append(p)
            else:
                out_parts.append(f'"{p}"')
        return "[" + ",".join(out_parts) + "]"

    # Conservatif : on ne touche pas aux tableaux imbriqués
    return re.sub(r"\[([^\[\]]+)\]", repl, text)


def _dedupe_quotes(text: str) -> str:
    """
    Fix cases like \"\"value\"\" -> \"value\" that peuvent apparaître
    après plusieurs passes de quoting.
    """
    # ""abc"" -> "abc"
    text = re.sub(r'""([^"]+)""', r'"\1"', text)
    return text


def fix_json(text: str) -> str:
    """
    Corrige un quasi-JSON en JSON valide :
    - normalise les valeurs vides
    - convertit les sous-objets Java/Map-like (key=value, listes non quotées)
    - ajoute des guillemets aux valeurs racines non-JSON
    - quote les valeurs texte dans tous les tableaux simples
    - nettoie les doubles guillemets accidentels
    """
    fixed = text.strip()

    # 1) Normaliser les valeurs vides
    fixed = _fix_empty_values(fixed)

    # 2) Forcer certains champs racine à être des chaînes (OPERATION, REQUEST_ID, etc.)
    fixed = _quote_specific_fields(fixed)

    # 3) Convertir les sous-objets map-like (BRS_REQUEST, etc.)
    fixed = _fix_map_like_blocks(fixed)

    # 4) Corriger les valeurs de type Java (REQUEST_MESSAGE, CBEFF_DATA, etc.)
    fixed = _fix_java_like_values(fixed)

    # 5) Ajouter des guillemets autour des valeurs non-JSON restantes au niveau racine
    def quote_root_value(match: re.Match) -> str:
        key = match.group(1)
        value = match.group(2).strip()
        ending = match.group(3)

        # Cas spéciaux : dumps d'objets Java (Class@hash, [B@hash)
        if re.match(r"^[A-Za-z0-9_.\$]+@[0-9a-fA-F]+$", value) or re.match(
            r"^\[B@[0-9a-fA-F]+\]?$", value
        ):
            return f'"{key}":"{value}"{ending}'

        # Ne pas toucher aux nombres, booléens, null, objets et arrays JSON normaux
        if re.match(r"^(true|false|null|[+-]?\d+(\.\d+)?|[\{\[].*)$", value):
            return match.group(0)

        # Par défaut, traiter comme une chaîne
        return f'"{key}":"{value}"{ending}'

    fixed = re.sub(
        r'"([^"]+)"\s*:\s*([^",\{\[\s][^,\}\]]*?)(\s*[,\}])',
        quote_root_value,
        fixed,
    )

    # 6) Globalement, s'assurer que les tableaux ne contiennent pas de valeurs non-quotées simples
    fixed = _fix_global_arrays(fixed)

    # 7) Nettoyer les doubles guillemets accidentels
    fixed = _dedupe_quotes(fixed)

    return fixed


if __name__ == "__main__":
    # Petit mode CLI pour tester rapidement ce parseur sur un fichier
    if len(sys.argv) > 1:
        path = sys.argv[1]
        text = Path(path).read_text(encoding="utf-8")
        fixed = fix_json(text)
        try:
            obj = json.loads(fixed)
            print(json.dumps(obj, indent=2, ensure_ascii=False))
        except Exception as e:  # pragma: no cover - debug only
            print("Erreur de parsing après correction:", e, file=sys.stderr)
            print(fixed)

