"""
Sous-package `parser` pour `abis_logs_ingestor`.

Contient les parseurs avancés :
- `fix_json_v2` : correction de quasi-JSON en JSON strict.
- `build_full_json` : parsing de fichiers de logs complexes (multi-lignes).
"""

from .fix_json_v2 import fix_json  # noqa: F401

__all__ = ["fix_json"]


