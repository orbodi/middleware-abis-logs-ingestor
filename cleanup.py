"""Nettoyage par age et par espace disque sous ROOT_DIR."""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from disk import get_available_gb
from storage import StorageLayout

_AGE_TARGETS = (
    ("archives_inputs", "archives/inputs", "tree"),
    ("archives_json", "archives/json_data", "tree"),
    ("backups", "backups", "tree"),
    ("errors", "errors", "flat"),
    ("logs", "logs", "tree"),
    ("json_data", "json_data", "flat"),
)

_DISK_TIERS: Tuple[Tuple[str, str, str], ...] = (
    ("logs", "logs", "tree"),
    ("backups", "backups", "tree"),
    ("archives_inputs", "archives/inputs", "tree"),
    ("archives_json", "archives/json_data", "tree"),
    ("errors", "errors", "flat"),
    ("processing", "inputs/processing_data", "flat"),
    ("inputs", "inputs", "flat"),
    ("json_data", "json_data", "flat"),
)


@dataclass
class CleanupConfig:
    cleanup_archives_after_days: int = 30
    cleanup_backups_after_days: int = 30
    cleanup_errors_after_days: int = 90
    cleanup_logs_after_days: int = 30
    cleanup_json_data_after_days: int = 0
    disk_space_threshold_gb: int = 10
    disk_space_target_gb: int = 0
    disk_cleanup_include_json_data: bool = False


@dataclass
class _FileCandidate:
    path: Path
    label: str
    mtime: float
    size: int


def cleanup_config_from_env(env: Dict[str, str]) -> CleanupConfig:
    return CleanupConfig(
        cleanup_archives_after_days=int(env.get("CLEANUP_ARCHIVES_AFTER_DAYS", "30")),
        cleanup_backups_after_days=int(env.get("CLEANUP_BACKUPS_AFTER_DAYS", "30")),
        cleanup_errors_after_days=int(env.get("CLEANUP_ERRORS_AFTER_DAYS", "90")),
        cleanup_logs_after_days=int(env.get("CLEANUP_LOGS_AFTER_DAYS", "30")),
        cleanup_json_data_after_days=int(env.get("CLEANUP_JSON_DATA_AFTER_DAYS", "0")),
        disk_space_threshold_gb=int(env.get("DISK_SPACE_THRESHOLD_GB", "10")),
        disk_space_target_gb=int(env.get("DISK_SPACE_TARGET_GB", "0")),
        disk_cleanup_include_json_data=env.get("DISK_CLEANUP_INCLUDE_JSON_DATA", "false").lower()
        == "true",
    )


def _file_age_days(path: Path) -> float:
    return (time.time() - path.stat().st_mtime) / 86400


class CleanupManager:
    def __init__(self, layout: StorageLayout, config: CleanupConfig):
        self.layout = layout
        self.config = config

    def _resolve_dir(self, key: str) -> Path:
        mapping = {
            "archives_inputs": self.layout.archive_inputs_dir,
            "archives_json": self.layout.archive_json_dir,
            "backups": self.layout.backups_dir,
            "errors": self.layout.errors_dir,
            "logs": self.layout.logs_dir,
            "json_data": self.layout.json_data_dir,
            "processing": self.layout.processing_dir,
            "inputs": self.layout.inputs_dir,
        }
        return mapping[key]

    def _age_limit(self, key: str) -> int:
        if key in ("archives_inputs", "archives_json"):
            return self.config.cleanup_archives_after_days
        if key == "backups":
            return self.config.cleanup_backups_after_days
        if key == "errors":
            return self.config.cleanup_errors_after_days
        if key == "logs":
            return self.config.cleanup_logs_after_days
        if key == "json_data":
            return self.config.cleanup_json_data_after_days
        return 0

    def _iter_files(self, base_dir: Path, mode: str) -> Iterator[_FileCandidate]:
        if not base_dir.is_dir():
            return

        if mode == "flat":
            paths = (p for p in base_dir.iterdir() if p.is_file())
        else:
            paths = (p for p in base_dir.rglob("*") if p.is_file())

        for filepath in paths:
            stat = filepath.stat()
            yield _FileCandidate(
                path=filepath,
                label=str(filepath.relative_to(self.layout.root_dir)),
                mtime=stat.st_mtime,
                size=stat.st_size,
            )

    def _cleanup_by_age_in_dir(
        self,
        base_dir: Path,
        max_age_days: int,
        label: str,
        mode: str,
    ) -> Dict[str, int]:
        if max_age_days <= 0:
            return {"deleted": 0, "errors": 0, "skipped": 1}

        deleted = 0
        errors = 0

        for candidate in self._iter_files(base_dir, mode):
            try:
                if _file_age_days(candidate.path) < max_age_days:
                    continue
                candidate.path.unlink()
                deleted += 1
                print(
                    f"[cleanup:age] deleted {candidate.path.name} from {label} "
                    f"(retention {max_age_days}d)",
                    flush=True,
                )
            except OSError as exc:
                errors += 1
                print(f"[cleanup:age] failed {candidate.path}: {exc}", flush=True)

        return {"deleted": deleted, "errors": errors, "skipped": 0}

    def run_age_cleanup(self) -> Dict[str, Dict[str, int]]:
        stats: Dict[str, Dict[str, int]] = {}
        for key, label, mode in _AGE_TARGETS:
            stats[key] = self._cleanup_by_age_in_dir(
                self._resolve_dir(key),
                self._age_limit(key),
                label,
                mode,
            )
        return stats

    def _collect_disk_candidates(self) -> List[_FileCandidate]:
        candidates: List[_FileCandidate] = []
        for key, _label, mode in _DISK_TIERS:
            if key == "json_data" and not self.config.disk_cleanup_include_json_data:
                continue
            tier_files = sorted(
                self._iter_files(self._resolve_dir(key), mode),
                key=lambda c: c.mtime,
            )
            candidates.extend(tier_files)
        return candidates

    def run_disk_cleanup(self) -> Dict[str, int | float | bool]:
        threshold = self.config.disk_space_threshold_gb
        if threshold <= 0:
            available = get_available_gb(self.layout.root_dir)
            return {
                "deleted": 0,
                "errors": 0,
                "skipped": 1,
                "triggered": False,
                "available_gb_before": round(available, 2),
                "available_gb_after": round(available, 2),
            }

        target = self.config.disk_space_target_gb or threshold
        root = self.layout.root_dir
        available_before = get_available_gb(root)

        if available_before >= threshold:
            return {
                "deleted": 0,
                "errors": 0,
                "skipped": 0,
                "triggered": False,
                "available_gb_before": round(available_before, 2),
                "available_gb_after": round(available_before, 2),
            }

        print(
            f"[cleanup:disk] low space {available_before:.2f} GB "
            f"(threshold {threshold} GB, target {target} GB)",
            flush=True,
        )

        deleted = 0
        errors = 0
        freed_bytes = 0

        for candidate in self._collect_disk_candidates():
            if get_available_gb(root) >= target:
                break
            try:
                candidate.path.unlink()
                deleted += 1
                freed_bytes += candidate.size
                print(
                    f"[cleanup:disk] deleted {candidate.path.name} "
                    f"({candidate.size / (1024 ** 2):.1f} MB)",
                    flush=True,
                )
            except OSError as exc:
                errors += 1
                print(f"[cleanup:disk] failed {candidate.path}: {exc}", flush=True)

        available_after = get_available_gb(root)
        print(
            f"[cleanup:disk] done: {deleted} file(s), "
            f"{freed_bytes / (1024 ** 3):.2f} GB freed, "
            f"{available_before:.2f} -> {available_after:.2f} GB",
            flush=True,
        )

        return {
            "deleted": deleted,
            "errors": errors,
            "skipped": 0,
            "triggered": True,
            "available_gb_before": round(available_before, 2),
            "available_gb_after": round(available_after, 2),
            "freed_gb": round(freed_bytes / (1024 ** 3), 2),
        }

    def run(self) -> Dict[str, object]:
        return {"age": self.run_age_cleanup(), "disk": self.run_disk_cleanup()}


def run_cleanup(layout: StorageLayout, config: CleanupConfig) -> Dict[str, object]:
    return CleanupManager(layout, config).run()


def run_disk_cleanup_only(layout: StorageLayout, config: CleanupConfig) -> Dict[str, int | float | bool]:
    return CleanupManager(layout, config).run_disk_cleanup()
