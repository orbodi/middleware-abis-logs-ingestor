"""Retry avec backoff exponentiel pour les opérations base de données."""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Dict, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError

T = TypeVar("T")


@dataclass
class DbRetryConfig:
    max_retries: int = 3
    delay_base: float = 5.0
    delay_max: float = 60.0
    backoff_multiplier: float = 2.0


def db_retry_config_from_env(env: Dict[str, str]) -> DbRetryConfig:
    return DbRetryConfig(
        max_retries=int(env.get("MAX_RETRY_DB", "3")),
        delay_base=float(env.get("DB_RETRY_DELAY_BASE", "5")),
        delay_max=float(env.get("DB_RETRY_DELAY_MAX", "60")),
        backoff_multiplier=float(env.get("DB_RETRY_BACKOFF_MULTIPLIER", "2")),
    )


def calculate_backoff_delay(
    attempt: int,
    config: DbRetryConfig,
    use_jitter: bool = True,
) -> float:
    delay = config.delay_base * (config.backoff_multiplier ** (attempt - 1))
    delay = min(delay, config.delay_max)
    if use_jitter:
        delay += delay * 0.25 * random.random()
    return delay


def is_retryable_db_error(exc: BaseException) -> bool:
    """True pour les erreurs transitoires (connexion, timeout, serveur indisponible)."""
    if isinstance(exc, OperationalError):
        return True
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True

    orig = getattr(exc, "orig", None)
    if orig is not None:
        try:
            import psycopg2

            if isinstance(orig, (psycopg2.OperationalError, psycopg2.InterfaceError)):
                return True
        except ImportError:
            pass

    return False


def run_with_db_retry(
    operation: Callable[[], T],
    config: DbRetryConfig,
    operation_name: str = "db",
) -> T:
    """Exécute operation() avec retry sur erreurs DB transitoires."""
    last_error: BaseException | None = None

    for attempt in range(1, config.max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt >= config.max_retries or not is_retryable_db_error(exc):
                raise

            delay = calculate_backoff_delay(attempt, config)
            print(
                f"[retry] {operation_name} failed ({attempt}/{config.max_retries}): "
                f"{exc}. Retrying in {delay:.1f}s...",
                flush=True,
            )
            time.sleep(delay)

    if last_error:
        raise last_error
    raise RuntimeError(f"{operation_name} failed after {config.max_retries} attempts")
