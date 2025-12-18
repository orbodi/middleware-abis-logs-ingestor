from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class Event:
    """
    Modèle Python qui mappe la table SQL `abis.events`.

    Ce modèle est volontairement simple (dataclass) pour être utilisé comme
    DTO entre le parseur (handler) et la couche repository.
    """

    id: Optional[int]              # clé primaire (None avant insertion)

    # Métadonnées techniques
    source_file: str

    # Champs principaux des logs ABIS
    business_id: Optional[str]
    origin: Optional[str]
    origin_id: Optional[str]
    log_category: Optional[str]
    service: Optional[str]
    activity: Optional[str]
    activity_result: Optional[str]
    owner: Optional[str]
    host: Optional[str]

    event_timestamp: Optional[datetime]
    duration: Optional[int]
    operation: Optional[str]
    reference_id: Optional[str]
    request_id: Optional[str]
    request_time: Optional[datetime]
    response_time: Optional[datetime]

    brs_url: Optional[str]

    # Sous-objets complexes
    request_message: Optional[dict[str, Any]]
    brs_request: Optional[dict[str, Any]]
    brs_response: Optional[Any]  # peut être une liste ou autre structure JSON
    response_published_to_mosip_queue: Optional[dict[str, Any]]

    # Payload complet (JSON complet de l'événement)
    payload: dict[str, Any]


