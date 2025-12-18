from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional

from sqlalchemy.orm import Session

from ..models.events import Event
from ..models.event_orm import EventORM


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse un timestamp ISO8601 (avec ou sans 'Z') en datetime Python."""
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        return None


def json_to_event_model(obj: Mapping[str, Any], source_file: str) -> Event:
    """
    Transforme un objet JSON (tel que produit dans json_data) en modèle Event.
    """
    return Event(
        id=None,
        source_file=source_file,
        business_id=obj.get("BUSINESS_ID"),
        origin=obj.get("ORIGIN"),
        origin_id=obj.get("ORIGIN_ID"),
        log_category=obj.get("LOG_CATEGORY"),
        service=obj.get("SERVICE"),
        activity=obj.get("ACTIVITY"),
        activity_result=obj.get("ACTIVITY_RESULT"),
        owner=obj.get("OWNER"),
        host=obj.get("HOST"),
        event_timestamp=_parse_ts(obj.get("TIMESTAMP")),
        duration=obj.get("DURATION"),
        operation=obj.get("OPERATION"),
        reference_id=obj.get("REFERENCE_ID"),
        request_id=obj.get("REQUEST_ID"),
        request_time=_parse_ts(obj.get("REQUEST_TIME")),
        response_time=_parse_ts(obj.get("RESPONSE_TIME")),
        brs_url=obj.get("BRS_URL"),
        request_message=obj.get("REQUEST_MESSAGE"),
        brs_request=obj.get("BRS_REQUEST"),
        brs_response=obj.get("BRS_RESPONSE"),
        response_published_to_mosip_queue=obj.get("RESPONSE_PUBLISHED_TO_MOSIP_QUEUE"),
        payload=dict(obj),
    )


_INSERT_SQL = """
INSERT INTO abis.events (
    source_file,
    business_id,
    origin,
    origin_id,
    log_category,
    service,
    activity,
    activity_result,
    owner,
    host,
    event_timestamp,
    duration,
    operation,
    reference_id,
    request_id,
    request_time,
    response_time,
    brs_url,
    request_message,
    brs_request,
    brs_response,
    parse_error,
    payload
) VALUES (
    %(source_file)s,
    %(business_id)s,
    %(origin)s,
    %(origin_id)s,
    %(log_category)s,
    %(service)s,
    %(activity)s,
    %(activity_result)s,
    %(owner)s,
    %(host)s,
    %(event_timestamp)s,
    %(duration)s,
    %(operation)s,
    %(reference_id)s,
    %(request_id)s,
    %(request_time)s,
    %(response_time)s,
    %(brs_url)s,
    %(request_message)s,
    %(brs_request)s,
    %(brs_response)s,
    %(parse_error)s,
    %(payload)s
)
RETURNING id
"""


def events_model_to_database(
    events: Iterable[Event],
    session: Session,
) -> List[int]:
    """
    Insère une liste de modèles Event dans la table abis.events.

    Args:
        events: itérable de Event déjà mappés.
        conn: connexion psycopg2 ouverte.

    Returns:
        Liste des identifiants (id) générés.
    """
    ids: List[int] = []
    for event in events:
        orm = EventORM(
            source_file=event.source_file,
            business_id=event.business_id,
            origin=event.origin,
            origin_id=event.origin_id,
            log_category=event.log_category,
            service=event.service,
            activity=event.activity,
            activity_result=event.activity_result,
            owner=event.owner,
            host=event.host,
            event_timestamp=event.event_timestamp,
            duration=event.duration,
            operation=event.operation,
            reference_id=event.reference_id,
            request_id=event.request_id,
            request_time=event.request_time,
            response_time=event.response_time,
            brs_url=event.brs_url,
            request_message=event.request_message,
            brs_request=event.brs_request,
            brs_response=event.brs_response,
            response_published_to_mosip_queue=event.response_published_to_mosip_queue,
            payload=event.payload,
        )
        session.add(orm)
        session.flush()
        ids.append(orm.id)
    session.commit()
    return ids


