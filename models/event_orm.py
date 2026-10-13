from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from .sa_base import Base


class EventORM(Base):
    """
    Modèle ORM SQLAlchemy mappant la table `{schema}.events`.
    Le schéma effectif est défini via DB_SCHEMA (configure_orm_schema).
    """

    __tablename__ = "events"
    __table_args__ = {"schema": "abis"}

    id: int = Column(Integer, primary_key=True, autoincrement=True)

    # Métadonnées techniques
    source_file: str = Column(Text, nullable=False)

    # Champs principaux
    business_id: Optional[str] = Column(UUID(as_uuid=False), nullable=True)
    origin: Optional[str] = Column(Text)
    origin_id: Optional[str] = Column(Text)
    log_category: Optional[str] = Column(Text)
    service: Optional[str] = Column(Text)
    activity: Optional[str] = Column(Text)
    activity_result: Optional[str] = Column(Text)
    owner: Optional[str] = Column(Text)
    host: Optional[str] = Column(Text)

    event_timestamp: Optional[datetime] = Column(DateTime(timezone=True))
    duration: Optional[int] = Column(Integer)
    operation: Optional[str] = Column(Text)
    reference_id: Optional[str] = Column(Text)
    ref_id: Optional[str] = Column(Text)
    request_id: Optional[str] = Column(Text)
    request_time: Optional[datetime] = Column(DateTime(timezone=True))
    response_time: Optional[datetime] = Column(DateTime(timezone=True))

    brs_url: Optional[str] = Column(Text)

    # Sous-objets JSONB
    request_message: Optional[dict[str, Any]] = Column(JSONB)
    # Non persisté : None doit donner un NULL SQL (pas le JSON 'null').
    brs_request: Optional[dict[str, Any]] = Column(JSONB(none_as_null=True))
    brs_response: Optional[Any] = Column(JSONB)
    response_published_to_mosip_queue: Optional[dict[str, Any]] = Column(JSONB)

    # Payload complet — non persisté : None doit donner un NULL SQL
    # (sans none_as_null=True, SQLAlchemy écrirait le JSON 'null').
    payload: Optional[dict[str, Any]] = Column(JSONB(none_as_null=True), nullable=True)

    created_at: datetime = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )


