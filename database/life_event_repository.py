"""Manual event-start/end persistence using the actual life_event schema.

WARNING:
- This module does not detect events and does not run automatically.
- Use a *single* event engine writer until DB-level uniqueness is designed.
- start_time and detect_at must be naive datetimes in the SAME KST clock basis
  used by sensor_raw.window_start; MySQL DATETIME has no timezone information.
- Risk labels are temporary policy values, not model confidence.
- The database must already contain life_event.risk_level.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from database.connection import engine
from event_engine.risk_policy import risk_for_event


FIND_OPEN_EVENT = text(
    """
    SELECT event_id
      FROM life_event
     WHERE subject_id = :subject_id
       AND event_type = :event_type
       AND end_time IS NULL
     ORDER BY event_id DESC
     LIMIT 1
     FOR UPDATE
    """
)

INSERT_LIFE_EVENT = text(
    """
    INSERT INTO life_event (
        subject_id, sensor_id, event_type, start_time, detect_at,
        end_time, duration_sec, zone, confidence, risk_level, evidence
    ) VALUES (
        :subject_id, :sensor_id, :event_type, :start_time, :detect_at,
        NULL, NULL, :zone, :confidence, :risk_level, :evidence
    )
    """
)

CLOSE_LIFE_EVENT = text(
    """
    UPDATE life_event
       SET end_time = :end_time,
           duration_sec = TIMESTAMPDIFF(SECOND, start_time, :end_time)
     WHERE event_id = :event_id
       AND end_time IS NULL
       AND start_time <= :end_time
    """
)


def _require_naive_datetime(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise ValueError(f"{field} must be a naive KST datetime")


def start_event(
    *,
    subject_id: int,
    event_type: str,
    start_time: datetime,
    detect_at: datetime,
    sensor_id: int | None = None,
    zone: str | None = None,
    confidence: float | None = None,
    evidence: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    """Insert an open event, or reuse an existing open event (single writer).

    Returns (event_id, created). Does not generate detection labels.
    """
    risk_level = risk_for_event(event_type)
    if not isinstance(subject_id, int) or subject_id <= 0:
        raise ValueError("subject_id must be a positive integer")
    _require_naive_datetime(start_time, "start_time")
    _require_naive_datetime(detect_at, "detect_at")
    if detect_at < start_time:
        raise ValueError("detect_at must not precede start_time")
    if confidence is not None and not 0 <= confidence <= 1:
        raise ValueError("confidence must be between 0 and 1")
    if zone is not None and len(zone) > 30:
        raise ValueError("zone must be at most 30 characters")

    evidence_json = (
        json.dumps(evidence, ensure_ascii=False, allow_nan=False)
        if evidence is not None else None
    )
    params = {
        "subject_id": subject_id,
        "sensor_id": sensor_id,
        "event_type": event_type,
        "start_time": start_time,
        "detect_at": detect_at,
        "zone": zone,
        "confidence": confidence,
        "risk_level": risk_level,
        "evidence": evidence_json,
    }

    with engine.begin() as connection:
        existing = connection.execute(
            FIND_OPEN_EVENT, {"subject_id": subject_id, "event_type": event_type}
        ).scalar_one_or_none()
        if existing is not None:
            return int(existing), False

        result = connection.execute(INSERT_LIFE_EVENT, params)
        return int(result.lastrowid), True


def end_event(*, event_id: int, end_time: datetime) -> bool:
    """Close a previously opened event exactly once; return False if unchanged."""
    if not isinstance(event_id, int) or event_id <= 0:
        raise ValueError("event_id must be a positive integer")
    _require_naive_datetime(end_time, "end_time")

    with engine.begin() as connection:
        result = connection.execute(
            CLOSE_LIFE_EVENT, {"event_id": event_id, "end_time": end_time}
        )
        return result.rowcount == 1
