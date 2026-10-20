"""Temporary CareWave 4-level risk policy.

7 detection categories use 8 event_type values because presence and absence
are recorded separately. Risk levels are a policy, not an AI prediction.
"""

EVENT_RISK_LEVELS: dict[str, str] = {
    "PRESENCE": "정상",
    "MOVEMENT": "정상",
    "BED_STAY": "정상",
    "SLEEP": "정상",
    "ABSENCE": "관찰",
    "NIGHT_EXIT": "관찰",
    "INACTIVITY": "주의",
    "FALL": "위험",
}


def risk_for_event(event_type: str) -> str:
    """Return the configured risk level; reject unexpected event names."""
    try:
        return EVENT_RISK_LEVELS[event_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported CareWave event_type: {event_type!r}") from exc
