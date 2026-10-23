"""Safe, opt-in backfill of COMPLETED provisional events from sensor_raw.

Default: read-only preview. Do not use this script as the live event daemon.

Example preview:
 python -m event_engine.db_backfill --subject-id 1 --sensor-id 1 ^
  --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"

Only after reviewing the output and independently verifying labels, a single
explicit event type can be committed with --apply --confirm and --expected-count.
No missing time is fabricated. Old open demo rows are not touched.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from typing import Any

from sqlalchemy import text

from event_engine.db_replay import load_sensor_rows, replay_samples
from event_engine.risk_policy import risk_for_event

# No FALL: no validated fall detector is connected.
ALLOWED_TYPES = frozenset({
    "PRESENCE", "ABSENCE", "MOVEMENT", "BED_STAY",
    "SLEEP", "INACTIVITY", "NIGHT_EXIT",
})
# A gap does not prove that the physical event ended.
UNVERIFIED_END_REASONS = frozenset({
    "sensor_data_gap", "unknown_movement_data",
})

EXACT_MATCH = text("""
SELECT event_id, detect_at, end_time, risk_level
FROM life_event
WHERE subject_id = :subject_id
  AND sensor_id = :sensor_id
  AND event_type = :event_type
  AND start_time = :start_time
ORDER BY event_id DESC
LIMIT 2
""")
INSERT_CLOSED_EVENT = text("""
INSERT INTO life_event (
  subject_id, sensor_id, event_type, start_time, detect_at,
  end_time, duration_sec, zone, confidence, risk_level, evidence
) VALUES (
  :subject_id, :sensor_id, :event_type, :start_time, :detect_at,
  :end_time, :duration_sec, :zone, NULL, :risk_level, :evidence
)
""")


def _parse_time(value: str | datetime, label: str) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is not None:
        raise ValueError(f"{label} must be a timezone-naive KST datetime")
    return result


def review_candidates(events: list[dict], *, selected_types: set[str],
                      since: datetime, until: datetime) -> tuple[list[dict], Counter]:
    """Accept only explicitly selected completed events in the exact time range."""
    if not selected_types or not selected_types <= ALLOWED_TYPES:
        raise ValueError("Select at least one known non-FALL event type")
    if since >= until:
        raise ValueError("Invalid time interval")
    prepared: list[dict] = []
    skipped: Counter = Counter()
    for item in events:
        event_type = item["event_type"]
        if event_type not in selected_types:
            skipped["other_type"] += 1
            continue
        if item.get("source") != "RULE_REPLAY_DRY_RUN":
            raise ValueError("Unexpected event source: refusing backfill")
        start = _parse_time(item["start_time"], "start_time")
        detected = _parse_time(item["detect_at"], "detect_at")
        end_str = item.get("end_time")
        if not end_str:
            skipped["still_open_at_end_of_observation"] += 1
            continue
        end = _parse_time(end_str, "end_time")
        if item.get("close_reason") in UNVERIFIED_END_REASONS:
            skipped["ended_by_missing_data"] += 1
            continue
        if start < since or not (start <= detected <= until):
            raise ValueError("Candidate timestamps extend outside the requested range")
        if end < start or end > until:
            raise ValueError("Event ends outside the requested range")
        if item.get("risk_level") != risk_for_event(event_type):
            raise ValueError("Unexpected risk level")
        if item.get("confidence") is not None:
            raise ValueError("Unverified rule candidates cannot contain AI confidence")
        subject_id, sensor_id = int(item["subject_id"]), int(item["sensor_id"])
        if subject_id <= 0 or sensor_id <= 0:
            raise ValueError("Missing subject/sensor identity")
        zone = item.get("zone")
        if zone is not None and len(zone) > 30:
            raise ValueError("Invalid zone")
        evidence = {
            "source": "CAREWAVE_RULE_REPLAY_V1",
            "validation": "PROVISIONAL_NOT_CLINICALLY_VALIDATED",
            "close_reason": item.get("close_reason"),
            "bridged_gaps": int(item.get("bridged_gaps", 0)),
            "analysis_window_start": since.isoformat(sep=" "),
            "analysis_window_end": until.isoformat(sep=" "),
        }
        params = {
            "subject_id": subject_id, "sensor_id": sensor_id,
            "event_type": event_type, "start_time": start,
            "detect_at": detected, "end_time": end,
            "duration_sec": max(0, int((end - start).total_seconds())),
            "zone": zone, "risk_level": risk_for_event(event_type),
            "evidence": json.dumps(evidence, ensure_ascii=False),
        }
        prepared.append(params)
    return prepared, skipped


def save_completed(db_engine, prepared: list[dict]) -> dict[str, int]:
    """Atomic, idempotent single-writer insert for a reviewed historical session.

    Does not touch old or open records. No DB-wide uniqueness guarantee yet.
    """
    if not prepared:
        return {"inserted": 0, "existing": 0}
    outcomes = Counter()
    with db_engine.begin() as conn:
        for record in prepared:
            existing = conn.execute(EXACT_MATCH, record).mappings().all()
            if len(existing) > 1:
                raise RuntimeError("Multiple matching existing events; cannot proceed")
            if existing:
                found = existing[0]
                old_detected = _parse_time(found["detect_at"], "stored detect_at")
                old_end = (
                    _parse_time(found["end_time"], "stored end_time")
                    if found["end_time"] is not None else None
                )
                if (
                    old_detected != record["detect_at"]
                    or old_end != record["end_time"]
                    or found["risk_level"] != record["risk_level"]
                ):
                    raise RuntimeError(
                        f"Conflicting existing event_id={found['event_id']}; "
                        "historical data was not modified"
                    )
                outcomes["existing"] += 1
                continue
            conn.execute(INSERT_CLOSED_EVENT, record)
            outcomes["inserted"] += 1
    return {"inserted": outcomes["inserted"], "existing": outcomes["existing"]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Reviewed historical event backfill")
    parser.add_argument("--subject-id", type=int, required=True)
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--event-type", action="append", dest="event_types",
                        choices=sorted(ALLOWED_TYPES))
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()

    since = _parse_time(args.since, "since")
    until = _parse_time(args.until, "until")
    if since >= until:
        parser.error("since must be before until")

    selected = set(args.event_types or ("PRESENCE", "MOVEMENT"))
    if args.apply:
        if not args.event_types or len(selected) != 1:
            parser.error("--apply requires exactly one explicit --event-type")
        if args.confirm != "WRITE_REVIEWED_COMPLETED_EVENTS":
            parser.error("--apply requires --confirm WRITE_REVIEWED_COMPLETED_EVENTS")
        if args.expected_count is None or args.expected_count < 1:
            parser.error("--apply requires positive --expected-count")
    elif args.confirm or args.expected_count is not None:
        parser.error("--confirm / --expected-count are only used with --apply")

    from database.connection import engine
    samples = load_sensor_rows(
        engine, subject_id=args.subject_id, sensor_id=args.sensor_id,
        since=since, until=until, max_rows=args.max_rows)
    events, gaps = replay_samples(samples)
    candidates, skipped = review_candidates(
        events, selected_types=selected, since=since, until=until)
    if any(rec["subject_id"] != args.subject_id or
           rec["sensor_id"] != args.sensor_id for rec in candidates):
        raise ValueError("Mismatched subject or sensor: refusing")
    print(f"sensor_raw_rows={len(samples)}, gaps={gaps}, "
          f"all_candidates={len(events)}")
    print("selected_event_types:", ", ".join(sorted(selected)))
    print(f"eligible_completed={len(candidates)}, excluded={dict(skipped)}")
    for rec in candidates[:30]:
        print(f"PREVIEW: {rec['event_type']:12s} "
              f"{rec['start_time']} -> {rec['end_time']} "
              f"risk={rec['risk_level']}")
    if len(candidates) > 30:
        print(f"... and {len(candidates)-30} more")
    if not args.apply:
        print("DRY RUN: Only sensor_raw SELECT executed; no events saved.")
        print("Review classification errors before an explicit commit.")
        return
    if len(candidates) != args.expected_count:
        raise ValueError("Eligible count changed; refuse to write")
    stats = save_completed(engine, candidates)
    print("COMMITTED:", stats)
    print("No old rows were updated; no open event was inserted.")


if __name__ == "__main__":
    main()
