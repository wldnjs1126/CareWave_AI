"""Read-only CareWave sensor_raw -> provisional event replay from NCP MySQL.

Example (Windows, from C:\\CareWave_AI):
    python -m event_engine.db_replay --subject-id 1 --sensor-id 1 ^
      --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"

This script ONLY performs a SELECT against sensor_raw and writes local JSON.
It NEVER inserts into or modifies life_event.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from event_engine.rule_replay import ReplayEngine, Sample

SENSOR_QUERY = text("""
SELECT
    raw_id, subject_id, sensor_id, window_start, window_end,
    sample_count, presence, zone, avg_x, avg_y, avg_speed
FROM sensor_raw
WHERE subject_id = :subject_id
  AND sensor_id = :sensor_id
  AND window_start >= :since
  AND window_end <= :until
ORDER BY window_start, raw_id
LIMIT :fetch_limit
""")


def _naive_datetime(value: datetime | str, name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip())
    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise ValueError(f"{name} must be a local naive datetime")
    return value


def _optional_finite(value):
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def load_sensor_rows(db_engine, *, subject_id: int, sensor_id: int,
                     since: datetime, until: datetime,
                     max_rows: int = 5000) -> list[Sample]:
    """Retrieve one subject + sensor only. Never skip rows silently."""
    if subject_id <= 0 or sensor_id <= 0:
        raise ValueError("subject-id and sensor-id must be positive")
    if since.tzinfo is not None or until.tzinfo is not None or since >= until:
        raise ValueError("since and until must be naive datetimes in increasing order")
    if not 1 <= max_rows <= 50000:
        raise ValueError("max_rows must be between 1 and 50000")

    params = {
        "subject_id": subject_id, "sensor_id": sensor_id,
        "since": since, "until": until, "fetch_limit": max_rows + 1,
    }
    with db_engine.connect() as conn:
        rows = conn.execute(SENSOR_QUERY, params).mappings().all()

    if len(rows) > max_rows:
        raise ValueError(
            f"More than {max_rows} rows match. Narrow --since / --until, "
            "or explicitly raise --max-rows. No partial replay produced."
        )

    result = []
    for row in rows:
        start = _naive_datetime(row["window_start"], "window_start")
        end = _naive_datetime(row["window_end"], "window_end")
        if row["presence"] is None:
            raise ValueError(f"Missing presence at raw_id={row['raw_id']}")
        result.append(Sample(
            raw_id=int(row["raw_id"]),
            subject_id=int(row["subject_id"]),
            sensor_id=int(row["sensor_id"]),
            start=start,
            end=end,
            presence=bool(int(row["presence"])),
            zone=row["zone"],
            x=_optional_finite(row["avg_x"]),
            y=_optional_finite(row["avg_y"]),
            speed=_optional_finite(row["avg_speed"]),
        ))
    return result


def replay_samples(samples: list[Sample]) -> tuple[list[dict], int]:
    engine = ReplayEngine()
    for sample in samples:
        engine.add(sample)
    gaps = sum(state.skipped_gaps for state in engine.tracks.values())
    return engine.finish(), gaps


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only sensor_raw validation (NO database writes)"
    )
    parser.add_argument("--subject-id", type=int, required=True)
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--since", required=True,
                        help='KST local datetime e.g. "2026-10-20 13:00:30"')
    parser.add_argument("--until", required=True,
                        help='KST local datetime e.g. "2026-10-20 13:06:40"')
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--out", default="carewave_db_event_preview.json")
    args = parser.parse_args()
    since = _naive_datetime(args.since, "since")
    until = _naive_datetime(args.until, "until")

    # Lazy import so unit tests can use an in-memory DB without .env.
    from database.connection import engine as db_engine
    samples = load_sensor_rows(
        db_engine, subject_id=args.subject_id, sensor_id=args.sensor_id,
        since=since, until=until, max_rows=args.max_rows,
    )
    events, gaps = replay_samples(samples)
    destination = Path(args.out)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("READ ONLY: No life_event or sensor_raw data was changed.")
    print("source: sensor_raw (NCP MySQL)")
    print(f"subject_id={args.subject_id}, sensor_id={args.sensor_id}")
    print(f"period: {since} ~ {until}")
    print(f"input_rows={len(samples)}, candidate_events={len(events)}, gaps={gaps}")
    print("event_counts:", dict(Counter(e["event_type"] for e in events)))
    if samples:
        print(f"first_raw_id={samples[0].raw_id}, "
              f"last_raw_id={samples[-1].raw_id}")
        print("zones:", dict(Counter(s.zone for s in samples)))
    if any(s.zone not in ("BED", "ROOM", None) for s in samples):
        print("Note: verify zone meanings against current CareWave room geometry.")
    print("No FALL model is enabled. BED and SLEEP require valid BED zones.")
    print(f"preview_file={destination}")


if __name__ == "__main__":
    main()
