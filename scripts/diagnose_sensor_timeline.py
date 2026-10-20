"""Read-only diagnosis of sensor_raw timeline gaps and insertion cadence.

Usage:
 python -m scripts.diagnose_sensor_timeline --subject-id 1 --sensor-id 1 \
   --since "2026-10-20 13:00:20" --until "2026-10-20 13:06:45"

The timestamp gap in window_start does NOT automatically imply that ESP32,
FastAPI, or radar frames were lost. Compare with created_at and frame counts.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
from statistics import mean
from sqlalchemy import text


SENSOR_TIMELINE_SQL = text("""
SELECT raw_id, sensor_id, subject_id, window_start, window_end,
       created_at, sample_count
FROM sensor_raw
WHERE subject_id = :subject_id
  AND sensor_id = :sensor_id
  AND window_start >= :since
  AND window_start < :until
ORDER BY window_start, raw_id
LIMIT :limit
""")


def _dt(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def analyze_timeline(rows):
    """Evaluate recorded timestamps only; do not infer radar frame delivery."""
    result = {"count": len(rows), "gaps": [], "created_deltas": [],
              "bad_windows": 0, "frame_counts": []}
    previous = None
    for item in rows:
        row = dict(item)
        start, end, created = (
            _dt(row["window_start"]), _dt(row["window_end"]),
            _dt(row["created_at"]),
        )
        if end <= start:
            result["bad_windows"] += 1
        if row.get("sample_count") is not None:
            result["frame_counts"].append(int(row["sample_count"]))
        if previous is not None:
            window_gap = (start - previous["end"]).total_seconds()
            created_diff = (created - previous["created"]).total_seconds()
            result["created_deltas"].append(created_diff)
            if window_gap > 0.25:
                raw_diff = int(row["raw_id"]) - previous["raw_id"]
                # Diagnostic clue, NOT proof of successful transport.
                suspected_rebase = (
                    window_gap >= 3.5 and created_diff <= 2.5
                    and created_diff >= 0 and raw_diff == 1
                )
                result["gaps"].append({
                    "previous_raw_id": previous["raw_id"],
                    "next_raw_id": int(row["raw_id"]),
                    "previous_end": previous["end"],
                    "next_start": start,
                    "empty_seconds": window_gap,
                    "created_delta_seconds": created_diff,
                    "raw_id_delta": raw_diff,
                    "suspected_timeline_rebase": suspected_rebase,
                })
        previous = {"raw_id": int(row["raw_id"]),
                    "end": end, "created": created}
    result["suspected_rebases"] = sum(
        gap["suspected_timeline_rebase"] for gap in result["gaps"]
    )
    return result


def query_timeline(db_engine, *, subject_id, sensor_id, since, until, max_rows=2000):
    if subject_id <= 0 or sensor_id <= 0 or since >= until:
        raise ValueError("Invalid subject, sensor or time window")
    if not 1 <= max_rows <= 10000:
        raise ValueError("max_rows must be 1..10000")
    with db_engine.connect() as conn:
        rows = conn.execute(SENSOR_TIMELINE_SQL, dict(
            subject_id=subject_id, sensor_id=sensor_id,
            since=since, until=until, limit=max_rows + 1
        )).mappings().all()
    if len(rows) > max_rows:
        raise ValueError("Too many rows; use a shorter time range")
    return analyze_timeline(rows)


def main():
    parser = argparse.ArgumentParser(
        description="CareWave window_start vs created_at diagnostic (READ ONLY)"
    )
    parser.add_argument("--subject-id", type=int, required=True)
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--max-rows", type=int, default=2000)
    args = parser.parse_args()
    from database.connection import engine
    result = query_timeline(
        engine, subject_id=args.subject_id, sensor_id=args.sensor_id,
        since=datetime.fromisoformat(args.since),
        until=datetime.fromisoformat(args.until),
        max_rows=args.max_rows
    )
    intervals = result["created_deltas"]
    frames = result["frame_counts"]
    print("READ ONLY: sensor_raw SELECT; no DB changes")
    print("rows:", result["count"])
    print("missing_window_intervals:", len(result["gaps"]))
    print("suspected_time_rebases:", result["suspected_rebases"])
    if intervals:
        print(f"mean_created_at_interval_sec: {mean(intervals):.3f}")
    if frames:
        print(f"mean_sample_count_per_row: {mean(frames):.2f}")
    print("window_gap_distribution:",
          dict(Counter(gap["empty_seconds"] for gap in result["gaps"])))
    for gap in result["gaps"][:35]:
        print(
            f"raw_id {gap['previous_raw_id']} -> {gap['next_raw_id']} | "
            f"{gap['previous_end']} -> {gap['next_start']} | "
            f"window_gap={gap['empty_seconds']:.1f}s, "
            f"DB_insert_delta={gap['created_delta_seconds']:.1f}s, "
            f"suspected_rebase={gap['suspected_timeline_rebase']}"
        )
    print("NOTE: Need ESP32 consecutive frame_id evidence to confirm no raw radar")
    print("      frame loss. The 1s DB window is currently defined per HTTP batch.")


if __name__ == "__main__":
    main()
