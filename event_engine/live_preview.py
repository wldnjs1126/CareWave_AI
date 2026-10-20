"""CareWave live candidate-event PREVIEW from new NCP sensor_raw rows.

NO database writes: SELECT sensor_raw only; append transitions to a LOCAL JSONL.
Default starts at the current latest raw_id, avoiding historical dummy replay.
Restart also starts at latest; it does not restore an active event (conservative).

Test a known historical range:
 python -m event_engine.live_preview --subject-id 1 --sensor-id 1 \
   --after-raw-id 193222 --until "2026-10-20 13:06:40" \
   --batch-size 400 --max-polls 1

Start watching new arrivals:
 python -m event_engine.live_preview --subject-id 1 --sensor-id 1

Do not deploy this provisional detector as a fall/medical alarm.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from event_engine.db_replay import _naive_datetime, _optional_finite
from event_engine.rule_replay import ReplayEngine, Sample

LATEST_RAW_ID = text("""
SELECT COALESCE(MAX(raw_id), 0)
FROM sensor_raw
WHERE subject_id = :subject_id AND sensor_id = :sensor_id
""")

NEW_ROWS = text("""
SELECT raw_id, subject_id, sensor_id, window_start, window_end,
       sample_count, presence, zone, avg_x, avg_y, avg_speed
FROM sensor_raw
WHERE subject_id = :subject_id AND sensor_id = :sensor_id
  AND raw_id > :after_raw_id
  AND (:until IS NULL OR window_end <= :until)
ORDER BY raw_id
LIMIT :row_limit
""")


def latest_raw_id(db_engine, *, subject_id: int, sensor_id: int) -> int:
    if subject_id <= 0 or sensor_id <= 0:
        raise ValueError("subject_id and sensor_id must be positive")
    with db_engine.connect() as conn:
        return int(conn.execute(
            LATEST_RAW_ID, {"subject_id": subject_id, "sensor_id": sensor_id}
        ).scalar_one())


def fetch_new_rows(db_engine, *, subject_id: int, sensor_id: int,
                   after_raw_id: int, batch_size: int,
                   until: datetime | None = None):
    """Return (ordered rows, has_more); no matching rows are silently dropped."""
    if subject_id <= 0 or sensor_id <= 0 or after_raw_id < 0:
        raise ValueError("Invalid sensor scope or cursor")
    if not 1 <= batch_size <= 5000:
        raise ValueError("batch_size must be between 1 and 5000")
    if until is not None and until.tzinfo is not None:
        raise ValueError("until must be a naive DB datetime")
    params = {
        "subject_id": subject_id, "sensor_id": sensor_id,
        "after_raw_id": after_raw_id, "row_limit": batch_size + 1,
        "until": until,
    }
    with db_engine.connect() as conn:
        rows = conn.execute(NEW_ROWS, params).mappings().all()
    return rows[:batch_size], len(rows) > batch_size


def to_sample(row) -> Sample:
    if row["presence"] is None:
        raise ValueError("sensor_raw.presence is missing")
    if row["sample_count"] is None or int(row["sample_count"]) <= 0:
        raise ValueError("sensor_raw.sample_count is zero or missing")
    if int(row["presence"]) not in (0, 1):
        raise ValueError("sensor_raw.presence must be 0 or 1")
    start = _naive_datetime(row["window_start"], "window_start")
    end = _naive_datetime(row["window_end"], "window_end")
    if end <= start:
        raise ValueError("sensor_raw has nonpositive window duration")
    return Sample(
        raw_id=int(row["raw_id"]),
        subject_id=int(row["subject_id"]),
        sensor_id=int(row["sensor_id"]),
        start=start, end=end, presence=bool(int(row["presence"])),
        zone=row["zone"],
        x=_optional_finite(row["avg_x"]),
        y=_optional_finite(row["avg_y"]),
        speed=_optional_finite(row["avg_speed"]),
    )


def event_key(event) -> str:
    return (
        f"{event.subject_id}:{event.sensor_id}:{event.event_type}:"
        f"{event.start_time.isoformat(timespec='microseconds')}"
    )


class LivePreview:
    """One sensor stream. Tracks transitions in memory; no persisted state."""

    def __init__(self):
        self.detector = ReplayEngine()
        self.last_sample: Sample | None = None
        self.counts: Counter = Counter()

    @staticmethod
    def _event_record(phase, event, raw_id):
        return {
            "recorded_at": datetime.now().astimezone().isoformat(),
            "record_type": "EVENT_TRANSITION",
            "phase": phase,
            "raw_id": raw_id,
            "event_key": event_key(event),
            "event": event.as_json(),
            "persisted_to_db": False,
        }

    @staticmethod
    def _quality_record(reason, raw_id):
        return {
            "recorded_at": datetime.now().astimezone().isoformat(),
            "record_type": "DATA_QUALITY",
            "reason": reason,
            "raw_id": raw_id,
            "persisted_to_db": False,
        }

    def process(self, sample: Sample) -> list[dict]:
        previous = self.last_sample
        if previous is not None and sample.start < previous.end:
            # Do not use unordered/overlapping observations for state tracking.
            self.counts["out_of_order"] += 1
            return [self._quality_record("overlapping_or_old_window", sample.raw_id)]

        state = self.detector.tracks.get((sample.subject_id, sample.sensor_id))
        previously_open = dict(state.open_events) if state is not None else {}
        prior_event_count = len(self.detector.events)
        transitions = []

        if previous is not None:
            gap = (sample.start - previous.end).total_seconds()
            if gap > 0.25:
                self.counts["missing_window"] += 1
                transitions.append(self._quality_record(
                    f"unknown_data_gap_seconds={gap:.3f}", sample.raw_id
                ))
        self.detector.add(sample)
        for event in previously_open.values():
            if event.end_time is not None:
                phase = ("INTERRUPTED" if event.close_reason in
                         ("sensor_data_gap", "unknown_movement_data")
                         else "ENDED")
                transitions.append(self._event_record(phase, event, sample.raw_id))
                self.counts[phase] += 1

        for event in self.detector.events[prior_event_count:]:
            transitions.append(self._event_record("STARTED", event, sample.raw_id))
            self.counts["STARTED"] += 1
            if event.end_time is not None:
                transitions.append(self._event_record("ENDED", event, sample.raw_id))
                self.counts["ENDED"] += 1

        self.last_sample = sample
        return transitions

    def invalid_row(self, raw_id: int, error: Exception):
        # Unknown quality must interrupt the continuity of any open episode.
        transitions = []
        state = None
        if self.last_sample is not None:
            state = self.detector.tracks.get(
                (self.last_sample.subject_id, self.last_sample.sensor_id)
            )
        if state is not None and self.last_sample is not None:
            for event in list(state.open_events.values()):
                self.detector._close(
                    state, event.event_type, self.last_sample.end,
                    "invalid_sensor_row"
                )
                transitions.append(self._event_record(
                    "INTERRUPTED", event, raw_id
                ))
            self.detector.tracks.pop(
                (self.last_sample.subject_id, self.last_sample.sensor_id), None
            )
        self.last_sample = None
        self.counts["invalid_row"] += 1
        transitions.append(self._quality_record(
            f"invalid_row:{type(error).__name__}:{error}", raw_id
        ))
        return transitions


def watch(db_engine, *, subject_id: int, sensor_id: int, after_raw_id: int,
          batch_size: int, poll_seconds: float, max_polls: int | None,
          output: Path, until: datetime | None = None) -> dict:
    preview = LivePreview()
    cursor = after_raw_id
    polls = 0
    processed = 0
    output.parent.mkdir(parents=True, exist_ok=True)
    while True:
        rows, has_more = fetch_new_rows(
            db_engine, subject_id=subject_id, sensor_id=sensor_id,
            after_raw_id=cursor, batch_size=batch_size, until=until
        )
        transitions = []
        for row in rows:
            raw_id = int(row["raw_id"])
            if raw_id <= cursor:
                raise RuntimeError("Non-monotonic raw_id cursor")
            try:
                sample = to_sample(row)
                if sample.subject_id != subject_id or sample.sensor_id != sensor_id:
                    raise ValueError("Subject/sensor mismatch")
                transitions.extend(preview.process(sample))
            except (ValueError, TypeError, OverflowError) as error:
                transitions.extend(preview.invalid_row(raw_id, error))
            cursor = raw_id
        if transitions:
            # Text output only. The DB connection is never used for writes.
            with output.open("a", encoding="utf-8") as target:
                for record in transitions:
                    target.write(json.dumps(record, ensure_ascii=False) + "\n")
        processed += len(rows)
        polls += 1
        print(
            f"poll={polls} rows={len(rows)} transitions={len(transitions)} "
            f"cursor={cursor} more_rows_pending={has_more} "
            f"data_gaps={preview.counts['missing_window']}",
            flush=True,
        )
        if max_polls is not None and polls >= max_polls:
            break
        if not has_more:
            time.sleep(poll_seconds)
    return {
        "polled": polls, "rows": processed,
        "cursor": cursor, "output": str(output),
        "counts": dict(preview.counts),
    }


def main():
    parser = argparse.ArgumentParser(
        description="CareWave live event PREVIEW, sensor_raw SELECT only"
    )
    parser.add_argument("--subject-id", type=int, required=True)
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--after-raw-id", type=int, help=(
        "Historical replay cursor. If omitted, start at latest raw_id."
    ))
    parser.add_argument("--until", help="Optional upper KST datetime for replay")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--max-polls", type=int)
    parser.add_argument("--out", default="logs/carewave_live_preview.jsonl")
    args = parser.parse_args()
    if args.subject_id <= 0 or args.sensor_id <= 0:
        parser.error("subject and sensor IDs must be positive")
    if args.after_raw_id is not None and args.after_raw_id < 0:
        parser.error("--after-raw-id must be nonnegative")
    if args.max_polls is not None and args.max_polls < 1:
        parser.error("--max-polls must be positive")
    if not 0.5 <= args.poll_seconds <= 3600.0:
        parser.error("--poll-seconds must be between 0.5 and 3600 seconds")
    if not 1 <= args.batch_size <= 5000:
        parser.error("--batch-size must be between 1 and 5000")
    if args.until is not None and args.after_raw_id is None:
        parser.error("--until is only supported with --after-raw-id")

    from database.connection import engine
    cursor = args.after_raw_id
    if cursor is None:
        cursor = latest_raw_id(
            engine, subject_id=args.subject_id, sensor_id=args.sensor_id
        )
        print(f"Watching NEW data only, starting after raw_id={cursor}")
    else:
        print(f"Replaying after raw_id={cursor} (historical preview mode)")
    until = (_naive_datetime(args.until, "until") if args.until else None)
    print("SAFE MODE: sensor_raw SELECT only; life_event is NEVER written.")
    try:
        result = watch(
            engine, subject_id=args.subject_id, sensor_id=args.sensor_id,
            after_raw_id=cursor, batch_size=args.batch_size,
            poll_seconds=args.poll_seconds, max_polls=args.max_polls,
            until=until, output=Path(args.out),
        )
    except KeyboardInterrupt:
        print("Stopped by user. No database rows were changed.")
    else:
        print("Summary:", json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
