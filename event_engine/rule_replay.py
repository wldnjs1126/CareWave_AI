"""CSV-only provisional CareWave event replay. NEVER writes to life_event.

Run: python -m event_engine.rule_replay sensor_raw_validation_01.csv
FALL remains disabled until a validated model is available.
"""
from __future__ import annotations
import argparse
import csv
import json
import math
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from event_engine.risk_policy import risk_for_event

PRESENCE_SECONDS = 10
ABSENCE_SECONDS = 10
BED_SECONDS = 10
MOVE_SECONDS = 3
MOVE_SPEED = 0.10
MOVE_DISPLACEMENT = 0.30
STILL_SPEED = 0.10
INACTIVITY_SECONDS = 30 * 60
SLEEP_SECONDS = 20 * 60
SLEEP_SPEED = 0.02
SLEEP_DISTANCE = 0.15
GAP_TOLERANCE_SECONDS = 0.25
MAX_OCCUPANCY_GAP_BRIDGE_SECONDS = 5.0

def _val(value: str | None) -> float | None:
    if value in (None, "", "\\N", "NULL", "None"):
        return None
    try:
        value = float(value)
    except ValueError:
        return None
    return value if math.isfinite(value) else None

def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.strip())

@dataclass
class Sample:
    start: datetime
    end: datetime
    subject_id: int
    sensor_id: int | None
    presence: bool
    zone: str | None
    x: float | None
    y: float | None
    speed: float | None
    raw_id: int | None = None

@dataclass
class Event:
    event_type: str
    subject_id: int
    sensor_id: int | None
    start_time: datetime
    detect_at: datetime
    zone: str | None
    end_time: datetime | None = None
    close_reason: str | None = None
    bridged_gaps: int = 0

    def as_json(self) -> dict:
        return {
            "event_type": self.event_type,
            "risk_level": risk_for_event(self.event_type),
            "subject_id": self.subject_id, "sensor_id": self.sensor_id,
            "start_time": self.start_time.isoformat(sep=" "),
            "detect_at": self.detect_at.isoformat(sep=" "),
            "end_time": self.end_time.isoformat(sep=" ") if self.end_time else None,
            "duration_sec": (
                max(0, int((self.end_time - self.start_time).total_seconds()))
                if self.end_time else None
            ),
            "zone": self.zone, "confidence": None,
            "source": "RULE_REPLAY_DRY_RUN",
            "bridged_gaps": self.bridged_gaps,
            "close_reason": self.close_reason,
        }

@dataclass
class TrackState:
    previous: Sample | None = None
    occupancy_condition: bool | None = None
    occupancy_streak_start: datetime | None = None
    occupancy_streak_seconds: float = 0
    occupancy_confirmed: str | None = None
    open_events: dict[str, Event] = field(default_factory=dict)
    movement_window: deque[Sample] = field(default_factory=lambda: deque(maxlen=3))
    bed_start: datetime | None = None
    sleep_start: datetime | None = None
    sleep_anchor: tuple[float, float] | None = None
    inactivity_start: datetime | None = None
    bed_confirmed: bool = False
    skipped_gaps: int = 0

class ReplayEngine:
    """Conservative candidate detector, not a validated medical event engine."""

    def __init__(self):
        self.tracks: dict[tuple[int, int | None], TrackState] = {}
        self.events: list[Event] = []

    def _start(self, state: TrackState, kind: str, sample: Sample, started: datetime):
        if kind not in state.open_events:
            event = Event(kind, sample.subject_id, sample.sensor_id,
                          started, sample.end, sample.zone)
            self.events.append(event)
            state.open_events[kind] = event

    def _close(self, state: TrackState, kind: str, at: datetime, reason: str):
        event = state.open_events.pop(kind, None)
        if event is not None:
            event.end_time = max(at, event.start_time)
            event.close_reason = reason

    def _reset_on_gap(self, state: TrackState, next_sample: Sample, gap: float):
        """Treat unobserved time as UNKNOWN, never as absence or inactivity.

        A confirmed occupancy episode may be bridged across <=5s of missing
        measurements only when both surrounding observed samples agree. No
        unobserved second contributes to confirmation or stillness timers.
        """
        previous = state.previous
        state.skipped_gaps += 1
        old_kind = state.occupancy_confirmed
        new_kind = "PRESENCE" if next_sample.presence else "ABSENCE"
        bridge = (
            previous is not None
            and 0 < gap <= MAX_OCCUPANCY_GAP_BRIDGE_SECONDS
            and previous.presence == next_sample.presence
            and old_kind == new_kind
            and old_kind in state.open_events
        )
        if previous is not None:
            for kind in list(state.open_events):
                if bridge and kind == new_kind:
                    state.open_events[kind].bridged_gaps += 1
                else:
                    self._close(state, kind, previous.end, "sensor_data_gap")

        # Short gap preserves only a CONFIRMED occupancy episode, nothing else.
        state.occupancy_confirmed = new_kind if bridge else None
        state.occupancy_condition = new_kind == "PRESENCE" if bridge else None
        state.occupancy_streak_start = next_sample.start if bridge else None
        state.occupancy_streak_seconds = (
            PRESENCE_SECONDS if bridge and next_sample.presence else
            ABSENCE_SECONDS if bridge else 0
        )
        state.movement_window.clear()
        state.bed_start = None
        state.bed_confirmed = False
        state.sleep_start = None
        state.sleep_anchor = None
        state.inactivity_start = None
        state.previous = None

    def add(self, sample: Sample):
        key = (sample.subject_id, sample.sensor_id)
        state = self.tracks.setdefault(key, TrackState())
        previous = state.previous
        if sample.end <= sample.start:
            return
        if previous is not None:
            gap = (sample.start - previous.end).total_seconds()
            if abs(gap) > GAP_TOLERANCE_SECONDS:
                self._reset_on_gap(state, sample, gap)

        elapsed = (sample.end - sample.start).total_seconds()
        if sample.presence != state.occupancy_condition:
            state.occupancy_condition = sample.presence
            state.occupancy_streak_start = sample.start
            state.occupancy_streak_seconds = 0
        state.occupancy_streak_seconds += elapsed
        kind = "PRESENCE" if sample.presence else "ABSENCE"
        threshold = PRESENCE_SECONDS if sample.presence else ABSENCE_SECONDS
        if state.occupancy_streak_seconds >= threshold and state.occupancy_confirmed != kind:
            old = state.occupancy_confirmed
            if old:
                self._close(state, old, state.occupancy_streak_start, "occupancy_transition")
            state.occupancy_confirmed = kind
            self._start(state, kind, sample, state.occupancy_streak_start)

        # Missing speed / position is unknown, NEVER evidence of no movement.
        if not sample.presence:
            for old_kind in ("MOVEMENT", "BED_STAY", "SLEEP", "INACTIVITY"):
                self._close(state, old_kind, sample.start, "presence_lost")
            state.movement_window.clear()
            state.bed_start = None
            state.bed_confirmed = False
            state.sleep_start = None
            state.sleep_anchor = None
            state.inactivity_start = None
            state.previous = sample
            return

        # 3 consecutive measured seconds, with both speed and XY movement.
        state.movement_window.append(sample)
        moving = False
        if len(state.movement_window) == MOVE_SECONDS:
            seq = list(state.movement_window)
            if all(s.speed is not None and s.speed >= MOVE_SPEED and
                   s.x is not None and s.y is not None for s in seq):
                displacement = math.hypot(seq[-1].x - seq[0].x,
                                          seq[-1].y - seq[0].y)
                moving = displacement >= MOVE_DISPLACEMENT
        if moving:
            self._start(state, "MOVEMENT", sample, state.movement_window[0].start)
        else:
            self._close(state, "MOVEMENT", sample.start, "movement_window_not_met")

        # Bed can only be recognized if the real zone is BED.
        in_bed = sample.zone == "BED"
        if in_bed:
            if state.bed_start is None:
                state.bed_start = sample.start
            if (sample.end - state.bed_start).total_seconds() >= BED_SECONDS:
                state.bed_confirmed = True
                self._start(state, "BED_STAY", sample, state.bed_start)
        else:
            if state.bed_confirmed and (sample.start.hour >= 22 or sample.start.hour < 6):
                # Bed departure is a point-in-time event; not proof of a fall.
                self.events.append(Event("NIGHT_EXIT", sample.subject_id,
                                         sample.sensor_id, sample.start,
                                         sample.end, sample.zone,
                                         end_time=sample.start,
                                         close_reason="bed_departure_at_night"))
            self._close(state, "BED_STAY", sample.start, "left_bed")
            state.bed_start = None
            state.bed_confirmed = False

        quiet_in_bed = (state.bed_confirmed and sample.speed is not None
                        and sample.speed <= SLEEP_SPEED and
                        sample.x is not None and sample.y is not None)
        if quiet_in_bed:
            if state.sleep_anchor is None or state.sleep_start is None:
                state.sleep_anchor = (sample.x, sample.y)
                state.sleep_start = sample.start
            elif math.hypot(sample.x - state.sleep_anchor[0],
                            sample.y - state.sleep_anchor[1]) > SLEEP_DISTANCE:
                state.sleep_anchor = (sample.x, sample.y)
                state.sleep_start = sample.start
            if (sample.end - state.sleep_start).total_seconds() >= SLEEP_SECONDS:
                self._start(state, "SLEEP", sample, state.sleep_start)
        else:
            state.sleep_start = None
            state.sleep_anchor = None
            self._close(state, "SLEEP", sample.start, "sleep_conditions_ended")

        # Provisional: suppress INACTIVITY in verified BED_STAY to avoid sleep alarms.
        inactive = (not state.bed_confirmed and sample.speed is not None
                    and sample.speed <= STILL_SPEED)
        if inactive:
            if state.inactivity_start is None:
                state.inactivity_start = sample.start
            if (sample.end - state.inactivity_start).total_seconds() >= INACTIVITY_SECONDS:
                self._start(state, "INACTIVITY", sample, state.inactivity_start)
        else:
            state.inactivity_start = None
            self._close(state, "INACTIVITY", sample.start, "activity_or_bed_state")
        # FALL intentionally disabled until independently validated.
        state.previous = sample

    def finish(self) -> list[dict]:
        # End of CSV does not mean the person physically ended the activity.
        return [event.as_json() for event in self.events]

def load_csv(path: str | Path) -> list[Sample]:
    samples: list[Sample] = []
    with open(path, newline="", encoding="utf-8-sig") as file:
        for row in csv.DictReader(file):
            if not row.get("subject_id") or row["subject_id"] == "\\N":
                raise ValueError("CSV requires subject_id to match life_event")
            sensor = row.get("sensor_id")
            samples.append(Sample(
                start=_time(row["window_start"]), end=_time(row["window_end"]),
                subject_id=int(row["subject_id"]),
                sensor_id=int(sensor) if sensor not in (None, "", "\\N") else None,
                presence=bool(int(row["presence"])),
                zone=row.get("zone") or None,
                x=_val(row.get("avg_x")), y=_val(row.get("avg_y")),
                speed=_val(row.get("avg_speed")),
                raw_id=int(row["raw_id"]) if row.get("raw_id") else None,
            ))
    samples.sort(key=lambda s: (
        s.subject_id, -1 if s.sensor_id is None else s.sensor_id, s.start, s.end
    ))
    return samples

def main():
    parser = argparse.ArgumentParser(description="CareWave CSV rule replay (read-only)")
    parser.add_argument("csv_file", help="sensor_raw CSV containing subject_id, sensor_id")
    parser.add_argument("--out", default="carewave_event_candidates_dryrun.json")
    args = parser.parse_args()
    engine = ReplayEngine()
    samples = load_csv(args.csv_file)
    for sample in samples:
        engine.add(sample)
    events = engine.finish()
    Path(args.out).write_text(
        json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"input_rows={len(samples)} events={len(events)} "
          f"gaps={sum(s.skipped_gaps for s in engine.tracks.values())}")
    print("event_counts:", dict(Counter(e["event_type"] for e in events)))
    print("FALL detection: DISABLED (no validated model)")
    print("This is a DRY RUN. No life_event rows were written.")
    print(f"output={args.out}")

if __name__ == "__main__":
    main()
