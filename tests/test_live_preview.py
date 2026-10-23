"""Local in-memory regression tests for the read-only real-time watcher."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from event_engine.live_preview import (
    LivePreview, fetch_new_rows, latest_raw_id, to_sample, watch
)
from event_engine.rule_replay import Sample


class LivePreviewTests(unittest.TestCase):
    def setUp(self):
        self.db = create_engine("sqlite+pysqlite:///:memory:")
        with self.db.begin() as conn:
            conn.execute(text("""
                CREATE TABLE sensor_raw (
                    raw_id INTEGER PRIMARY KEY,
                    subject_id INTEGER NOT NULL, sensor_id INTEGER NOT NULL,
                    window_start TEXT, window_end TEXT,
                    sample_count INTEGER, presence INTEGER, zone TEXT,
                    avg_x REAL, avg_y REAL, avg_speed REAL
                )
            """))

    def tearDown(self):
        self.db.dispose()

    def add_rows(self, *, count=12):
        with self.db.begin() as conn:
            for i in range(count):
                start = datetime(2026, 10, 20, 13) + timedelta(seconds=i)
                conn.execute(text("""
                    INSERT INTO sensor_raw
                    (raw_id, subject_id, sensor_id, window_start, window_end,
                     sample_count, presence, zone, avg_x, avg_y, avg_speed)
                    VALUES (:id, 1, 1, :start, :end, 22, 1, 'ROOM', 1.0, 2.0, 0.01)
                """), {
                    "id": i + 1, "start": str(start),
                    "end": str(start + timedelta(seconds=1)),
                })

    def sample(self, second, *, raw_id=None):
        start = datetime(2026, 10, 20, 13) + timedelta(seconds=second)
        return Sample(start, start + timedelta(seconds=1), 1, 1, True,
                      "ROOM", 1.0, 2.0, 0.01,
                      raw_id=second+1 if raw_id is None else raw_id)

    def test_query_incremental_limit(self):
        self.add_rows(count=12)
        self.assertEqual(latest_raw_id(self.db, subject_id=1, sensor_id=1), 12)
        rows, more = fetch_new_rows(
            self.db, subject_id=1, sensor_id=1,
            after_raw_id=2, batch_size=3)
        self.assertEqual([r["raw_id"] for r in rows], [3, 4, 5])
        self.assertTrue(more)

    def test_closed_historical_filter(self):
        self.add_rows(count=12)
        until = datetime(2026, 10, 20, 13, 0, 3)
        rows, more = fetch_new_rows(
            self.db, subject_id=1, sensor_id=1,
            after_raw_id=0, batch_size=20, until=until)
        self.assertEqual([r["raw_id"] for r in rows], [1, 2, 3])
        self.assertFalse(more)
        self.assertTrue(to_sample(rows[0]).presence)

    def test_presence_start_is_not_repeated(self):
        observer = LivePreview()
        records = []
        for i in range(12):
            records.extend(observer.process(self.sample(i)))
        started = [r for r in records if r.get("phase") == "STARTED"]
        self.assertEqual(len(started), 1)
        self.assertEqual(started[0]["event"]["event_type"], "PRESENCE")
        self.assertFalse(started[0]["persisted_to_db"])

    def test_short_gap_keeps_presence_and_marks_quality(self):
        observer = LivePreview()
        records = []
        for i in range(12):
            records.extend(observer.process(self.sample(i)))
        records.extend(observer.process(self.sample(16)))
        self.assertEqual(
            [r["phase"] for r in records if r["record_type"] == "EVENT_TRANSITION"],
            ["STARTED"],
        )
        self.assertEqual(
            sum(r["record_type"] == "DATA_QUALITY" for r in records), 1
        )

    def test_invalid_row_interrupts_open_event(self):
        observer = LivePreview()
        for i in range(12):
            observer.process(self.sample(i))
        records = observer.invalid_row(13, ValueError("sample_count is 0"))
        self.assertIn("INTERRUPTED", [r.get("phase") for r in records])
        self.assertIsNone(observer.last_sample)

    def test_duplicate_window_is_not_used_to_change_event(self):
        observer = LivePreview()
        for i in range(12):
            observer.process(self.sample(i))
        rows = observer.process(self.sample(11, raw_id=99))
        self.assertEqual([r["record_type"] for r in rows], ["DATA_QUALITY"])
        self.assertEqual(observer.last_sample.raw_id, 12)

    def test_one_poll_only_writes_local_jsonl(self):
        self.add_rows(count=12)
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / "live.jsonl"
            result = watch(
                self.db, subject_id=1, sensor_id=1, after_raw_id=0,
                batch_size=50, poll_seconds=1, max_polls=1, output=dest
            )
            self.assertEqual(result["rows"], 12)
            records = [json.loads(line) for line in
                       dest.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                [r["phase"] for r in records if r["record_type"] == "EVENT_TRANSITION"],
                ["STARTED"]
            )
            with self.db.connect() as conn:
                self.assertEqual(
                    conn.execute(text("SELECT COUNT(*) FROM sensor_raw")).scalar_one(),
                    12
                )


if __name__ == "__main__":
    unittest.main()
