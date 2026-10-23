"""In-memory DB tests for the read-only sensor_raw reader."""
import unittest
from datetime import datetime

from sqlalchemy import create_engine, text

from event_engine.db_replay import load_sensor_rows, replay_samples


class DBReplayTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE sensor_raw (
                    raw_id INTEGER PRIMARY KEY,
                    subject_id INTEGER, sensor_id INTEGER,
                    window_start TEXT, window_end TEXT,
                    sample_count INTEGER, presence INTEGER,
                    zone TEXT, avg_x REAL, avg_y REAL, avg_speed REAL
                )
            """))
            conn.execute(text("""
                INSERT INTO sensor_raw
                    (raw_id, subject_id, sensor_id, window_start, window_end,
                     sample_count, presence, zone, avg_x, avg_y, avg_speed)
                VALUES
                    (12, 1, 1, '2026-10-20 13:00:01',
                     '2026-10-20 13:00:02', 20, 1, 'ROOM', 0.2, 2.1, 0.1),
                    (11, 1, 1, '2026-10-20 13:00:00',
                     '2026-10-20 13:00:01', 20, 1, 'ROOM', 0.0, 2.1, 0.1),
                    (13, 2, 1, '2026-10-20 13:00:02',
                     '2026-10-20 13:00:03', 20, 1, 'ROOM', 0.4, 2.1, 0.1),
                    (14, 1, 2, '2026-10-20 13:00:02',
                     '2026-10-20 13:00:03', 20, 1, 'ROOM', 0.4, 2.1, 0.1)
            """))

    def tearDown(self):
        self.engine.dispose()

    def _read(self, max_rows=5000):
        return load_sensor_rows(
            self.engine, subject_id=1, sensor_id=1,
            since=datetime(2026, 10, 20, 13),
            until=datetime(2026, 10, 20, 13, 1),
            max_rows=max_rows,
        )

    def test_filters_subject_and_sensor_and_sorts(self):
        rows = self._read()
        self.assertEqual([s.raw_id for s in rows], [11, 12])

    def test_replay_does_not_write(self):
        rows = self._read()
        events, gaps = replay_samples(rows)
        self.assertEqual(events, [])
        self.assertEqual(gaps, 0)
        with self.engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM sensor_raw")).scalar_one()
        self.assertEqual(total, 4)

    def test_limit_refuses_partial_result(self):
        with self.assertRaisesRegex(ValueError, "More than 1 rows"):
            self._read(max_rows=1)

    def test_invalid_time_window(self):
        with self.assertRaisesRegex(ValueError, "increasing"):
            load_sensor_rows(
                self.engine, subject_id=1, sensor_id=1,
                since=datetime(2026, 10, 20, 13, 1),
                until=datetime(2026, 10, 20, 13)
            )


if __name__ == "__main__":
    unittest.main()
