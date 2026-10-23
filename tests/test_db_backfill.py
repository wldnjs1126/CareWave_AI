import unittest
from datetime import datetime
from sqlalchemy import create_engine, text
from event_engine.db_backfill import review_candidates, save_completed

SINCE = datetime(2026, 10, 20, 13)
UNTIL = datetime(2026, 10, 20, 14)

def ev(kind="MOVEMENT", end="2026-10-20 13:00:10",
       reason="movement_stopped_after_observed_quiet"):
    return {
        "event_type": kind,
        "risk_level": "정상",
        "subject_id": 1, "sensor_id": 1, "zone": "ROOM",
        "start_time": "2026-10-20 13:00:03",
        "detect_at": "2026-10-20 13:00:05",
        "end_time": end, "confidence": None,
        "close_reason": reason,
        "source": "RULE_REPLAY_DRY_RUN",
    }

class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.db = create_engine("sqlite+pysqlite:///:memory:")
        with self.db.begin() as conn:
            conn.execute(text("""
                CREATE TABLE life_event (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subject_id INTEGER NOT NULL, sensor_id INTEGER,
                    event_type TEXT, start_time DATETIME, detect_at DATETIME,
                    end_time DATETIME, duration_sec INTEGER, zone TEXT,
                    confidence REAL, risk_level TEXT, evidence TEXT
                )
            """))
            conn.execute(text("""
                INSERT INTO life_event
                (event_id,subject_id,sensor_id,event_type,
                 start_time,detect_at,end_time,risk_level)
                VALUES
                (88,1,1,'PRESENCE','2026-09-23 21:15:40',
                 '2026-09-23 21:15:50',NULL,NULL),
                (432,1,1,'PRESENCE','2026-09-30 09:17:01',
                 '2026-09-30 09:17:11',NULL,NULL)
            """))

    def tearDown(self):
        self.db.dispose()

    def test_does_not_write_old_events_and_is_idempotent(self):
        valid, skipped = review_candidates(
            [ev()], selected_types={"MOVEMENT"}, since=SINCE, until=UNTIL)
        self.assertEqual(len(valid), 1)
        self.assertEqual(dict(skipped), {})
        self.assertEqual(save_completed(self.db, valid)["inserted"], 1)
        self.assertEqual(save_completed(self.db, valid)["existing"], 1)
        with self.db.connect() as conn:
            rows = conn.execute(text(
                "SELECT event_id, event_type, end_time FROM life_event "
                "ORDER BY event_id"
            )).all()
        self.assertEqual(len(rows), 3)
        self.assertIsNone(rows[0].end_time)
        self.assertIsNone(rows[1].end_time)

    def test_skips_open_and_unknown_ends(self):
        records = [
            ev(end=None),
            ev(reason="sensor_data_gap"),
            ev(reason="unknown_movement_data"),
        ]
        eligible, skipped = review_candidates(
            records, selected_types={"MOVEMENT"},
            since=SINCE, until=UNTIL)
        self.assertEqual(len(eligible), 0)
        self.assertEqual(skipped["still_open_at_end_of_observation"], 1)
        self.assertEqual(skipped["ended_by_missing_data"], 2)

    def test_rejects_fake_confidence_or_risk(self):
        bad = ev()
        bad["confidence"] = 0.99
        with self.assertRaises(ValueError):
            review_candidates([bad], selected_types={"MOVEMENT"},
                              since=SINCE, until=UNTIL)
        bad = ev()
        bad["risk_level"] = "위험"
        with self.assertRaises(ValueError):
            review_candidates([bad], selected_types={"MOVEMENT"},
                              since=SINCE, until=UNTIL)

    def test_refuses_unvalidated_fall(self):
        with self.assertRaises(ValueError):
            review_candidates([], selected_types={"FALL"},
                              since=SINCE, until=UNTIL)

    def test_transaction_rolls_back_on_conflict(self):
        valid, _ = review_candidates(
            [ev()], selected_types={"MOVEMENT"}, since=SINCE, until=UNTIL)
        self.assertEqual(save_completed(self.db, valid)["inserted"], 1)
        changed = dict(valid[0])
        changed["end_time"] = datetime(2026, 10, 20, 13, 0, 20)
        with self.assertRaises(RuntimeError):
            save_completed(self.db, [changed])
        with self.db.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM life_event")).scalar_one()
        self.assertEqual(count, 3)


if __name__ == "__main__":
    unittest.main()
