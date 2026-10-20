import unittest
from datetime import datetime
from sqlalchemy import create_engine, text
from scripts.check_life_event_write_conflicts import check


class WriterPreflightTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE life_event(event_id INT, subject_id INT, "
                "sensor_id INT, event_type TEXT, start_time TEXT, "
                "end_time TEXT, risk_level TEXT)"
            ))
            conn.execute(text("""
                INSERT INTO life_event
                VALUES
                (88,1,1,'PRESENCE','2026-09-23 21:15:40',NULL,NULL),
                (432,1,1,'PRESENCE','2026-09-30 09:17:01',NULL,NULL),
                (445,1,1,'MOVEMENT','2026-10-20 13:02:30',
                 '2026-10-20 13:02:45','정상'),
                (446,2,1,'MOVEMENT','2026-10-20 13:03:00',NULL,NULL)
            """))

    def tearDown(self):
        self.engine.dispose()

    def test_history_not_current(self):
        current, stale = check(
            self.engine, subject_id=1, sensor_id=1,
            since=datetime(2026,10,20,13), until=datetime(2026,10,20,14))
        self.assertEqual([x["event_id"] for x in current], [445])
        self.assertEqual([x["event_id"] for x in stale], [432, 88])
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(
                text("SELECT COUNT(*) FROM life_event")).scalar_one(),4)

    def test_invalid_dates(self):
        with self.assertRaises(ValueError):
            check(self.engine,subject_id=1,sensor_id=1,
                  since=datetime(2026,10,21),until=datetime(2026,10,20))

if __name__ == "__main__":
    unittest.main()
