import unittest
from datetime import datetime, timedelta
from event_engine.rule_replay import ReplayEngine, Sample

class TestRuleReplay(unittest.TestCase):
    def feed(self, engine, start, seconds, *, presence=True, zone="ROOM",
             speed=0.01, x=0.0, y=0.0):
        for i in range(seconds):
            t = start + timedelta(seconds=i)
            engine.add(Sample(t, t + timedelta(seconds=1), 1, 1,
                              presence, zone, x, y, speed))

    def test_presence_absence(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 12)
        self.feed(engine, t + timedelta(seconds=12), 11,
                  presence=False, speed=None)
        events = engine.finish()
        self.assertEqual([e["event_type"] for e in events],
                         ["PRESENCE", "ABSENCE"])

    def test_gap_not_absence(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 10)
        self.feed(engine, t + timedelta(seconds=20), 10)
        self.assertNotIn("ABSENCE", [e["event_type"] for e in engine.finish()])
        self.assertEqual(sum(s.skipped_gaps for s in engine.tracks.values()), 1)

    def test_movement_requires_displacement(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        for i in range(6):
            start = t + timedelta(seconds=i)
            engine.add(Sample(start, start + timedelta(seconds=1), 1, 1,
                              True, "ROOM", i * 0.2, 1.0, 0.2))
        self.assertIn("MOVEMENT", [e["event_type"] for e in engine.finish()])
        still = ReplayEngine()
        self.feed(still, t, 8, speed=0.2)
        self.assertNotIn("MOVEMENT", [e["event_type"] for e in still.finish()])

    def test_bed_sleep(self):
        engine = ReplayEngine()
        self.feed(engine, datetime(2026, 10, 20, 12), 1215,
                  zone="BED", speed=0.01, x=0.5, y=1.7)
        names = [e["event_type"] for e in engine.finish()]
        self.assertIn("BED_STAY", names)
        self.assertIn("SLEEP", names)
        self.assertNotIn("FALL", names)

    def test_inactivity(self):
        engine = ReplayEngine()
        self.feed(engine, datetime(2026, 10, 20, 12), 1805)
        self.assertIn("INACTIVITY", [e["event_type"] for e in engine.finish()])

    def test_night_exit(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 23)
        self.feed(engine, t, 12, zone="BED", x=0.5, y=1.7)
        self.feed(engine, t + timedelta(seconds=12), 1,
                  zone="ROOM", speed=0.15, x=1.5, y=2.5)
        self.assertIn("NIGHT_EXIT", [e["event_type"] for e in engine.finish()])

    def test_short_gap_bridges_confirmed_presence(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 12)
        self.feed(engine, t + timedelta(seconds=16), 11)
        events = engine.finish()
        self.assertEqual([event["event_type"] for event in events], ["PRESENCE"])
        self.assertEqual(events[0]["bridged_gaps"], 1)

    def test_short_gap_does_not_invent_absence(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 12)
        self.feed(engine, t + timedelta(seconds=16), 11, presence=False, speed=None)
        events = engine.finish()
        self.assertEqual([event["event_type"] for event in events],
                         ["PRESENCE", "ABSENCE"])
        self.assertEqual(events[0]["end_time"],
                         (t + timedelta(seconds=12)).isoformat(sep=" "))

    def test_long_gap_breaks_confirmed_presence(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 12)
        self.feed(engine, t + timedelta(seconds=20), 11)
        self.assertEqual(
            [event["event_type"] for event in engine.finish()],
            ["PRESENCE", "PRESENCE"]
        )

    def test_pending_presence_cannot_count_missing_seconds(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        self.feed(engine, t, 6)
        self.feed(engine, t + timedelta(seconds=10), 5)
        self.assertEqual(engine.finish(), [])

    def test_movement_short_invalid_window_is_not_immediate_stop(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        for i in range(6):
            now = t + timedelta(seconds=i)
            engine.add(Sample(now, now + timedelta(seconds=1), 1, 1,
                              True, "ROOM", i * 0.2, 1.0, 0.2))
        for i in range(2):
            now = t + timedelta(seconds=6 + i)
            engine.add(Sample(now, now + timedelta(seconds=1), 1, 1,
                              True, "ROOM", 1.0, 1.0, 0.02))
        events = [e for e in engine.finish() if e["event_type"] == "MOVEMENT"]
        self.assertEqual(len(events), 1)
        self.assertIsNone(events[0]["end_time"])

    def test_movement_stops_after_five_observed_quiet_seconds(self):
        engine = ReplayEngine()
        t = datetime(2026, 10, 20, 12)
        for i in range(6):
            now = t + timedelta(seconds=i)
            engine.add(Sample(now, now + timedelta(seconds=1), 1, 1,
                              True, "ROOM", i * 0.2, 1.0, 0.2))
        for i in range(7):
            now = t + timedelta(seconds=6 + i)
            engine.add(Sample(now, now + timedelta(seconds=1), 1, 1,
                              True, "ROOM", 1.0, 1.0, 0.02))
        movement = [e for e in engine.finish() if e["event_type"] == "MOVEMENT"]
        self.assertEqual(len(movement), 1)
        self.assertIsNotNone(movement[0]["end_time"])

if __name__ == "__main__":
    unittest.main()
