import unittest
from datetime import datetime, timedelta
from scripts.diagnose_sensor_timeline import analyze_timeline


class SensorTimelineDiagnosticTests(unittest.TestCase):
    def test_rebase_pattern(self):
        # Consecutive inserts; artificially skipped 4 seconds of window labels.
        a = datetime(2026, 10, 20, 13, 0, 22)
        rows = [
            {"raw_id":193230, "window_start":a, "window_end":a+timedelta(seconds=1),
             "created_at":a+timedelta(seconds=4), "sample_count":21},
            {"raw_id":193231, "window_start":a+timedelta(seconds=5),
             "window_end":a+timedelta(seconds=6),
             "created_at":a+timedelta(seconds=6), "sample_count":21},
        ]
        stats = analyze_timeline(rows)
        self.assertEqual(stats["count"], 2)
        self.assertEqual(len(stats["gaps"]), 1)
        self.assertEqual(stats["gaps"][0]["empty_seconds"], 4)
        self.assertEqual(stats["suspected_rebases"], 1)

    def test_real_insert_delay_does_not_claim_rebase(self):
        a = datetime(2026, 10, 20, 13)
        rows = [
            {"raw_id":1,"window_start":a,"window_end":a+timedelta(seconds=1),
             "created_at":a+timedelta(seconds=1),"sample_count":20},
            {"raw_id":2,"window_start":a+timedelta(seconds=5),
             "window_end":a+timedelta(seconds=6),
             "created_at":a+timedelta(seconds=6),"sample_count":20},
        ]
        stats = analyze_timeline(rows)
        self.assertEqual(stats["suspected_rebases"], 0)

    def test_normal_continuity(self):
        a = datetime(2026, 10, 20, 13)
        rows = [
            {"raw_id":i+1,"window_start":a+timedelta(seconds=i),
             "window_end":a+timedelta(seconds=i+1),
             "created_at":a+timedelta(seconds=i+2),"sample_count":22}
            for i in range(5)
        ]
        stats = analyze_timeline(rows)
        self.assertEqual(stats["gaps"], [])

if __name__ == "__main__":
    unittest.main()
