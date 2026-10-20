import unittest

from event_engine.risk_policy import EVENT_RISK_LEVELS, risk_for_event


class TestRiskPolicy(unittest.TestCase):
    def test_all_eight_event_types(self):
        self.assertEqual(len(EVENT_RISK_LEVELS), 8)
        self.assertEqual(
            {event for event, risk in EVENT_RISK_LEVELS.items() if risk == "위험"},
            {"FALL"},
        )

    def test_four_levels(self):
        self.assertEqual(set(EVENT_RISK_LEVELS.values()), {"정상", "관찰", "주의", "위험"})

    def test_unknown_event_fails_closed(self):
        with self.assertRaises(ValueError):
            risk_for_event("UNKNOWN_EVENT")

    def test_mapping(self):
        for event_type, risk_level in EVENT_RISK_LEVELS.items():
            with self.subTest(event_type=event_type):
                self.assertEqual(risk_for_event(event_type), risk_level)


if __name__ == "__main__":
    unittest.main()
