"""Read-only life_event preflight; does not detect other writing processes."""
from datetime import datetime
import argparse
from sqlalchemy import text

CURRENT_EVENTS = text("""
SELECT event_id, event_type, start_time, end_time, risk_level
FROM life_event WHERE subject_id = :subject_id
AND sensor_id = :sensor_id
AND start_time >= :since AND start_time < :until
ORDER BY start_time, event_id LIMIT 101
""")
STALE_EVENTS = text("""
SELECT event_id, event_type, start_time FROM life_event
WHERE subject_id = :subject_id AND sensor_id = :sensor_id
AND start_time < :since AND end_time IS NULL
ORDER BY start_time DESC LIMIT 101
""")


def check(db_engine, *, subject_id, sensor_id, since, until):
    if subject_id <= 0 or sensor_id <= 0:
        raise ValueError("Positive subject_id and sensor_id required")
    if since.tzinfo is not None or until.tzinfo is not None or since >= until:
        raise ValueError("Invalid KST naive datetime range")
    params = dict(subject_id=subject_id, sensor_id=sensor_id,
                  since=since, until=until)
    with db_engine.connect() as conn:
        current = conn.execute(CURRENT_EVENTS, params).mappings().all()
        stale = conn.execute(STALE_EVENTS, params).mappings().all()
    return current, stale


def main():
    p = argparse.ArgumentParser(description="Read-only life_event preflight")
    p.add_argument("--subject-id", type=int, required=True)
    p.add_argument("--sensor-id", type=int, required=True)
    p.add_argument("--since", required=True)
    p.add_argument("--until", required=True)
    args = p.parse_args()
    from database.connection import engine
    current, stale = check(
        engine, subject_id=args.subject_id, sensor_id=args.sensor_id,
        since=datetime.fromisoformat(args.since),
        until=datetime.fromisoformat(args.until))
    print("READ ONLY: no database changes.")
    print("event_records_in_session=", len(current))
    print("historical_open_records_before_session=", len(stale))
    for r in current[:15]:
        print("session:", r["event_id"], r["event_type"], r["start_time"],
              r["end_time"], r["risk_level"])
    if len(current) > 100 or len(stale) > 100:
        print("WARNING: list truncated at 101 rows.")
    print("Historical open demo rows are untouched and excluded from the session.")
    print("This does NOT prove other event writers are stopped.")


if __name__ == "__main__":
    main()
