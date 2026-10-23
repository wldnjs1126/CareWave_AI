"""Read-only verification of the NCP MySQL life_event schema and existing events.

Run from the CareWave_AI project root:
    python -m scripts.check_life_event_ready
"""

from sqlalchemy import text

from database.connection import engine
from event_engine.risk_policy import EVENT_RISK_LEVELS


def main() -> None:
    with engine.connect() as connection:
        columns = {
            row[0] for row in connection.execute(text("SHOW COLUMNS FROM life_event"))
        }
        required = {
            "event_id", "subject_id", "sensor_id", "event_type",
            "start_time", "detect_at", "end_time", "duration_sec",
            "zone", "confidence", "created_at", "evidence", "risk_level",
        }
        missing = sorted(required - columns)
        if missing:
            print("Missing columns:", ", ".join(missing))
            print("Check the migration SQL before using event_repository.")
            raise SystemExit(1)

        print("life_event schema: OK")
        results = connection.execute(
            text(
                """
                SELECT event_type, COUNT(*) AS event_count
                FROM life_event
                GROUP BY event_type
                ORDER BY event_type
                """
            )
        ).all()
        for event_type, count in results:
            level = EVENT_RISK_LEVELS.get(event_type, "UNKNOWN")
            print(f"{event_type:14s} {count:6d} -> {level}")

        unknown = [event for event, _ in results if event not in EVENT_RISK_LEVELS]
        if unknown:
            print("Warning: unmapped historical event types:", ", ".join(unknown))

        duplicates = connection.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT subject_id, event_type
                    FROM life_event
                    WHERE end_time IS NULL
                    GROUP BY subject_id, event_type
                    HAVING COUNT(*) > 1
                ) AS conflicting_open_events
                """
            )
        ).scalar_one()
        print("Duplicate open-event groups:", duplicates)
        if duplicates:
            print("Warning: inspect duplicate open events before enabling a writer.")

    print("Read-only check complete. No database rows changed.")


if __name__ == "__main__":
    main()
