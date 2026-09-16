from datetime import datetime

from sqlalchemy import text

from database.connection import engine


INSERT_RADAR_FRAME = text("""
INSERT INTO radar_frame
(
    sensor_id,
    subject_id,

    x,
    y,
    z,

    velocity_x,
    velocity_y,
    velocity_z,

    acceleration_x,
    acceleration_y,
    acceleration_z,

    doppler,
    point_count,
    snr,

    presence,
    timestamp
)
VALUES
(
    :sensor_id,
    :subject_id,

    :x,
    :y,
    :z,

    :velocity_x,
    :velocity_y,
    :velocity_z,

    :acceleration_x,
    :acceleration_y,
    :acceleration_z,

    :doppler,
    :point_count,
    :snr,

    :presence,
    :timestamp
)
""")


INSERT_EVENT = text("""
INSERT INTO simulation_event_log
(
    sensor_id,
    subject_id,

    event_type,
    event_detail,

    started_at,
    ended_at,

    duration_sec
)
VALUES
(
    :sensor_id,
    :subject_id,

    :event_type,
    :event_detail,

    :started_at,
    :ended_at,

    :duration_sec
)
""")


class DBStorage:

    def save_frames(
        self,
        frames
    ):
        """
        CSV용 frame dict 목록을 받아서
        NCP MySQL radar_frame에 batch INSERT.
        """

        if not frames:
            return

        rows = []

        for frame in frames:

            # CSV에서는 timestamp가 문자열이므로
            # MySQL 저장 시 datetime으로 변환
            timestamp = frame["timestamp"]

            if isinstance(
                timestamp,
                str
            ):
                timestamp = datetime.strptime(
                    timestamp,
                    "%Y-%m-%d %H:%M:%S.%f"
                )

            subject_id = (
                frame["subject_id"]
                if frame["subject_id"] != ""
                else None
            )

            rows.append(
                {
                    "sensor_id":
                        frame["sensor_id"],

                    "subject_id":
                        subject_id,

                    "x":
                        frame["x"],

                    "y":
                        frame["y"],

                    "z":
                        frame["z"],

                    "velocity_x":
                        frame["velocity_x"],

                    "velocity_y":
                        frame["velocity_y"],

                    "velocity_z":
                        frame["velocity_z"],

                    "acceleration_x":
                        frame["acceleration_x"],

                    "acceleration_y":
                        frame["acceleration_y"],

                    "acceleration_z":
                        frame["acceleration_z"],

                    "doppler":
                        frame["doppler"],

                    "point_count":
                        frame["point_count"],

                    "snr":
                        frame["snr"],

                    "presence":
                        bool(
                            frame["presence"]
                        ),

                    "timestamp":
                        timestamp,
                }
            )

        with engine.begin() as conn:

            conn.execute(
                INSERT_RADAR_FRAME,
                rows
            )


    def save_event(
        self,
        *,
        event_type,
        event_detail,
        started_at,
        ended_at,
        sensor_id,
        subject_id
    ):

        duration_sec = (
            ended_at
            - started_at
        ).total_seconds()

        row = {
            "sensor_id":
                sensor_id,

            "subject_id":
                subject_id,

            "event_type":
                event_type,

            "event_detail":
                event_detail,

            "started_at":
                started_at,

            "ended_at":
                ended_at,

            "duration_sec":
                round(
                    duration_sec,
                    3
                ),
        }

        with engine.begin() as conn:

            conn.execute(
                INSERT_EVENT,
                row
            )
