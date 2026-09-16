from sqlalchemy import text

from database.connection import engine


CREATE_RADAR_FRAME = """
CREATE TABLE IF NOT EXISTS radar_frame (
    frame_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    sensor_id BIGINT NOT NULL,
    subject_id BIGINT NULL,

    x FLOAT NULL,
    y FLOAT NULL,
    z FLOAT NULL,

    velocity_x FLOAT NULL,
    velocity_y FLOAT NULL,
    velocity_z FLOAT NULL,

    acceleration_x FLOAT NULL,
    acceleration_y FLOAT NULL,
    acceleration_z FLOAT NULL,

    doppler FLOAT NULL,
    point_count INT NULL,
    snr FLOAT NULL,

    presence BOOLEAN NOT NULL,
    timestamp DATETIME(3) NOT NULL,

    INDEX idx_radar_timestamp (timestamp),
    INDEX idx_radar_sensor_timestamp (sensor_id, timestamp)
);
"""


CREATE_EVENT_LOG = """
CREATE TABLE IF NOT EXISTS simulation_event_log (
    event_log_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    sensor_id BIGINT NOT NULL,
    subject_id BIGINT NULL,

    event_type VARCHAR(30) NOT NULL,
    event_detail VARCHAR(100) NULL,

    started_at DATETIME(3) NOT NULL,
    ended_at DATETIME(3) NULL,

    duration_sec FLOAT NULL,

    created_at DATETIME(3)
        NOT NULL
        DEFAULT CURRENT_TIMESTAMP(3),

    INDEX idx_event_type (event_type),
    INDEX idx_event_started_at (started_at),
    INDEX idx_event_subject_started (subject_id, started_at)
);
"""


def main():

    with engine.begin() as conn:

        conn.execute(
            text(CREATE_RADAR_FRAME)
        )

        conn.execute(
            text(CREATE_EVENT_LOG)
        )

    print("DB 테이블 생성/확인 완료")
    print("- radar_frame")
    print("- simulation_event_log")


if __name__ == "__main__":
    main()
