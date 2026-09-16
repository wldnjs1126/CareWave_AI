import os
from datetime import datetime

import requests
from dotenv import load_dotenv


load_dotenv()


class APIClient:

    def __init__(self):

        self.base_url = os.getenv(
            "API_BASE_URL",
            "http://127.0.0.1:8000"
        ).rstrip("/")

        self.api_key = os.getenv(
            "CAREWAVE_API_KEY"
        )

        if not self.api_key:
            raise RuntimeError(
                "CAREWAVE_API_KEY가 .env에 없습니다."
            )

        self.session = requests.Session()

        self.session.headers.update(
            {
                "X-API-Key":
                    self.api_key
            }
        )

        self.timeout = 5


    @staticmethod
    def _none_if_empty(value):

        if value == "":
            return None

        return value


    def send_frames(self, frames):

        if not frames:
            return True

        send_rows = []

        for frame in frames:

            send_rows.append(
                {
                    "sensor_id":
                        int(frame["sensor_id"]),

                    "subject_id":
                        self._none_if_empty(
                            frame.get("subject_id")
                        ),

                    "x":
                        self._none_if_empty(
                            frame.get("x")
                        ),

                    "y":
                        self._none_if_empty(
                            frame.get("y")
                        ),

                    "z":
                        self._none_if_empty(
                            frame.get("z")
                        ),

                    "velocity_x":
                        self._none_if_empty(
                            frame.get("velocity_x")
                        ),

                    "velocity_y":
                        self._none_if_empty(
                            frame.get("velocity_y")
                        ),

                    "velocity_z":
                        self._none_if_empty(
                            frame.get("velocity_z")
                        ),

                    "acceleration_x":
                        self._none_if_empty(
                            frame.get("acceleration_x")
                        ),

                    "acceleration_y":
                        self._none_if_empty(
                            frame.get("acceleration_y")
                        ),

                    "acceleration_z":
                        self._none_if_empty(
                            frame.get("acceleration_z")
                        ),

                    "doppler":
                        self._none_if_empty(
                            frame.get("doppler")
                        ),

                    "point_count":
                        self._none_if_empty(
                            frame.get("point_count")
                        ),

                    "snr":
                        self._none_if_empty(
                            frame.get("snr")
                        ),

                    "presence":
                        bool(frame["presence"]),

                    "timestamp":
                        frame["timestamp"],
                }
            )

        response = self.session.post(
            f"{self.base_url}/api/radar-frames",
            json={
                "frames": send_rows
            },
            timeout=self.timeout
        )

        response.raise_for_status()

        return True


    def send_event(
        self,
        *,
        sensor_id,
        subject_id,
        event_type,
        event_detail,
        started_at,
        ended_at
    ):

        if isinstance(started_at, datetime):

            started_at = (
                started_at.isoformat(
                    timespec="milliseconds"
                )
            )

        if isinstance(ended_at, datetime):

            ended_at = (
                ended_at.isoformat(
                    timespec="milliseconds"
                )
            )

        response = self.session.post(
            f"{self.base_url}/api/events",
            json={
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
            },
            timeout=self.timeout
        )

        response.raise_for_status()

        return True
