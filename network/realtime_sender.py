import json
import os
import threading
from pathlib import Path
from queue import Empty, Full, Queue

import requests
from dotenv import load_dotenv


load_dotenv()


class RealtimeSender:
    """
    radar_frame 1개가 생성될 때마다  API로 전송.

    - 시뮬레이터 생성 Thread와 HTTP 전송 Thread를 분리
    - API 응답이 잠깐 느려도 10FPS 생성 루프를 직접 막지 않음
    - 전송 실패 데이터는 JSONL 파일에 백업
    """

    def __init__(self):

        self.api_url = os.getenv(
            "API_URL"
        )

        self.api_key = os.getenv(
            "API_KEY"
        )

        if not self.api_url:
            raise RuntimeError(
                "API_URL이 .env에 없습니다."
            )

        self.timeout = float(
            os.getenv(
                "API_TIMEOUT",
                "2"
            )
        )

        self.queue = Queue(
            maxsize=10000
        )

        self.running = True

        self.session = requests.Session()

        self.headers = {
            "Content-Type":
                "application/json"
        }

        if self.api_key:

            self.headers[
                "X-API-Key"
            ] = self.api_key

        project_root = (
            Path(__file__)
            .resolve()
            .parent
            .parent
        )

        self.failed_path = (
            project_root
            / "data"
            / "failed_radar_frames.jsonl"
        )

        self.failed_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self.sent_count = 0
        self.failed_count = 0

        self.worker = threading.Thread(
            target=self._worker_loop,
            daemon=True
        )

        self.worker.start()


    @staticmethod
    def _clean_value(value):

        if value == "":
            return None

        return value


    def _make_payload(
        self,
        frame
    ):

        # frame_id는 로컬 CSV용 PK이므로
        # 서버로 보내는 원본 센서 데이터에서는 제외.
        return {
            "sensor_id":
                int(frame["sensor_id"]),

            "subject_id":
                self._clean_value(
                    frame.get(
                        "subject_id"
                    )
                ),

            "x":
                self._clean_value(
                    frame.get("x")
                ),

            "y":
                self._clean_value(
                    frame.get("y")
                ),

            "z":
                self._clean_value(
                    frame.get("z")
                ),

            "velocity_x":
                self._clean_value(
                    frame.get(
                        "velocity_x"
                    )
                ),

            "velocity_y":
                self._clean_value(
                    frame.get(
                        "velocity_y"
                    )
                ),

            "velocity_z":
                self._clean_value(
                    frame.get(
                        "velocity_z"
                    )
                ),

            "acceleration_x":
                self._clean_value(
                    frame.get(
                        "acceleration_x"
                    )
                ),

            "acceleration_y":
                self._clean_value(
                    frame.get(
                        "acceleration_y"
                    )
                ),

            "acceleration_z":
                self._clean_value(
                    frame.get(
                        "acceleration_z"
                    )
                ),

            "doppler":
                self._clean_value(
                    frame.get(
                        "doppler"
                    )
                ),

            "point_count":
                self._clean_value(
                    frame.get(
                        "point_count"
                    )
                ),

            "snr":
                self._clean_value(
                    frame.get(
                        "snr"
                    )
                ),

            "presence":
                bool(
                    frame["presence"]
                ),

            "timestamp":
                frame["timestamp"],
        }


    def submit(
        self,
        frame
    ):

        payload = (
            self._make_payload(
                frame
            )
        )

        try:

            self.queue.put_nowait(
                payload
            )

        except Full:

            # 전송 Queue가 꽉 차도
            # 10FPS 데이터 생성 자체는 멈추지 않는다.
            self._save_failed(
                payload,
                "queue_full"
            )


    def _send(
        self,
        payload
    ):

        response = (
            self.session.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=self.timeout
            )
        )

        response.raise_for_status()

        self.sent_count += 1


    def _save_failed(
        self,
        payload,
        reason
    ):

        self.failed_count += 1

        record = {
            "reason":
                reason,

            "payload":
                payload,
        }

        with open(
            self.failed_path,
            "a",
            encoding="utf-8"
        ) as file:

            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                )
                + "\n"
            )


    def _worker_loop(self):

        while (
            self.running
            or not self.queue.empty()
        ):

            try:

                payload = (
                    self.queue.get(
                        timeout=0.2
                    )
                )

            except Empty:

                continue

            try:

                self._send(
                    payload
                )

            except Exception as e:

                self._save_failed(
                    payload,
                    str(e)
                )

                print(
                    "[API WARNING] "
                    "0.1초 frame 전송 실패:",
                    e
                )

            finally:

                self.queue.task_done()


    def close(self):

        self.running = False

        self.worker.join(
            timeout=5
        )

        self.session.close()

        print(
            f"[API] 전송 성공: "
            f"{self.sent_count}"
        )

        print(
            f"[API] 전송 실패: "
            f"{self.failed_count}"
        )

        if self.failed_count:

            print(
                "[API] 실패 데이터 백업:"
            )

            print(
                self.failed_path
            )
