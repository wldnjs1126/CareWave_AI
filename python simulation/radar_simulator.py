import csv
import os
import math
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty

import numpy as np

import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# CSV 전용 시험: set SIMULATOR_OFFLINE=1 (CMD), 또는 $env:SIMULATOR_OFFLINE="1" (PowerShell)
# 기본값은 기존 network.realtime_sender.RealtimeSender를 그대로 사용하여 API로 전송.
if os.getenv("SIMULATOR_OFFLINE", "0") == "1":
    class RealtimeSender:
        api_url = "OFFLINE (CSV ONLY)"

        def submit(self, frame):
            pass

        def close(self):
            pass
else:
    try:
        from network.realtime_sender import RealtimeSender
    except ImportError as exc:
        raise ImportError(
            "실시간 전송용 network/realtime_sender.py가 필요합니다. "
            "CSV만 시험하려면 SIMULATOR_OFFLINE=1을 설정하세요."
        ) from exc


# ============================================================
# CareWave Random 24H / Continuous Simulator
#
# 주요 기능
# 1) 실제 컴퓨터 시간 datetime.now() 사용
# 2) 10 FPS radar_frame.csv 저장 (방 전역 좌표계, 천장 중앙 설치 가정)
# 3) 상태 기반 랜덤 행동 생성
# 4) 7종 이벤트 로그
#    - FALL
#    - OCCUPANCY (PRESENT / ABSENT)
#    - BED_STAY
#    - MOVEMENT
#    - INACTIVITY
#    - NIGHT_EXIT
#    - SLEEP
# 5) NIGHT_EXIT는 강제로 넣지 않고
#    실제 시간 + 최근 침대 이탈 + ABSENT를 이용해 자동 파생
#
# 실행 명령
#   auto 6
#   auto 12
#   auto 24
#   auto continuous
#   auto_stop
#   status
#   stop
# ============================================================


# ============================================================
# 기본 설정
# ============================================================

FPS = 10
DT = 1.0 / FPS

SENSOR_ID = 1
SUBJECT_ID = 1

ROOM_WIDTH = 4.0
ROOM_DEPTH = 5.0
ROOM_HEIGHT = 2.7

# 야간 기준
NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 6

# NIGHT_EXIT 판정을 위해
# 침대에서 나온 뒤 몇 분 이내의 부재를 야간이탈 후보로 볼지
NIGHT_EXIT_BED_WINDOW_MINUTES = 15

# 낙상은 가끔만 선택. 확률은 매 행동 선택 시의 가중치이며 시간당 확률이 아님.
FALL_WEIGHT_DAY = 0.006
FALL_WEIGHT_NIGHT = 0.002
FALL_COOLDOWN_SECONDS = 3 * 3600

# 장시간 미움직임 테스트: 현재 Event Engine의 기본 기준(30분=1800초)을 넘도록 유지.
# 임계값을 별도로 변경했다면 INACTIVITY_DURATION_MIN/MAX도 함께 변경할 것.
INACTIVITY_DURATION_MIN = 1880
INACTIVITY_DURATION_MAX = 2100

# 실제 경과시간 1초에 센서 프레임 10개. 장시간 이벤트를 몇 분 안에 강제하지 않음.
WALK_ROUNDS_MIN = 2
WALK_ROUNDS_MAX = 4

# 랜덤 seed.
# None이면 실행할 때마다 다르게 생성됨.
RANDOM_SEED = None


# ============================================================
# 병실 / 센서
# ============================================================

# 전역 좌표: 바닥 왼쪽 아래 (0,0,0), x=0~4, y=0~5, z=바닥 높이.
# 실제 천장 정중앙 센서(예시): (2.0, 2.5, 2.7). 센서의 x/y/z가 이미
# 이 방 좌표계로 변환되어 있다고 가정; FOV/차폐/포인트클라우드는 모사하지 않음.
SENSOR_POSITION = np.array([2.0, 2.5, ROOM_HEIGHT], dtype=float)

BED_X_MIN, BED_X_MAX = 0.6, 1.6
BED_Y_MIN, BED_Y_MAX = 1.6, 3.7
BED_CENTER = np.array([1.1, 2.65], dtype=float)
MATTRESS_Z = 0.65
LYING_Z = 0.90      # 누운 사람 대표 중심 (0.80~1.00m)
SLEEPING_Z = 0.87
SITTING_Z = 1.00
STANDING_Z = 1.32

ZONES = {
    "bed": BED_CENTER.copy(),
    "bed_side": np.array([1.95, 2.65], dtype=float),
    "center": np.array([2.65, 2.35], dtype=float),
    "door": np.array([3.5, 4.65], dtype=float),
    "bathroom": np.array([3.35, 3.6], dtype=float),
}


def in_bed_zone(x, y):
    return BED_X_MIN <= x <= BED_X_MAX and BED_Y_MIN <= y <= BED_Y_MAX


# ============================================================
# CSV 경로
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

RADAR_CSV = DATA_DIR / "radar_frame.csv"
EVENT_CSV = DATA_DIR / "simulation_event_log.csv"

RADAR_COLUMNS = [
    "frame_id",
    "sensor_id",
    "subject_id",
    "x",
    "y",
    "z",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "acceleration_x",
    "acceleration_y",
    "acceleration_z",
    "doppler",
    "point_count",
    "snr",
    "presence",
    "timestamp",
]

EVENT_COLUMNS = [
    "event_log_id",
    "sensor_id",
    "subject_id",
    "event_type",
    "event_detail",
    "started_at",
    "ended_at",
    "duration_sec",
]


# ============================================================
# 시간 관련 함수
# ============================================================

def is_night_time(dt=None):
    """
    22:00 ~ 05:59 를 야간으로 정의.
    """

    dt = dt or datetime.now()

    hour = dt.hour

    return (
        hour >= NIGHT_START_HOUR
        or hour < NIGHT_END_HOUR
    )


def day_period(dt=None):
    """
    실제 컴퓨터 시간을 기준으로 생활 구간 반환.
    """

    dt = dt or datetime.now()

    hour = dt.hour

    if (
        hour >= NIGHT_START_HOUR
        or hour < NIGHT_END_HOUR
    ):
        return "night"

    if 6 <= hour < 9:
        return "morning"

    if 9 <= hour < 18:
        return "day"

    return "evening"


# ============================================================
# CSV Storage
# ============================================================

class CSVStorage:

    def __init__(self):

        DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        self.lock = threading.Lock()

        # CSV는 항상 저장하고,
        # radar_frame은 0.1초마다 친구 API로 전송한다.
        self.sender = RealtimeSender()

        self._ensure_file(
            RADAR_CSV,
            RADAR_COLUMNS
        )

        self._ensure_file(
            EVENT_CSV,
            EVENT_COLUMNS
        )

        self.frame_id = (
            self._next_id(
                RADAR_CSV
            )
        )

        self.event_id = (
            self._next_id(
                EVENT_CSV
            )
        )


    @staticmethod
    def _ensure_file(
        path,
        columns
    ):

        if path.exists():
            return

        with open(
            path,
            "w",
            newline="",
            encoding="utf-8-sig"
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=columns
            )

            writer.writeheader()


    @staticmethod
    def _next_id(path):

        with open(
            path,
            "r",
            encoding="utf-8-sig"
        ) as file:

            count = max(
                sum(1 for _ in file) - 1,
                0
            )

        return count + 1


    def save_frames(
        self,
        frames
    ):

        if not frames:
            return

        with self.lock:

            with open(
                RADAR_CSV,
                "a",
                newline="",
                encoding="utf-8-sig"
            ) as file:

                writer = csv.DictWriter(
                    file,
                    fieldnames=RADAR_COLUMNS
                )

                for frame in frames:

                    frame[
                        "frame_id"
                    ] = self.frame_id

                    self.frame_id += 1

                    writer.writerow(
                        frame
                    )

        # ----------------------------------------------------
        # 0.1초 frame 개별 실시간 전송
        # HTTP 요청은 별도 Thread에서 처리되므로
        # 10FPS 생성 루프를 직접 막지 않는다.
        # ----------------------------------------------------
        for frame in frames:

            self.sender.submit(
                frame.copy()
            )


    def save_event(
        self,
        event_type,
        event_detail,
        started_at,
        ended_at
    ):

        duration = (
            ended_at
            - started_at
        ).total_seconds()

        row = {
            "event_log_id":
                self.event_id,

            "sensor_id":
                SENSOR_ID,

            "subject_id":
                SUBJECT_ID,

            "event_type":
                event_type,

            "event_detail":
                event_detail,

            "started_at":
                started_at.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3],

            "ended_at":
                ended_at.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3],

            "duration_sec":
                round(
                    duration,
                    3
                ),
        }

        self.event_id += 1

        with self.lock:

            with open(
                EVENT_CSV,
                "a",
                newline="",
                encoding="utf-8-sig"
            ) as file:

                writer = csv.DictWriter(
                    file,
                    fieldnames=EVENT_COLUMNS
                )

                writer.writerow(row)



# ============================================================
# Event Manager
# ============================================================

class EventManager:
    """
    여러 이벤트를 동시에 기록할 수 있음.

    예:
    SLEEP + BED_STAY

    NIGHT_EXIT + OCCUPANCY(ABSENT)

    FALL 이후
    INACTIVITY
    """

    def __init__(self, storage):

        self.storage = storage

        self.active = {}

        self.lock = (
            threading.RLock()
        )


    def is_active(
        self,
        event_type
    ):

        with self.lock:

            return (
                event_type
                in self.active
            )


    def start(
        self,
        event_type,
        detail=""
    ):

        with self.lock:

            if (
                event_type
                in self.active
            ):
                return

            now = datetime.now()

            self.active[
                event_type
            ] = {
                "detail": detail,
                "started_at": now,
            }

            print(
                f"[EVENT START] "
                f"{event_type}"
                f"{' / ' + detail if detail else ''}"
            )


    def end(
        self,
        event_type
    ):

        with self.lock:

            info = (
                self.active.pop(
                    event_type,
                    None
                )
            )

            if info is None:
                return

            now = datetime.now()

            self.storage.save_event(
                event_type=
                    event_type,

                event_detail=
                    info["detail"],

                started_at=
                    info["started_at"],

                ended_at=
                    now
            )

            print(
                f"[EVENT END]   "
                f"{event_type}"
                f"{' / ' + info['detail'] if info['detail'] else ''}"
            )


    def switch_occupancy(
        self,
        detail
    ):

        self.end(
            "OCCUPANCY"
        )

        self.start(
            "OCCUPANCY",
            detail
        )


    def end_all(self):

        with self.lock:

            for event_type in list(
                self.active.keys()
            ):

                self.end(
                    event_type
                )


    def names(self):

        with self.lock:

            return list(
                self.active.keys()
            )


# ============================================================
# Virtual Patient
# ============================================================

class VirtualPatient:

    def __init__(self):

        self.rng = (
            np.random.default_rng(
                RANDOM_SEED
            )
        )

        # 초기 위치
        self.x = float(
            ZONES["bed_side"][0]
        )

        self.y = float(
            ZONES["bed_side"][1]
        )

        self.z = STANDING_Z
        self.sitting_on_bed = False

        # 이전 위치
        self.prev_x = self.x
        self.prev_y = self.y
        self.prev_z = self.z

        # 속도
        self.vx = 0.0
        self.vy = 0.0
        self.vz = 0.0

        self.prev_vx = 0.0
        self.prev_vy = 0.0
        self.prev_vz = 0.0

        # 가속도
        self.ax = 0.0
        self.ay = 0.0
        self.az = 0.0

        self.presence = True

        self.state = "standing"

        self.target = None
        self.target_name = None

        self.walk_speed = 0.8

        self.phase = 0.0

        # Fall
        self.fall_start = None
        self.fall_end = None
        self.fall_progress = 0.0


    # ========================================================
    # 공통
    # ========================================================

    def clamp_room(self):

        self.x = float(
            np.clip(
                self.x,
                0.20,
                ROOM_WIDTH - 0.20
            )
        )

        self.y = float(
            np.clip(
                self.y,
                0.20,
                ROOM_DEPTH - 0.20
            )
        )

        self.z = float(
            np.clip(
                self.z,
                0.15,
                1.55
            )
        )


    def save_previous(self):

        self.prev_x = self.x
        self.prev_y = self.y
        self.prev_z = self.z

        self.prev_vx = self.vx
        self.prev_vy = self.vy
        self.prev_vz = self.vz


    def calculate_motion(self):

        # 실제 위치 변화 -> 속도
        self.vx = (
            self.x
            - self.prev_x
        ) / DT

        self.vy = (
            self.y
            - self.prev_y
        ) / DT

        self.vz = (
            self.z
            - self.prev_z
        ) / DT

        # 속도 변화 -> 가속도
        self.ax = (
            self.vx
            - self.prev_vx
        ) / DT

        self.ay = (
            self.vy
            - self.prev_vy
        ) / DT

        self.az = (
            self.vz
            - self.prev_vz
        ) / DT


    # ========================================================
    # 상태 전환
    # ========================================================

    def set_standing(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "standing"


    def set_sitting(self, on_bed=False):
        self.presence = True
        self.target = None
        self.target_name = None
        self.sitting_on_bed = on_bed
        self.state = "sitting"

    def set_lying(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "lying"


    def set_sleeping(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "sleeping"


    def set_inactivity(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "inactivity"


    def set_absent(self):

        self.presence = False

        self.target = None
        self.target_name = None

        self.state = "absent"

        self.vx = 0.0
        self.vy = 0.0
        self.vz = 0.0

        self.ax = 0.0
        self.ay = 0.0
        self.az = 0.0


    def enter_room(self):

        self.presence = True

        self.x = float(
            ZONES["door"][0]
        )

        self.y = float(
            ZONES["door"][1]
        )

        self.z = STANDING_Z

        self.state = "standing"

        self.target = None
        self.target_name = None

        # 순간 이동으로 인한 비정상 속도 방지
        self.prev_x = self.x
        self.prev_y = self.y
        self.prev_z = self.z

        self.vx = 0.0
        self.vy = 0.0
        self.vz = 0.0

        self.prev_vx = 0.0
        self.prev_vy = 0.0
        self.prev_vz = 0.0

        self.ax = 0.0
        self.ay = 0.0
        self.az = 0.0


    # ========================================================
    # 걷기
    # ========================================================

    def walk_to(
        self,
        target_name
    ):

        if target_name not in ZONES:

            raise ValueError(
                f"존재하지 않는 Zone: "
                f"{target_name}"
            )

        self.presence = True

        self.state = "walking"

        self.target_name = (
            target_name
        )

        self.target = (
            ZONES[
                target_name
            ].copy()
        )

        self.walk_speed = float(
            self.rng.uniform(
                0.60,
                1.05
            )
        )


    # ========================================================
    # 낙상
    # ========================================================

    def start_fall(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "falling"

        self.fall_progress = 0.0

        self.fall_start = np.array(
            [
                self.x,
                self.y,
                self.z
            ],
            dtype=float
        )

        angle = float(
            self.rng.uniform(
                0,
                2 * math.pi
            )
        )

        fall_distance = float(
            self.rng.uniform(
                0.35,
                0.60
            )
        )

        end_x = (
            self.x
            + math.cos(angle)
            * fall_distance
        )

        end_y = (
            self.y
            + math.sin(angle)
            * fall_distance
        )

        end_x = float(
            np.clip(
                end_x,
                0.20,
                ROOM_WIDTH - 0.20
            )
        )

        end_y = float(
            np.clip(
                end_y,
                0.20,
                ROOM_DEPTH - 0.20
            )
        )

        self.fall_end = np.array(
            [
                end_x,
                end_y,
                0.25
            ],
            dtype=float
        )


    def recover(self):

        self.presence = True

        self.target = None
        self.target_name = None

        self.state = "recovering"


    # ========================================================
    # 각 행동 업데이트
    # ========================================================

    def update_standing(self):
        self.phase += DT
        # 제자리 자세 변화는 작게; 위치 오차는 get_radar_frame에서 추가됨.
        self.x += math.sin(self.phase * 0.8) * 0.0006
        self.y += math.cos(self.phase * 0.6) * 0.0005
        self.z += (STANDING_Z - self.z) * 0.12

    def update_walking(self):

        current = np.array(
            [
                self.x,
                self.y
            ],
            dtype=float
        )

        delta = (
            self.target
            - current
        )

        distance = float(
            np.linalg.norm(
                delta
            )
        )

        if distance < 0.05:

            self.x = float(
                self.target[0]
            )

            self.y = float(
                self.target[1]
            )

            self.target = None
            self.target_name = None

            self.state = "standing"

            return True

        direction = (
            delta
            / distance
        )

        speed = float(
            np.clip(
                self.walk_speed
                + self.rng.normal(
                    0,
                    0.025
                ),
                0.45,
                1.20
            )
        )

        step = min(
            speed * DT,
            distance
        )

        self.x += float(
            direction[0]
            * step
        )

        self.y += float(
            direction[1]
            * step
        )

        self.phase += DT

        self.z = (
            STANDING_Z
            + 0.018
            * math.sin(
                self.phase
                * 2
                * math.pi
                * 1.6
            )
        )

        return False


    def update_sitting(self):
        target_z = 1.08 if self.sitting_on_bed else SITTING_Z
        self.z += (target_z - self.z) * 0.16

    def update_lying(self):
        self.x += (ZONES["bed"][0] - self.x) * 0.08
        self.y += (ZONES["bed"][1] - self.y) * 0.08
        self.z += (LYING_Z - self.z) * 0.12

    def update_sleeping(self):
        self.phase += DT
        self.x += (ZONES["bed"][0] - self.x) * 0.10
        self.y += (ZONES["bed"][1] - self.y) * 0.10
        self.z = SLEEPING_Z + 0.006 * math.sin(self.phase * 2 * math.pi * 0.25)

    def update_inactivity(self):

        self.phase += DT

        # 완전한 0은 아니라는 가정
        self.x += (
            math.sin(
                self.phase * 0.4
            )
            * 0.00012
        )

        self.y += (
            math.cos(
                self.phase * 0.5
            )
            * 0.00012
        )


    def update_falling(self):

        fall_duration = 0.8

        self.fall_progress += (
            DT / fall_duration
        )

        p = min(
            self.fall_progress,
            1.0
        )

        # Smoothstep
        eased = (
            p
            * p
            * (
                3
                - 2 * p
            )
        )

        pose = (
            self.fall_start
            + (
                self.fall_end
                - self.fall_start
            )
            * eased
        )

        self.x = float(
            pose[0]
        )

        self.y = float(
            pose[1]
        )

        self.z = float(
            pose[2]
        )

        if p >= 1.0:

            self.state = "fallen"

            return True

        return False


    def update_fallen(self):

        self.phase += DT

        self.z += (
            0.25 - self.z
        ) * 0.3

        self.x += (
            math.sin(
                self.phase * 0.3
            )
            * 0.00008
        )

        self.y += (
            math.cos(
                self.phase * 0.3
            )
            * 0.00008
        )


    def update_recovering(self):

        self.z += (
            STANDING_Z - self.z
        ) * 0.07

        if abs(
            self.z - STANDING_Z
        ) < 0.02:

            self.z = STANDING_Z

            self.state = "standing"

            return True

        return False


    # ========================================================
    # Frame Update
    # ========================================================

    def update(self):

        result = {
            "movement_finished": False,
            "fall_finished": False,
            "recovery_finished": False,
        }

        if not self.presence:

            self.vx = 0.0
            self.vy = 0.0
            self.vz = 0.0

            self.ax = 0.0
            self.ay = 0.0
            self.az = 0.0

            return result

        self.save_previous()

        if self.state == "standing":

            self.update_standing()

        elif self.state == "walking":

            result[
                "movement_finished"
            ] = self.update_walking()

        elif self.state == "sitting":

            self.update_sitting()

        elif self.state == "lying":

            self.update_lying()

        elif self.state == "sleeping":

            self.update_sleeping()

        elif self.state == "inactivity":

            self.update_inactivity()

        elif self.state == "falling":

            result[
                "fall_finished"
            ] = self.update_falling()

        elif self.state == "fallen":

            self.update_fallen()

        elif self.state == "recovering":

            result[
                "recovery_finished"
            ] = (
                self.update_recovering()
            )

        self.clamp_room()

        self.calculate_motion()

        return result


    # ========================================================
    # Radar 측정값
    # ========================================================

    def get_radar_frame(self):

        if not self.presence:

            return {
                "x": None,
                "y": None,
                "z": None,

                "velocity_x": None,
                "velocity_y": None,
                "velocity_z": None,

                "acceleration_x": None,
                "acceleration_y": None,
                "acceleration_z": None,

                "doppler": 0.0,

                "point_count": 0,

                "snr": 0.0,

                "presence": 0
            }

        # 실제 위치 생성 후
        # 측정 Noise만 추가
        measured_x = (
            self.x
            + self.rng.normal(
                0,
                0.012
            )
        )

        measured_y = (
            self.y
            + self.rng.normal(
                0,
                0.012
            )
        )

        measured_z = (
            self.z
            + self.rng.normal(
                0,
                0.008
            )
        )

        measured_position = (
            np.array(
                [
                    measured_x,
                    measured_y,
                    measured_z
                ],
                dtype=float
            )
        )

        velocity = np.array(
            [
                self.vx,
                self.vy,
                self.vz
            ],
            dtype=float
        )

        sensor_to_person = (
            measured_position
            - SENSOR_POSITION
        )

        distance = float(
            np.linalg.norm(
                sensor_to_person
            )
        )

        if distance > 1e-8:

            radial_direction = (
                sensor_to_person
                / distance
            )

            # 센서 방향 속도
            doppler = float(
                np.dot(
                    velocity,
                    radial_direction
                )
            )

        else:

            doppler = 0.0

        speed = float(
            np.linalg.norm(
                velocity
            )
        )

        point_count = int(
            np.clip(
                self.rng.normal(
                    30
                    + speed * 8,
                    4
                ),
                8,
                70
            )
        )

        snr = float(
            np.clip(
                24.5
                - distance * 1.7
                + self.rng.normal(
                    0,
                    1.2
                ),
                5,
                30
            )
        )

        return {
            "x":
                round(
                    float(
                        measured_x
                    ),
                    4
                ),

            "y":
                round(
                    float(
                        measured_y
                    ),
                    4
                ),

            "z":
                round(
                    float(
                        measured_z
                    ),
                    4
                ),

            "velocity_x":
                round(
                    float(
                        self.vx
                    ),
                    4
                ),

            "velocity_y":
                round(
                    float(
                        self.vy
                    ),
                    4
                ),

            "velocity_z":
                round(
                    float(
                        self.vz
                    ),
                    4
                ),

            "acceleration_x":
                round(
                    float(
                        self.ax
                    ),
                    4
                ),

            "acceleration_y":
                round(
                    float(
                        self.ay
                    ),
                    4
                ),

            "acceleration_z":
                round(
                    float(
                        self.az
                    ),
                    4
                ),

            "doppler":
                round(
                    doppler,
                    4
                ),

            "point_count":
                point_count,

            "snr":
                round(
                    snr,
                    3
                ),

            "presence":
                1
        }


# ============================================================
# Simulator
# ============================================================

class RadarSimulator:

    def __init__(self):

        self.storage = (
            CSVStorage()
        )

        self.events = (
            EventManager(
                self.storage
            )
        )

        self.patient = (
            VirtualPatient()
        )

        self.rng = (
            np.random.default_rng(
                RANDOM_SEED
            )
        )

        self.running = False

        self.auto_running = False

        self.auto_deadline = None

        self.state_lock = (
            threading.RLock()
        )

        self.frame_queue = (
            Queue()
        )

        # 야간이탈 파생에 사용
        self.last_bed_exit_at = None
        self.last_random_fall_at = None
        self.auto_steps = 0
        self.actions_since_inactivity = 3


    # ========================================================
    # 10 FPS Generator
    # ========================================================

    def generator_loop(self):

        next_time = (
            time.perf_counter()
        )

        while self.running:

            with self.state_lock:

                result = (
                    self.patient.update()
                )

                if result[
                    "movement_finished"
                ]:

                    self.events.end(
                        "MOVEMENT"
                    )

                if result[
                    "fall_finished"
                ]:

                    # FALL은 넘어지는 과정까지만
                    self.events.end(
                        "FALL"
                    )

                data = (
                    self.patient
                    .get_radar_frame()
                )

            frame = {
                "frame_id":
                    None,

                "sensor_id":
                    SENSOR_ID,

                "subject_id": SUBJECT_ID,

                **data,

                "timestamp":
                    (
                        datetime.now()
                        .strftime(
                            "%Y-%m-%d "
                            "%H:%M:%S.%f"
                        )[:-3]
                    )
            }

            self.frame_queue.put(
                frame
            )

            next_time += DT

            delay = (
                next_time
                - time.perf_counter()
            )

            if delay > 0:

                time.sleep(
                    delay
                )

            else:

                next_time = (
                    time.perf_counter()
                )


    # ========================================================
    # Writer
    # ========================================================

    def writer_loop(self):

        # frame_queue에 0.1초마다 들어오는
        # radar_frame을 즉시 CSV 저장 + API 전송 Queue에 등록
        while (
            self.running
            or not self.frame_queue.empty()
        ):

            try:

                frame = (
                    self.frame_queue.get(
                        timeout=0.2
                    )
                )

            except Empty:

                continue

            self.storage.save_frames(
                [frame]
            )

            self.frame_queue.task_done()


    # ========================================================
    # 자동 모드 시간 관리
    # ========================================================

    def auto_active(self):

        if not self.running:
            return False

        if not self.auto_running:
            return False

        if (
            self.auto_deadline
            is not None
            and datetime.now()
            >= self.auto_deadline
        ):

            self.auto_running = False

            return False

        return True


    def scenario_wait(
        self,
        seconds
    ):

        end_time = (
            time.monotonic()
            + seconds
        )

        while self.auto_active():

            remaining = (
                end_time
                - time.monotonic()
            )

            if remaining <= 0:

                return True

            time.sleep(
                min(
                    0.5,
                    remaining
                )
            )

        return False


    def wait_for_state(
        self,
        states,
        timeout=30
    ):

        if isinstance(
            states,
            str
        ):

            states = {
                states
            }

        else:

            states = set(
                states
            )

        limit = (
            time.monotonic()
            + timeout
        )

        while self.running:

            # 자동 모드 중이면 종료 시간도 체크
            if (
                self.auto_running
                and not self.auto_active()
            ):

                return False

            with self.state_lock:

                current = (
                    self.patient.state
                )

            if current in states:

                return True

            if (
                time.monotonic()
                >= limit
            ):

                return False

            time.sleep(0.1)

        return False


    # ========================================================
    # Event / 행동 공통
    # ========================================================

    def mark_bed_exit(self):

        if self.events.is_active(
            "BED_STAY"
        ):

            self.events.end(
                "BED_STAY"
            )

            self.last_bed_exit_at = (
                datetime.now()
            )


    def stand(self):

        self.events.end(
            "SLEEP"
        )

        self.events.end(
            "INACTIVITY"
        )

        self.mark_bed_exit()

        with self.state_lock:

            self.patient.set_standing()


    def sit(
        self,
        on_bed=False
    ):

        self.events.end(
            "SLEEP"
        )

        self.events.end(
            "INACTIVITY"
        )

        if not on_bed:

            self.mark_bed_exit()

        with self.state_lock:

            self.patient.set_sitting(on_bed=on_bed)


    def lie(self):

        self.events.end(
            "INACTIVITY"
        )

        with self.state_lock:

            self.patient.set_lying()

        self.events.start(
            "BED_STAY",
            "ON_BED"
        )


    def sleep(self):

        self.events.end(
            "INACTIVITY"
        )

        with self.state_lock:

            self.patient.set_sleeping()

        self.events.start(
            "BED_STAY",
            "ON_BED"
        )

        self.events.start(
            "SLEEP",
            "SLEEPING"
        )


    def inactivity(
        self,
        detail="LOW_MOVEMENT"
    ):

        with self.state_lock:

            self.patient.set_inactivity()

        self.events.start(
            "INACTIVITY",
            detail
        )


    def walk(
        self,
        target
    ):

        self.events.end(
            "INACTIVITY"
        )

        self.events.end(
            "SLEEP"
        )

        if target != "bed":

            self.mark_bed_exit()

        with self.state_lock:

            self.patient.walk_to(
                target
            )

        self.events.start(
            "MOVEMENT",
            f"WALK_TO_{target.upper()}"
        )


    def fall(self):

        self.events.end(
            "MOVEMENT"
        )

        self.events.end(
            "INACTIVITY"
        )

        self.events.end(
            "SLEEP"
        )

        self.mark_bed_exit()

        with self.state_lock:

            self.patient.start_fall()

        self.events.start(
            "FALL",
            "FALL_SUSPECT"
        )


    def recover(self):

        self.events.end(
            "INACTIVITY"
        )

        with self.state_lock:

            self.patient.recover()

        self.wait_for_state(
            "standing",
            timeout=30
        )


    def should_be_night_exit(self):

        now = datetime.now()

        if not is_night_time(
            now
        ):

            return False

        if (
            self.last_bed_exit_at
            is None
        ):

            return False

        elapsed = (
            now
            - self.last_bed_exit_at
        )

        return (
            elapsed
            <= timedelta(
                minutes=
                    NIGHT_EXIT_BED_WINDOW_MINUTES
            )
        )


    def set_absent(self):

        self.events.end(
            "MOVEMENT"
        )

        self.events.end(
            "INACTIVITY"
        )

        self.events.end(
            "SLEEP"
        )

        self.mark_bed_exit()

        # --------------------------------------------
        # NIGHT_EXIT는 여기서 자동 파생
        # --------------------------------------------

        if (
            self.should_be_night_exit()
        ):

            self.events.start(
                "NIGHT_EXIT",
                "AUTO_DERIVED_FROM_"
                "NIGHT_ABSENCE"
            )

        with self.state_lock:

            self.patient.set_absent()

        self.events.switch_occupancy(
            "ABSENT"
        )


    def enter_room(self):

        with self.state_lock:

            self.patient.enter_room()

        self.events.switch_occupancy(
            "PRESENT"
        )

        # 야간 이탈은 재입실 시 종료
        self.events.end(
            "NIGHT_EXIT"
        )


    # ========================================================
    # 랜덤 행동 helper
    # ========================================================

    def random_seconds(
        self,
        min_seconds,
        max_seconds
    ):

        return float(
            self.rng.uniform(
                min_seconds,
                max_seconds
            )
        )


    def wake_up_sequence(self):
        """침대에서는 앉았다 기립, 의자에서 앉았다면 바로 기립."""
        self.events.end("SLEEP")
        with self.state_lock:
            current = self.patient.state
        if current in {"sleeping", "lying"}:
            self.sit(on_bed=True)
            if not self.scenario_wait(self.random_seconds(8, 25)):
                return False
        self.stand()
        return self.scenario_wait(self.random_seconds(4, 12))

    def random_walk_action(self):
        """한 행동에서 여러 목적지까지 걸어 움직임 이벤트를 반복 생성."""
        print("[AUTO] 활발한 병실 이동")
        with self.state_lock:
            current_state = self.patient.state
        if current_state in {"sleeping", "lying", "sitting"}:
            if not self.wake_up_sequence():
                return
        targets = ["center", "bed_side", "bathroom", "door"]
        rounds = int(self.rng.integers(WALK_ROUNDS_MIN, WALK_ROUNDS_MAX + 1))
        for _ in range(rounds):
            if not self.auto_active():
                return
            with self.state_lock:
                pos = np.array([self.patient.x, self.patient.y])
            far = [t for t in targets if np.linalg.norm(ZONES[t] - pos) >= 1.2]
            target = str(self.rng.choice(far or targets))
            print(f"[AUTO] 병실 이동 -> {target}")
            self.walk(target)
            if not self.wait_for_state("standing", timeout=30):
                return
            if not self.scenario_wait(self.random_seconds(5, 22)):
                return

    def bathroom_action(self):
        print("[AUTO] 화장실 방향 이동 (짧은 휴식은 LONG_INACTIVITY 라벨 아님)")
        with self.state_lock:
            state = self.patient.state
        if state in {"sleeping", "lying", "sitting"}:
            if not self.wake_up_sequence():
                return
        self.walk("bathroom")
        if not self.wait_for_state("standing", timeout=30):
            return
        if not self.auto_active():
            return
        self.sit(on_bed=False)
        if not self.scenario_wait(self.random_seconds(45, 120)):
            return
        self.stand()

    def bed_rest_action(self):
        print("[AUTO] 침대 휴식")
        with self.state_lock:
            state = self.patient.state
        if state not in {"lying", "sleeping"}:
            self.walk("bed")
            if not self.wait_for_state("standing", timeout=30):
                return
        if not self.auto_active():
            return
        self.lie()
        self.scenario_wait(self.random_seconds(80, 240))

    def sleep_action(self):
        period = day_period()
        print(f"[AUTO] 수면 시작 (시간대={period})")
        with self.state_lock:
            state = self.patient.state
        if state not in {"lying", "sleeping"}:
            self.walk("bed")
            if not self.wait_for_state("standing", timeout=30):
                return
            if not self.auto_active():
                return
            self.lie()
            if not self.scenario_wait(self.random_seconds(12, 30)):
                return
        self.sleep()
        if period == "night":
            duration = self.random_seconds(3600, 21600)  # 15분 수면 기준 충족
        elif period == "evening":
            duration = self.random_seconds(1800, 3600)
        else:
            duration = self.random_seconds(920, 1800)
        self.scenario_wait(duration)

    def inactivity_action(self):
        print("[AUTO] 장시간 미움직임 (Event Engine 30분 기준 초과)")
        with self.state_lock:
            state = self.patient.state
            on_bed = in_bed_zone(self.patient.x, self.patient.y)
        if state in {"sleeping", "lying", "sitting"}:
            if not self.wake_up_sequence():
                return
        # LONG_INACTIVITY 엔진은 침대 밖에서만 검출. 먼저 실제 좌표를 이동시킴.
        if on_bed or self.events.is_active("BED_STAY"):
            self.walk("bed_side")
            if not self.wait_for_state("standing", timeout=30):
                return
            if not self.scenario_wait(self.random_seconds(5, 12)):
                return
        with self.state_lock:
            assert not in_bed_zone(self.patient.x, self.patient.y), "침대 밖 좌표 이동 실패"
        self.inactivity("LONG_INACTIVITY_TEST")
        duration = self.random_seconds(INACTIVITY_DURATION_MIN, INACTIVITY_DURATION_MAX)
        print(f"[AUTO] 저움직임 {duration/60:.1f}분 유지 예정 (실제 경과시간)")
        if not self.scenario_wait(duration):
            return
        self.events.end("INACTIVITY")
        self.stand()

    def sit_rest_action(self):
        print("[AUTO] 짧게 앉아서 휴식")
        with self.state_lock:
            state = self.patient.state
        if state in {"sleeping", "lying"}:
            self.events.end("SLEEP")
            self.sit(on_bed=True)
        else:
            self.sit(on_bed=False)
        self.scenario_wait(self.random_seconds(40, 130))

    def leave_room_action(self):
        period = day_period()
        print(f"[AUTO] 병실 외출/재입실 ({period})")
        with self.state_lock:
            state = self.patient.state
        if state in {"sleeping", "lying", "sitting"}:
            if not self.wake_up_sequence():
                return
        self.walk("door")
        if not self.wait_for_state("standing", timeout=30):
            return
        if not self.auto_active():
            return
        self.set_absent()
        # Engine 부재 10초 확정 시간보다 충분히 길게 유지.
        absence_seconds = self.random_seconds(35, 125) if is_night_time() else self.random_seconds(45, 180)
        if not self.scenario_wait(absence_seconds):
            return
        print("[AUTO] 병실 재입실")
        self.enter_room()
        self.scenario_wait(self.random_seconds(8, 24))

    def fall_action(self):
        """매우 낮은 확률로만 낙상 후보 생성; 모의 환자 안전 검증용이 아님."""
        print("!!! [AUTO] FALL 의심 (드문 합성 시나리오) !!!")
        self.last_random_fall_at = datetime.now()
        with self.state_lock:
            state = self.patient.state
        if state in {"sleeping", "lying", "sitting"}:
            if not self.wake_up_sequence():
                return
        if self.rng.random() < 0.65:
            target = str(self.rng.choice(["center", "bathroom", "bed_side"]))
            self.walk(target)
            if not self.scenario_wait(self.random_seconds(1.0, 2.2)):
                return
        self.fall()
        if not self.wait_for_state("fallen", timeout=10):
            return
        if not self.auto_active():
            return
        # 낮은 자세 유지 3초 초과(낙상 판별용), 30분 미움직임과는 구분
        if not self.scenario_wait(self.random_seconds(15, 45)):
            return
        print("[AUTO] 낙상 후 회복")
        self.recover()
        self.scenario_wait(self.random_seconds(15, 40))

    def choose_random_action(self):
        """활동/외출/미움직임을 늘리고 낙상은 드물게 선택."""
        # 처음 두 행동은 재실 이동과 외출/재입실: 시작하자마자 30분 정지하지 않음.
        if self.auto_steps == 0:
            return "walk"
        if self.auto_steps == 1:
            return "leave_room"
        period = day_period()
        if period == "night":
            weights = {
                "walk": 0.24, "leave_room": 0.14,
                "inactivity": 0.16, "bathroom": 0.12,
                "sleep": 0.22, "bed_rest": 0.06,
                "sit_rest": 0.06, "fall": FALL_WEIGHT_NIGHT,
            }
        else:
            weights = {
                "walk": 0.36, "leave_room": 0.23,
                "inactivity": 0.23, "bathroom": 0.08,
                "sleep": 0.03, "bed_rest": 0.025,
                "sit_rest": 0.045, "fall": FALL_WEIGHT_DAY,
            }
        # 최소 두 개의 다른 행동을 거친 뒤 다음 장시간 미움직임 허용.
        if self.actions_since_inactivity < 2:
            weights["inactivity"] = 0.0
        elif self.actions_since_inactivity >= 6:
            weights["inactivity"] *= 1.8
        if (self.last_random_fall_at is not None and
                (datetime.now() - self.last_random_fall_at).total_seconds() < FALL_COOLDOWN_SECONDS):
            weights["fall"] = 0.0
        actions = list(weights)
        probabilities = np.array(list(weights.values()), dtype=float)
        probabilities /= probabilities.sum()
        return str(self.rng.choice(actions, p=probabilities))

    def execute_random_action(
        self,
        action
    ):

        # 자동 실행 중단으로 외출 상태에서 멈췄다가 재시작했을 때
        # 다음 행동을 PRESENT로 오인하지 않도록 입실 이벤트부터 기록.
        with self.state_lock:
            absent = not self.patient.presence
        if absent:
            self.enter_room()

        if action == "sleep":

            self.sleep_action()

        elif action == "bed_rest":

            self.bed_rest_action()

        elif action == "bathroom":

            self.bathroom_action()

        elif action == "inactivity":

            self.inactivity_action()

        elif action == "walk":

            self.random_walk_action()

        elif action == "sit_rest":

            self.sit_rest_action()

        elif action == "leave_room":

            self.leave_room_action()

        elif action == "fall":

            self.fall_action()


    # ========================================================
    # 랜덤 자동 실행
    # ========================================================

    def run_random_auto(
        self,
        hours=None
    ):

        if self.auto_running:

            print(
                "자동 실행이 이미 "
                "진행 중입니다."
            )

            return

        self.auto_running = True
        self.auto_steps = 0
        self.actions_since_inactivity = 3

        if hours is None:

            self.auto_deadline = None

            mode_name = (
                "CONTINUOUS"
            )

        else:

            self.auto_deadline = (
                datetime.now()
                + timedelta(
                    hours=hours
                )
            )

            mode_name = (
                f"{hours:g} HOURS"
            )

        print()
        print(
            "=" * 58
        )
        print(
            f" RANDOM AUTO START : "
            f"{mode_name}"
        )
        print(
            f" 실제 시작시각 : "
            f"{datetime.now()}"
        )
        print(
            f" 현재 시간대   : "
            f"{day_period()}"
        )

        if self.auto_deadline:

            print(
                f" 자동 종료시각 : "
                f"{self.auto_deadline}"
            )

        else:

            print(
                " 자동 종료시각 : "
                "없음 (auto_stop 또는 stop)"
            )

        print(
            "=" * 58
        )
        print()

        try:

            while self.auto_active():

                action = (
                    self.choose_random_action()
                )

                print()
                print(
                    f"[AUTO SELECT] "
                    f"{action}"
                    f" / "
                    f"{datetime.now():%Y-%m-%d %H:%M:%S}"
                    f" / "
                    f"{day_period()}"
                )

                self.execute_random_action(
                    action
                )
                self.auto_steps += 1
                self.actions_since_inactivity = (
                    0 if action == "inactivity"
                    else self.actions_since_inactivity + 1
                )

                if not self.auto_active():

                    break

                # 행동과 행동 사이
                # 짧은 정상 상태 구간을 랜덤하게 추가
                with self.state_lock:

                    state = (
                        self.patient.state
                    )

                if state not in {
                    "sleeping",
                    "lying",
                    "fallen",
                    "absent"
                }:

                    self.stand()

                    self.scenario_wait(
                        self.random_seconds(
                            30,
                            240
                        )
                    )

        finally:

            self.auto_running = False

            print()
            print(
                "=" * 58
            )
            print(
                " RANDOM AUTO END"
            )
            print(
                f" 종료시각 : "
                f"{datetime.now()}"
            )
            print(
                "=" * 58
            )
            print()


    # ========================================================
    # 상태
    # ========================================================

    def show_status(self):

        with self.state_lock:

            p = self.patient

            print()
            print(
                "=" * 48
            )

            print(
                f"현재시각     : "
                f"{datetime.now()}"
            )

            print(
                f"시간대       : "
                f"{day_period()}"
            )

            print(
                f"야간 여부    : "
                f"{is_night_time()}"
            )

            print(
                f"state        : "
                f"{p.state}"
            )

            print(
                f"presence     : "
                f"{p.presence}"
            )

            print(
                f"x, y, z      : "
                f"{p.x:.3f}, "
                f"{p.y:.3f}, "
                f"{p.z:.3f}"
            )

            print(
                f"vx, vy, vz   : "
                f"{p.vx:.3f}, "
                f"{p.vy:.3f}, "
                f"{p.vz:.3f}"
            )

            print(
                f"active events: "
                f"{self.events.names()}"
            )

            print(
                f"auto running : "
                f"{self.auto_running}"
            )

            if self.auto_deadline:

                print(
                    f"auto deadline: "
                    f"{self.auto_deadline}"
                )

            else:

                print(
                    "auto deadline: None"
                )

            print(
                "=" * 48
            )
            print()


    # ========================================================
    # Command
    # ========================================================

    def handle_command(
        self,
        command
    ):

        parts = (
            command
            .strip()
            .lower()
            .split()
        )

        if not parts:
            return

        cmd = parts[0]

        # --------------------------------------------
        # AUTO
        # --------------------------------------------

        if cmd == "auto":

            if len(parts) < 2:

                print(
                    "사용법:"
                )

                print(
                    "  auto 6"
                )

                print(
                    "  auto 12"
                )

                print(
                    "  auto 24"
                )

                print(
                    "  auto continuous"
                )

                return

            value = parts[1]

            if value in {
                "continuous",
                "continue",
                "infinite"
            }:

                hours = None

            else:

                try:

                    hours = float(
                        value
                    )

                    if hours <= 0:

                        raise ValueError

                except ValueError:

                    print(
                        "시간은 양수 숫자 또는 "
                        "continuous를 입력하세요."
                    )

                    return

            thread = threading.Thread(
                target=
                    self.run_random_auto,

                args=(
                    hours,
                ),

                daemon=True
            )

            thread.start()

        elif cmd == "auto_stop":

            self.auto_running = False

            print(
                "자동 실행 중단 요청"
            )

        # --------------------------------------------
        # 수동
        # --------------------------------------------

        elif cmd == "walk":

            if len(parts) < 2:

                print(
                    "예: walk center"
                )

                return

            self.walk(
                parts[1]
            )

        elif cmd == "stand":

            self.stand()

        elif cmd == "sit":

            self.sit()

        elif cmd == "lie":

            self.lie()

        elif cmd == "sleep":

            self.sleep()

        elif cmd == "inactivity":

            self.inactivity()

        elif cmd == "fall":

            self.fall()

        elif cmd == "recover":

            self.recover()

        elif cmd == "absence":

            self.set_absent()

        elif cmd == "enter":

            self.enter_room()

        elif cmd == "status":

            self.show_status()

        else:

            print(
                f"알 수 없는 명령어: "
                f"{cmd}"
            )


    # ========================================================
    # RUN
    # ========================================================

    def run(self):

        self.running = True

        # 실행 시점부터 재실
        self.events.start(
            "OCCUPANCY",
            "PRESENT"
        )

        generator_thread = (
            threading.Thread(
                target=
                    self.generator_loop,

                daemon=True
            )
        )

        writer_thread = (
            threading.Thread(
                target=
                    self.writer_loop,

                daemon=True
            )
        )

        generator_thread.start()

        writer_thread.start()

        print()
        print(
            "=" * 64
        )
        print(
            " CareWave Random Continuous Radar Simulator"
        )
        print(
            "=" * 64
        )

        print(
            f"Radar CSV : "
            f"{RADAR_CSV}"
        )

        print(
            f"Event CSV : "
            f"{EVENT_CSV}"
        )

        print(
            "친구 API  : "
            f"{self.storage.sender.api_url}"
        )

        print(
            "전송 방식 : "
            "radar_frame 1개 생성 즉시 HTTP POST"
        )

        print(
            "전송 주기 : "
            "약 0.1초 / 10 requests per second"
        )

        print()
        print(
            "자동 랜덤 실행"
        )

        print(
            "  auto 6           "
            "6시간"
        )

        print(
            "  auto 12          "
            "12시간"
        )

        print(
            "  auto 24          "
            "24시간"
        )

        print(
            "  auto continuous  "
            "중단할 때까지 계속"
        )

        print(
            "  auto_stop        "
            "자동 행동만 중단"
        )

        print()
        print(
            "수동 명령"
        )

        print(
            "  walk bed"
        )

        print(
            "  walk bed_side"
        )

        print(
            "  walk center"
        )

        print(
            "  walk door"
        )

        print(
            "  walk bathroom"
        )

        print(
            "  stand"
        )

        print(
            "  sit"
        )

        print(
            "  lie"
        )

        print(
            "  sleep"
        )

        print(
            "  inactivity"
        )

        print(
            "  fall"
        )

        print(
            "  recover"
        )

        print(
            "  absence"
        )

        print(
            "  enter"
        )

        print(
            "  status"
        )

        print(
            "  stop"
        )

        print()
        print(
            f"야간 기준: "
            f"{NIGHT_START_HOUR}:00 ~ "
            f"{NIGHT_END_HOUR}:00"
        )

        print()

        try:

            while self.running:

                command = (
                    input(">>> ")
                    .strip()
                )

                if (
                    command.lower()
                    == "stop"
                ):

                    self.auto_running = False

                    self.running = False

                    break

                self.handle_command(
                    command
                )

        except KeyboardInterrupt:

            self.auto_running = False

            self.running = False

        generator_thread.join()

        writer_thread.join()

        self.events.end_all()

        # 남아 있는 API 전송 Queue 정리
        self.storage.sender.close()

        print()
        print(
            "Simulator 종료"
        )

        print(
            f"Radar CSV:"
            f"\n{RADAR_CSV}"
        )

        print()
        print(
            f"Event CSV:"
            f"\n{EVENT_CSV}"
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    simulator = (
        RadarSimulator()
    )

    simulator.run()
