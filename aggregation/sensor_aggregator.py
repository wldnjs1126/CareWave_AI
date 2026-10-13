from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import fmean

from api.schemas import RadarFrameIn
from database.sensor_raw_repository import SensorRawRepository


def _avg(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return fmean(cleaned) if cleaned else None


def _min(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return min(cleaned) if cleaned else None


def _max(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else None


def _mode(values: list[str | None]) -> str | None:
    cleaned = [value for value in values if value]
    if not cleaned:
        return None
    return Counter(cleaned).most_common(1)[0][0]


def _floor_to_second(value: datetime) -> datetime:
    return value.replace(microsecond=0)


@dataclass
class WindowBuffer:
    sensor_id: int
    subject_id: int | None
    window_start: datetime
    frames: list[RadarFrameIn] = field(default_factory=list)

    @property
    def window_end(self) -> datetime:
        return self.window_start + timedelta(seconds=1)


class SensorAggregator:
    """Collect radar frames in RAM and persist one sensor_raw row per second."""

    def __init__(self, repository: SensorRawRepository | None = None) -> None:
        self.repository = repository or SensorRawRepository()
        self._buffers: dict[tuple[int, int | None], WindowBuffer] = {}
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    async def add_frame(self, frame: RadarFrameIn) -> None:
        timestamp = frame.timestamp or datetime.now()
        frame.timestamp = timestamp
        key = (frame.sensor_id, frame.subject_id)
        window_start = _floor_to_second(timestamp)

        flush_row: dict | None = None

        async with self._lock:
            current = self._buffers.get(key)

            if current is None:
                current = WindowBuffer(
                    sensor_id=frame.sensor_id,
                    subject_id=frame.subject_id,
                    window_start=window_start,
                )
                self._buffers[key] = current

            elif window_start > current.window_start:
                flush_row = self._build_row(current)
                current = WindowBuffer(
                    sensor_id=frame.sensor_id,
                    subject_id=frame.subject_id,
                    window_start=window_start,
                )
                self._buffers[key] = current

            elif window_start < current.window_start:
                # Late frame: ignore instead of corrupting a newer second bucket.
                return

            current.frames.append(frame)

        if flush_row:
            await asyncio.to_thread(self.repository.save, flush_row)

    async def flush_expired(self) -> None:
        now = datetime.now()
        rows: list[dict] = []

        async with self._lock:
            expired_keys = [
                key
                for key, buffer in self._buffers.items()
                if buffer.window_end <= now
            ]

            for key in expired_keys:
                buffer = self._buffers.pop(key)
                if buffer.frames:
                    rows.append(self._build_row(buffer))

        for row in rows:
            await asyncio.to_thread(self.repository.save, row)

    async def run_flusher(self) -> None:
        self._stop_event.clear()
        while not self._stop_event.is_set():
            await self.flush_expired()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def shutdown(self) -> None:
        self._stop_event.set()
        await self.flush_expired()

    @staticmethod
    def _build_row(buffer: WindowBuffer) -> dict:
        frames = buffer.frames

        speeds: list[float | None] = []
        movement_levels: list[float | None] = []

        for frame in frames:
            speed = frame.speed
            if speed is None and None not in (
                frame.velocity_x,
                frame.velocity_y,
                frame.velocity_z,
            ):
                speed = (
                    frame.velocity_x ** 2
                    + frame.velocity_y ** 2
                    + frame.velocity_z ** 2
                ) ** 0.5
            speeds.append(speed)
            movement_levels.append(
                frame.movement_level if frame.movement_level is not None else speed
            )

        return {
            "sensor_id": buffer.sensor_id,
            "subject_id": buffer.subject_id,
            "window_start": buffer.window_start,
            "window_end": buffer.window_end,
            "sample_count": len(frames),
            "presence": int(any(frame.presence for frame in frames)),
            "avg_x": _avg([frame.x for frame in frames]),
            "avg_y": _avg([frame.y for frame in frames]),
            "avg_z": _avg([frame.z for frame in frames]),
            "min_z": _min([frame.z for frame in frames]),
            "avg_velocity_x": _avg([frame.velocity_x for frame in frames]),
            "avg_velocity_y": _avg([frame.velocity_y for frame in frames]),
            "avg_velocity_z": _avg([frame.velocity_z for frame in frames]),
            "min_velocity_z": _min([frame.velocity_z for frame in frames]),
            "avg_speed": _avg(speeds),
            "max_speed": _max(speeds),
            "avg_acceleration_z": _avg([frame.acceleration_z for frame in frames]),
            "min_acceleration_z": _min([frame.acceleration_z for frame in frames]),
            "avg_height": _avg([frame.height for frame in frames]),
            "min_height": _min([frame.height for frame in frames]),
            "avg_movement_level": _avg(movement_levels),
            "zone": _mode([frame.zone for frame in frames]),
            "created_at": datetime.now(),
        }
