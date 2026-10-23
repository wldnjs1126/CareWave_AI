"""CareWave sensor_raw CSV -> unlabeled 3-second features.

Usage:
    python prepare_carewave_3s.py sensor_raw_test_30.csv
    python prepare_carewave_3s.py input.csv output.csv

Preserves actual CareWave measurements. Does not invent activity labels,
fill missing seconds, or treat height as physical body height.
"""
import argparse
import csv
import math
import statistics
from datetime import datetime, timedelta
from pathlib import Path

REQUIRED = [
    "window_start", "window_end", "sample_count", "presence",
    "avg_x", "avg_y", "avg_speed", "max_speed", "avg_height",
    "min_velocity_z", "min_acceleration_z",
]
FIELDS = [
    "recording_id", "start_time", "end_time", "n_seconds",
    "sample_count_3s", "presence_all_3s", "avg_speed_mean_3s",
    "avg_speed_median_3s", "avg_speed_std_3s", "max_speed_3s",
    "position_xy_displacement_2s", "position_xy_path_2s",
    "height_metric_mean_3s", "height_metric_change_first_last_3s",
    "height_metric_range_3s", "min_velocity_z_3s",
    "min_acceleration_z_3s", "activity_label",
]


def numeric(value):
    if value in (None, "", "\\N", "NULL", "null"):
        return None
    return float(value)


def optional_min(items):
    values = [v for v in items if v is not None]
    return min(values) if values else None


def round_or_blank(value, places=6):
    return "" if value is None else round(value, places)


def convert(input_path: Path, output_path: Path):
    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = [name for name in REQUIRED if name not in (reader.fieldnames or [])]
        if missing:
            raise ValueError("Missing columns: " + ", ".join(missing))
        rows = list(reader)

    rows.sort(key=lambda item: item["window_start"])
    output = []
    for i in range(len(rows) - 2):
        group = rows[i:i + 3]
        starts = [datetime.fromisoformat(r["window_start"]) for r in group]
        ends = [datetime.fromisoformat(r["window_end"]) for r in group]
        if any((ends[j] - starts[j]) != timedelta(seconds=1) for j in range(3)):
            continue
        if any(starts[j + 1] - starts[j] != timedelta(seconds=1) for j in range(2)):
            continue  # Never bridge a 4-second recording gap
        required_measures = ("avg_x", "avg_y", "avg_speed", "max_speed", "avg_height")
        if any(numeric(r[k]) is None for r in group for k in required_measures):
            continue  # Do not impute unobserved positions / speed

        x = [numeric(r["avg_x"]) for r in group]
        y = [numeric(r["avg_y"]) for r in group]
        speed = [numeric(r["avg_speed"]) for r in group]
        height = [numeric(r["avg_height"]) for r in group]
        dis = math.hypot(x[-1] - x[0], y[-1] - y[0])
        path = sum(math.hypot(x[j+1]-x[j], y[j+1]-y[j]) for j in range(2))
        item = {
            "recording_id": input_path.stem,
            "start_time": group[0]["window_start"],
            "end_time": group[-1]["window_end"],
            "n_seconds": 3,
            "sample_count_3s": sum(int(r["sample_count"]) for r in group),
            "presence_all_3s": int(all(int(r["presence"]) == 1 for r in group)),
            "avg_speed_mean_3s": round_or_blank(statistics.mean(speed)),
            "avg_speed_median_3s": round_or_blank(statistics.median(speed)),
            "avg_speed_std_3s": round_or_blank(statistics.pstdev(speed)),
            "max_speed_3s": round_or_blank(max(numeric(r["max_speed"]) for r in group)),
            "position_xy_displacement_2s": round_or_blank(dis),
            "position_xy_path_2s": round_or_blank(path),
            "height_metric_mean_3s": round_or_blank(statistics.mean(height)),
            "height_metric_change_first_last_3s": round_or_blank(height[0] - height[-1]),
            "height_metric_range_3s": round_or_blank(max(height) - min(height)),
            "min_velocity_z_3s": round_or_blank(optional_min(numeric(r["min_velocity_z"]) for r in group)),
            "min_acceleration_z_3s": round_or_blank(optional_min(numeric(r["min_acceleration_z"]) for r in group)),
            "activity_label": "",  # Fill only after actual action start/end times are confirmed
        }
        output.append(item)

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(output)
    print(f"Input: {len(rows)} rows. Valid 3-second windows: {len(output)}")
    print(f"Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_csv", type=Path, nargs="?")
    args = parser.parse_args()
    output = args.output_csv or args.input_csv.with_name("carewave_features_3s_unlabeled.csv")
    convert(args.input_csv, output)


if __name__ == "__main__":
    main()
