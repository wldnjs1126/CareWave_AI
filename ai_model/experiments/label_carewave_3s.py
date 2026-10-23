"""Assign *provisional* action labels to CareWave 3-second feature windows.

The action-change times were APPROXIMATELY confirmed by the operator:
2026-10-20 11:01:12 standing -> walking
2026-10-20 11:02:37 walking -> sitting

We leave 15-second buffers before/after both transitions and trim the
beginning/end where session setup or leaving appears likely. Every labeled
window must be fully contained inside a safe interval. Labels are NOT
independent ground truth and must not be used as evidence of model quality.

Usage (Windows / VS Code terminal):
    python label_carewave_3s.py carewave_features_3s_unlabeled.csv
"""
import argparse
import csv
from collections import Counter
from datetime import datetime
from pathlib import Path

# The conservative windows are hypotheses based on approximate action times.
# Re-record with accurate action logs before external validation.
SAFE_PHASES = [
    ("STANDING", "2026-10-20 10:59:30", "2026-10-20 11:00:57"),
    ("WALKING",  "2026-10-20 11:01:27", "2026-10-20 11:02:22"),
    ("SITTING",  "2026-10-20 11:02:52", "2026-10-20 11:03:45"),
]


def assign_label(row):
    start = datetime.fromisoformat(row["start_time"])
    end = datetime.fromisoformat(row["end_time"])
    if end <= start:
        raise ValueError("Invalid feature window interval")
    for label, safe_start, safe_end in SAFE_PHASES:
        if start >= datetime.fromisoformat(safe_start) and end <= datetime.fromisoformat(safe_end):
            return label
    return ""


def build(source: Path, reviewed_path: Path, training_path: Path):
    with source.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header")
        headers = list(reader.fieldnames)
        if not {"start_time", "end_time", "activity_label"}.issubset(headers):
            raise ValueError("Expected start_time, end_time, activity_label columns")
        records = list(reader)

    output_headers = headers + ["label_status", "label_source"]
    for row in records:
        if row["activity_label"].strip():
            raise ValueError("Refusing to overwrite existing labels")
        row["activity_label"] = assign_label(row)
        row["label_status"] = "PROVISIONAL" if row["activity_label"] else "EXCLUDED"
        row["label_source"] = "approximate_user_confirmed_times" if row["activity_label"] else "excluded_transition_or_edge"

    with reviewed_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(records)

    eligible = [row for row in records if row["activity_label"]]
    with training_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(eligible)

    counts = Counter(row["activity_label"] or "EXCLUDED" for row in records)
    print(f"Reviewed: {len(records)} windows -> {reviewed_path}")
    print(f"Provisional training: {len(eligible)} windows -> {training_path}")
    for key in ("STANDING", "WALKING", "SITTING", "EXCLUDED"):
        print(f"{key}: {counts[key]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_csv", type=Path)
    parser.add_argument("--reviewed", type=Path, default=Path("carewave_features_3s_reviewed.csv"))
    parser.add_argument("--training", type=Path, default=Path("carewave_training_3activity_provisional.csv"))
    args = parser.parse_args()
    build(args.source_csv, args.reviewed, args.training)


if __name__ == "__main__":
    main()
