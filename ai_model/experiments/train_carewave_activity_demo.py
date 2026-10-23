"""Train a *demonstration only* Random Forest for 3 CareWave actions.

This CSV comes from a SINGLE session using approximate transition times.
Never use this fit for automated safety alerts or as a validated metric.

Run:
    pip install pandas scikit-learn joblib
    python train_carewave_activity_demo.py carewave_training_3activity_provisional.csv

The production 7-event engine (e.g. FALL, INACTIVITY, PRESENCE) is separate.
"""
import argparse
from pathlib import Path
from collections import Counter
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer

FEATURES = [
    "avg_speed_mean_3s", "avg_speed_std_3s", "max_speed_3s",
    "position_xy_displacement_2s", "position_xy_path_2s",
    "height_metric_mean_3s", "height_metric_range_3s",
    "min_velocity_z_3s", "min_acceleration_z_3s",
]
ALLOWED_LABELS = {"STANDING", "WALKING", "SITTING"}


def train(input_csv: Path, output_model: Path):
    data = pd.read_csv(input_csv)
    needed = FEATURES + ["activity_label", "label_status", "recording_id", "start_time", "end_time"]
    missing = sorted(set(needed) - set(data.columns))
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if not data["label_status"].eq("PROVISIONAL").all():
        raise ValueError("Expected only PROVISIONAL rows in demonstration dataset")
    labels = set(data["activity_label"].dropna())
    if labels != ALLOWED_LABELS:
        raise ValueError(f"Need all three labels {ALLOWED_LABELS}, got {labels}")
    if data["recording_id"].nunique() != 1:
        print("Note: multiple recordings detected. Evaluate with group-disjoint tests separately.")

    X = data[FEATURES].apply(pd.to_numeric, errors="coerce")
    y = data["activity_label"]
    if X.isna().all(axis=0).any():
        raise ValueError("At least one input feature is entirely missing")
    print("Provisional class counts:", dict(Counter(y)))
    print("Warning: all rows are overlapping windows from one recording.")
    print("Fitting a pipeline only to verify the software works.")
    print("NO random split and NO test accuracy is reported.")

    model = make_pipeline(
        SimpleImputer(strategy="median"),
        RandomForestClassifier(
            n_estimators=150,
            min_samples_leaf=3,
            max_depth=6,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        ),
    )
    model.fit(X, y)
    joblib.dump({
        "model": model,
        "features": FEATURES,
        "status": "DEMO_ONLY_NOT_VALIDATED",
        "label_source": "approximate_user_confirmed_times",
        "source_recordings": sorted(data["recording_id"].unique().tolist()),
    }, output_model)
    print("Demo model saved:", output_model)
    print("Classes:", list(model.classes_))
    print("Do NOT deploy it to infer life_event risk levels.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--output", type=Path, default=Path("carewave_activity_rf_DEMO_ONLY.joblib"))
    args = parser.parse_args()
    train(args.input_csv, args.output)


if __name__ == "__main__":
    main()
