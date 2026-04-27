from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _age_years(date_of_birth: pd.Series) -> pd.Series:
    as_of_date = datetime.now(timezone.utc).date()
    return ((pd.Timestamp(as_of_date) - date_of_birth).dt.days / 365.25).astype("float64")


def detect_anomalies(
    patients: pd.DataFrame,
    lab_results: pd.DataFrame,
    lab_test_ranges: dict[str, Any],
) -> dict[str, Any]:
    demographics = patients.loc[patients["has_patient_demographics"]].copy()
    demographics["age_years"] = _age_years(demographics["date_of_birth"])

    age_anomalies = demographics.loc[
        demographics["age_years"].lt(0) | demographics["age_years"].gt(120)
    ][["patient_id", "date_of_birth", "age_years", "site"]]

    discharge_before_admission = demographics.loc[
        demographics["admission_date"].notna()
        & demographics["discharge_date"].notna()
        & (demographics["discharge_date"] < demographics["admission_date"])
    ][["patient_id", "admission_date", "discharge_date", "site"]]

    impossible_lab_values = lab_results.loc[
        lab_results["test_value"].notna() & (lab_results["test_value"] < 0)
    ][["lab_result_id", "patient_id", "test_name", "test_value", "test_unit", "collection_date"]]

    lab_ranges = {key.upper(): value for key, value in lab_test_ranges.items()}
    lab_bounds_mask = lab_results.apply(
        lambda row: (
            row["test_name"] in lab_ranges
            and pd.notna(row["test_value"])
            and (
                row["test_value"] < lab_ranges[row["test_name"]]["critical_low"]
                or row["test_value"] > lab_ranges[row["test_name"]]["critical_high"]
            )
        ),
        axis=1,
    )
    outside_critical_bounds = lab_results.loc[lab_bounds_mask][
        ["lab_result_id", "patient_id", "test_name", "test_value", "test_unit", "collection_date"]
    ]

    unique_flagged_records = pd.concat(
        [
            impossible_lab_values[["lab_result_id"]].assign(record_type="lab"),
            outside_critical_bounds[["lab_result_id"]].assign(record_type="lab"),
            age_anomalies[["patient_id"]].rename(columns={"patient_id": "lab_result_id"}).assign(
                record_type="patient"
            ),
            discharge_before_admission[["patient_id"]]
            .rename(columns={"patient_id": "lab_result_id"})
            .assign(record_type="patient"),
        ],
        ignore_index=True,
    ).drop_duplicates()

    return {
        "rules": {
            "age_out_of_bounds": {
                "rule": "Patient age must be between 0 and 120 years, inclusive.",
                "count": int(len(age_anomalies)),
                "records": age_anomalies.to_dict(orient="records"),
            },
            "discharge_before_admission": {
                "rule": "Discharge date cannot occur before admission date.",
                "count": int(len(discharge_before_admission)),
                "records": discharge_before_admission.to_dict(orient="records"),
            },
            "negative_lab_value": {
                "rule": "Lab measurements cannot be negative for the supported clinical test set.",
                "count": int(len(impossible_lab_values)),
                "records": impossible_lab_values.to_dict(orient="records"),
            },
            "lab_outside_critical_bounds": {
                "rule": (
                    "Lab measurements must fall within the configured critical low/high "
                    "bounds in lab_test_ranges.json for supported assays."
                ),
                "count": int(len(outside_critical_bounds)),
                "records": outside_critical_bounds.to_dict(orient="records"),
            },
        },
        "total_flagged_records": int(len(unique_flagged_records)),
    }
