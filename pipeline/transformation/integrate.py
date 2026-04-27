from __future__ import annotations

from typing import Iterable

import pandas as pd

from pipeline.utils.reporting import DatasetAudit


def _pipe_join(values: Iterable[object]) -> str | None:
    cleaned = sorted({str(value) for value in values if pd.notna(value) and str(value).strip()})
    return " | ".join(cleaned) if cleaned else None


def aggregate_lab_results(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["patient_id"])
    summary = (
        frame.groupby("patient_id", dropna=False)
        .agg(
            lab_result_count=("lab_result_id", "nunique"),
            distinct_lab_tests=("test_name", "nunique"),
            avg_lab_value=("test_value", "mean"),
            latest_lab_date=("collection_date", "max"),
            lab_sites=("site", _pipe_join),
        )
        .reset_index()
    )
    return summary


def aggregate_diagnoses(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["patient_id"])
    summary = (
        frame.groupby("patient_id", dropna=False)
        .agg(
            diagnosis_count=("diagnosis_id", "nunique"),
            primary_diagnosis_count=("is_primary", lambda values: int(pd.Series(values).fillna(False).sum())),
            latest_diagnosis_date=("diagnosis_date", "max"),
            diagnosis_codes=("icd10_code", _pipe_join),
        )
        .reset_index()
    )
    summary["primary_diagnosis_count"] = summary["primary_diagnosis_count"].astype("Int64")
    return summary


def aggregate_medications(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["patient_id"])
    summary = (
        frame.groupby("patient_id", dropna=False)
        .agg(
            medication_count=("medication_id", "nunique"),
            active_medication_count=("status", lambda values: int(pd.Series(values).eq("active").sum())),
            latest_medication_start_date=("start_date", "max"),
            medications=("medication_name", _pipe_join),
        )
        .reset_index()
    )
    return summary


def aggregate_genomics(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["patient_id"])
    pathogenic_labels = {"Pathogenic", "Likely Pathogenic"}
    summary = (
        frame.groupby("patient_id", dropna=False)
        .agg(
            reliable_variant_count=("variant_id", "nunique"),
            pathogenic_variant_count=(
                "clinical_significance",
                lambda values: int(pd.Series(values).isin(pathogenic_labels).sum()),
            ),
            latest_genomics_sample_date=("sample_date", "max"),
            genes_detected=("gene", _pipe_join),
        )
        .reset_index()
    )
    return summary


def build_patient_master(
    patients: pd.DataFrame,
    lab_results: pd.DataFrame,
    diagnoses: pd.DataFrame,
    medications: pd.DataFrame,
    genomics: pd.DataFrame,
) -> tuple[pd.DataFrame, DatasetAudit]:
    patient_ids = sorted(
        set(patients["patient_id"])
        | set(lab_results["patient_id"])
        | set(diagnoses["patient_id"])
        | set(medications["patient_id"])
        | set(genomics["patient_id"])
    )

    patient_master = pd.DataFrame({"patient_id": patient_ids})
    patient_master = patient_master.merge(patients, on="patient_id", how="left")
    patient_master = patient_master.merge(aggregate_lab_results(lab_results), on="patient_id", how="left")
    patient_master = patient_master.merge(aggregate_diagnoses(diagnoses), on="patient_id", how="left")
    patient_master = patient_master.merge(
        aggregate_medications(medications), on="patient_id", how="left"
    )
    patient_master = patient_master.merge(aggregate_genomics(genomics), on="patient_id", how="left")
    count_columns = [
        "lab_result_count",
        "distinct_lab_tests",
        "diagnosis_count",
        "primary_diagnosis_count",
        "medication_count",
        "active_medication_count",
        "reliable_variant_count",
        "pathogenic_variant_count",
    ]
    for column in count_columns:
        if column in patient_master.columns:
            patient_master[column] = patient_master[column].astype("Int64")
    patient_master["has_patient_demographics"] = patient_master["source_dataset"].notna()
    patient_master = patient_master.sort_values("patient_id", kind="mergesort").reset_index(drop=True)

    audit = DatasetAudit(
        dataset="patient_master",
        rows_in=len(patient_ids),
        rows_out=len(patient_master),
        issues_found={
            "duplicates": 0,
            "nulls": int(patient_master.isna().sum().sum()),
            "encoding": 0,
        },
        metadata={
            "patients_without_demographics": int((~patient_master["has_patient_demographics"]).sum()),
        },
    )
    return patient_master, audit
