from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _age_series(date_of_birth: pd.Series) -> pd.Series:
    as_of_date = datetime.now(timezone.utc).date()
    ages = ((pd.Timestamp(as_of_date) - date_of_birth).dt.days / 365.25).astype("float64")
    return ages.where(date_of_birth.notna())


def summarize_demographics(patients: pd.DataFrame) -> dict[str, Any]:
    demographics = patients.loc[patients["has_patient_demographics"]].copy()
    demographics["age_years"] = _age_series(demographics["date_of_birth"])

    return {
        "total_patient_count": int(len(demographics)),
        "age_distribution": {
            "mean": round(float(demographics["age_years"].mean()), 2),
            "median": round(float(demographics["age_years"].median()), 2),
            "min": round(float(demographics["age_years"].min()), 2),
            "max": round(float(demographics["age_years"].max()), 2),
        },
        "gender_distribution": {
            str(key): int(value)
            for key, value in demographics["sex"].fillna("Unknown").value_counts().sort_index().items()
        },
        "site_distribution": {
            str(key): int(value)
            for key, value in demographics["site"].fillna("Unknown").value_counts().sort_index().items()
        },
    }
