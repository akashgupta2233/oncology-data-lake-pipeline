from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable

import pandas as pd

from pipeline.utils.reporting import DatasetAudit


PATIENT_SCHEMA = [
    "patient_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "sex",
    "blood_group",
    "admission_date",
    "discharge_date",
    "contact_phone",
    "contact_email",
    "site",
    "source_dataset",
]

RELIABLE_CALL_DEFINITION = {
    "rule_version": "v1",
    "description": (
        "Reliable calls are variants with sufficient depth, a non-extreme allele "
        "frequency, and complete core annotations."
    ),
    "criteria": {
        "read_depth_min": 30,
        "allele_frequency_min": 0.05,
        "allele_frequency_max": 0.95,
        "required_fields": [
            "patient_id",
            "gene",
            "chromosome",
            "position",
            "ref_allele",
            "alt_allele",
            "variant_type",
            "clinical_significance",
            "sample_date",
        ],
        "valid_chromosomes": [
            "chr1",
            "chr2",
            "chr3",
            "chr4",
            "chr5",
            "chr6",
            "chr7",
            "chr8",
            "chr9",
            "chr10",
            "chr11",
            "chr12",
            "chr13",
            "chr14",
            "chr15",
            "chr16",
            "chr17",
            "chr18",
            "chr19",
            "chr20",
            "chr21",
            "chr22",
            "chrX",
            "chrY",
            "chrM",
        ],
    },
}

_MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "â€™", "â€œ", "â€“", "�")
_NULL_LIKE_STRINGS = {"", "na", "n/a", "null", "none", "nan", "not available"}
_SLASH_DATE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_DAY_FIRST_DASH_DATE = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_ISO_DATE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}(?:T.*)?$")


def _normalize_text_value(value: Any) -> tuple[Any, int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NA, 0
    if not isinstance(value, str):
        return value, 0

    cleaned = unicodedata.normalize("NFKC", value).strip()
    encoding_fixes = 0
    if any(marker in cleaned for marker in _MOJIBAKE_MARKERS):
        try:
            repaired = cleaned.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            repaired = cleaned.replace("�", "")
        if repaired != cleaned:
            cleaned = repaired
            encoding_fixes = 1
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned.lower() in _NULL_LIKE_STRINGS:
        return pd.NA, encoding_fixes
    return cleaned, encoding_fixes


def normalize_text_columns(
    frame: pd.DataFrame, columns: Iterable[str] | None = None
) -> tuple[pd.DataFrame, int]:
    normalized = frame.copy()
    target_columns = list(columns) if columns is not None else list(normalized.columns)
    encoding_fixes = 0

    for column in target_columns:
        if column not in normalized.columns:
            continue
        values: list[Any] = []
        for value in normalized[column]:
            fixed_value, fixed_count = _normalize_text_value(value)
            values.append(fixed_value)
            encoding_fixes += fixed_count
        normalized[column] = values
    return normalized, encoding_fixes


def parse_date_series(
    series: pd.Series,
    *,
    slash_format: str = "mdy",
    dash_dayfirst: bool = False,
) -> pd.Series:
    def _parse(value: Any) -> pd.Timestamp:
        if value is None or pd.isna(value):
            return pd.NaT
        if isinstance(value, pd.Timestamp):
            return value.normalize()

        text = str(value).strip()
        if not text:
            return pd.NaT
        if _SLASH_DATE.match(text):
            format_string = "%m/%d/%Y" if slash_format == "mdy" else "%d/%m/%Y"
            return pd.to_datetime(text, format=format_string, errors="coerce")
        if _DAY_FIRST_DASH_DATE.match(text):
            format_string = "%d-%m-%Y" if dash_dayfirst else "%m-%d-%Y"
            return pd.to_datetime(text, format=format_string, errors="coerce")
        if _ISO_DATE.match(text):
            return pd.to_datetime(text, errors="coerce")
        return pd.to_datetime(text, errors="coerce", dayfirst=dash_dayfirst)

    parsed = series.apply(_parse)
    return pd.to_datetime(parsed, errors="coerce")


def normalize_boolean(series: pd.Series) -> pd.Series:
    true_values = {"y", "yes", "true", "1"}
    false_values = {"n", "no", "false", "0"}

    def _convert(value: Any) -> Any:
        if value is None or pd.isna(value):
            return pd.NA
        lowered = str(value).strip().lower()
        if lowered in true_values:
            return True
        if lowered in false_values:
            return False
        return pd.NA

    return series.apply(_convert).astype("boolean")


def resolve_duplicates(
    frame: pd.DataFrame, primary_key: str
) -> tuple[pd.DataFrame, int]:
    working = frame.copy()
    working["_completeness_score"] = working.notna().sum(axis=1)
    working = working.sort_values(
        by=[primary_key, "_completeness_score"],
        ascending=[True, False],
        kind="mergesort",
        na_position="last",
    )
    deduplicated = working.drop_duplicates(subset=[primary_key], keep="first").drop(
        columns="_completeness_score"
    )
    duplicates_removed = len(frame) - len(deduplicated)
    return deduplicated.reset_index(drop=True), duplicates_removed


def _build_audit(
    dataset: str,
    rows_in: int,
    cleaned_frame: pd.DataFrame,
    duplicates_removed: int,
    encoding_fixes: int,
    null_count: int,
    metadata: dict[str, Any] | None = None,
) -> DatasetAudit:
    return DatasetAudit(
        dataset=dataset,
        rows_in=rows_in,
        rows_out=len(cleaned_frame),
        issues_found={
            "duplicates": duplicates_removed,
            "nulls": null_count,
            "encoding": encoding_fixes,
        },
        metadata=metadata or {},
    )


def standardize_alpha_patients(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned = cleaned.rename(
        columns={
            "admission_dt": "admission_date",
            "discharge_dt": "discharge_date",
        }
    )
    cleaned["date_of_birth"] = parse_date_series(
        cleaned["date_of_birth"], slash_format="mdy"
    )
    cleaned["admission_date"] = parse_date_series(
        cleaned["admission_date"], slash_format="mdy"
    )
    cleaned["discharge_date"] = parse_date_series(
        cleaned["discharge_date"], slash_format="mdy"
    )
    null_count = int(cleaned.isna().sum().sum())
    cleaned["sex"] = cleaned["sex"].map({"M": "M", "F": "F"}).fillna("U")
    cleaned["blood_group"] = cleaned["blood_group"].fillna("Unknown")
    cleaned["contact_phone"] = cleaned["contact_phone"].fillna("Not Available")
    cleaned["contact_email"] = cleaned["contact_email"].fillna("Not Available")
    cleaned["source_dataset"] = "site_alpha_patients"
    cleaned = cleaned[PATIENT_SCHEMA]
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "patient_id")
    return cleaned, _build_audit(
        "site_alpha_patients",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )


def standardize_beta_patients(frame: pd.DataFrame) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    working = frame.copy()

    standardized = pd.DataFrame(
        {
            "patient_id": working["patientID"],
            "first_name": working["name"].apply(
                lambda value: value.get("given") if isinstance(value, dict) else pd.NA
            ),
            "last_name": working["name"].apply(
                lambda value: value.get("family") if isinstance(value, dict) else pd.NA
            ),
            "date_of_birth": working["birthDate"],
            "sex": working["gender"],
            "blood_group": working["bloodType"],
            "admission_date": working["encounter"].apply(
                lambda value: (
                    value.get("admissionDate") if isinstance(value, dict) else pd.NA
                )
            ),
            "discharge_date": working["encounter"].apply(
                lambda value: (
                    value.get("dischargeDate") if isinstance(value, dict) else pd.NA
                )
            ),
            "contact_phone": working["contact"].apply(
                lambda value: value.get("phone") if isinstance(value, dict) else pd.NA
            ),
            "contact_email": working["contact"].apply(
                lambda value: value.get("email") if isinstance(value, dict) else pd.NA
            ),
            "site": working["encounter"].apply(
                lambda value: (
                    value.get("facility") if isinstance(value, dict) else pd.NA
                )
            ),
        }
    )
    cleaned, encoding_fixes = normalize_text_columns(standardized)
    cleaned["date_of_birth"] = parse_date_series(cleaned["date_of_birth"])
    cleaned["admission_date"] = parse_date_series(
        cleaned["admission_date"], dash_dayfirst=True
    )
    cleaned["discharge_date"] = parse_date_series(
        cleaned["discharge_date"], dash_dayfirst=True
    )
    null_count = int(cleaned.isna().sum().sum())
    cleaned["sex"] = (
        cleaned["sex"]
        .astype("string")
        .str.lower()
        .map({"male": "M", "female": "F"})
        .fillna("U")
    )
    cleaned["blood_group"] = cleaned["blood_group"].fillna("Unknown")
    cleaned["contact_phone"] = cleaned["contact_phone"].fillna("Not Available")
    cleaned["contact_email"] = cleaned["contact_email"].fillna("Not Available")
    cleaned["source_dataset"] = "site_beta_patients"
    cleaned = cleaned[PATIENT_SCHEMA]
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "patient_id")
    return cleaned, _build_audit(
        "site_beta_patients",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )


def unify_patients(
    alpha: pd.DataFrame, beta: pd.DataFrame
) -> tuple[pd.DataFrame, DatasetAudit]:
    unified = pd.concat([alpha, beta], ignore_index=True)
    unified, duplicates_removed = resolve_duplicates(unified, "patient_id")
    audit = DatasetAudit(
        dataset="patients",
        rows_in=len(alpha) + len(beta),
        rows_out=len(unified),
        issues_found={
            "duplicates": duplicates_removed,
            "nulls": int(unified.isna().sum().sum()),
            "encoding": 0,
        },
    )
    return unified, audit


def clean_lab_results(frame: pd.DataFrame) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned = cleaned.rename(columns={"patient_ref": "patient_id", "site_name": "site"})
    cleaned["test_name"] = cleaned["test_name"].astype("string").str.upper()
    cleaned["test_value"] = pd.to_numeric(cleaned["test_value"], errors="coerce")
    cleaned["collection_date"] = parse_date_series(cleaned["collection_date"])
    null_count = int(cleaned.isna().sum().sum())
    cleaned["ordering_physician"] = cleaned["ordering_physician"].fillna("Unknown")
    cleaned["site"] = cleaned["site"].fillna("Unknown")
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "lab_result_id")
    return cleaned, _build_audit(
        "site_gamma_lab_results",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )


def clean_diagnoses(frame: pd.DataFrame) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned["diagnosis_date"] = parse_date_series(
        cleaned["diagnosis_date"], slash_format="mdy"
    )
    cleaned["is_primary"] = normalize_boolean(cleaned["is_primary"])
    null_count = int(cleaned.isna().sum().sum())
    cleaned["notes"] = cleaned["notes"].fillna("")
    cleaned["severity"] = cleaned["severity"].fillna("unknown")
    cleaned["status"] = cleaned["status"].fillna("unknown")
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "diagnosis_id")
    return cleaned, _build_audit(
        "diagnoses_icd10",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )


def clean_medications(frame: pd.DataFrame) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned["start_date"] = parse_date_series(cleaned["start_date"], slash_format="mdy")
    cleaned["end_date"] = parse_date_series(cleaned["end_date"], slash_format="mdy")
    null_count = int(cleaned.isna().sum().sum())
    cleaned["route"] = cleaned["route"].astype("string").str.lower()
    cleaned["frequency"] = cleaned["frequency"].astype("string").str.lower()
    cleaned["status"] = cleaned["status"].astype("string").str.lower().fillna("unknown")
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "medication_id")
    return cleaned, _build_audit(
        "medications_log",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )


def clean_genomics_variants(frame: pd.DataFrame) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned = cleaned.rename(columns={"patient_ref": "patient_id"})
    cleaned["allele_frequency"] = pd.to_numeric(
        cleaned["allele_frequency"], errors="coerce"
    )
    cleaned["read_depth"] = pd.to_numeric(cleaned["read_depth"], errors="coerce")
    cleaned["sample_date"] = parse_date_series(cleaned["sample_date"])
    null_count = int(cleaned.isna().sum().sum())
    cleaned["clinical_significance"] = cleaned["clinical_significance"].fillna(
        "Unknown"
    )
    cleaned["chromosome"] = cleaned["chromosome"].astype("string")
    cleaned["variant_type"] = cleaned["variant_type"].astype("string")
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "variant_id")

    required_fields = RELIABLE_CALL_DEFINITION["criteria"]["required_fields"]
    valid_chromosomes = set(RELIABLE_CALL_DEFINITION["criteria"]["valid_chromosomes"])
    reliable_mask = (
        cleaned[required_fields].notna().all(axis=1)
        & cleaned["read_depth"].ge(
            RELIABLE_CALL_DEFINITION["criteria"]["read_depth_min"]
        )
        & cleaned["allele_frequency"].between(
            RELIABLE_CALL_DEFINITION["criteria"]["allele_frequency_min"],
            RELIABLE_CALL_DEFINITION["criteria"]["allele_frequency_max"],
            inclusive="both",
        )
        & cleaned["chromosome"].isin(valid_chromosomes)
    )
    cleaned = cleaned.loc[reliable_mask].reset_index(drop=True)
    return cleaned, _build_audit(
        "genomics_variants",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
        metadata={"reliable_call_rule": RELIABLE_CALL_DEFINITION["rule_version"]},
    )


def clean_clinical_notes_metadata(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, DatasetAudit]:
    rows_in = len(frame)
    cleaned, encoding_fixes = normalize_text_columns(frame)
    cleaned["note_date"] = parse_date_series(cleaned["note_date"], slash_format="mdy")
    cleaned["is_addendum"] = normalize_boolean(cleaned["is_addendum"])
    null_count = int(cleaned.isna().sum().sum())
    cleaned, duplicates_removed = resolve_duplicates(cleaned, "note_id")
    return cleaned, _build_audit(
        "clinical_notes_metadata",
        rows_in,
        cleaned,
        duplicates_removed,
        encoding_fixes,
        null_count,
    )
