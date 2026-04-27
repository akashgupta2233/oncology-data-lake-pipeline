from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


DIABETES_RELATED_TESTS = {"GLUCOSE_FASTING", "HBA1C"}
ONCOLOGY_SIGNIFICANCE = {"Pathogenic", "Likely Pathogenic"}
ICD_PREFIX_PATTERN = re.compile(r"^([A-Z])(\d{2})([A-Z]?)$")


def load_gene_reference(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_lab_ranges(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_icd10_chapters(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _parse_icd_prefix(value: str) -> tuple[str, int, str] | None:
    normalized = re.sub(r"[^A-Z0-9]", "", value.upper())[:3]
    match = ICD_PREFIX_PATTERN.match(normalized)
    if not match:
        return None
    letter, number, suffix = match.groups()
    return letter, int(number), suffix or ""


def _icd_in_range(code: str, range_start: str, range_end: str) -> bool:
    code_tuple = _parse_icd_prefix(code)
    start_tuple = _parse_icd_prefix(range_start)
    end_tuple = _parse_icd_prefix(range_end)
    if code_tuple is None or start_tuple is None or end_tuple is None:
        return False
    return start_tuple <= code_tuple <= end_tuple


def map_icd10_chapters(diagnoses: pd.DataFrame, icd10_chapters: pd.DataFrame) -> pd.DataFrame:
    mapped = diagnoses.copy()
    mapped["chapter_name"] = "Unmapped"
    for row in icd10_chapters.itertuples(index=False):
        start_code, end_code = row.code_range.split("-")
        mask = mapped["icd10_code"].astype("string").apply(
            lambda value: _icd_in_range(str(value), start_code, end_code) if pd.notna(value) else False
        )
        mapped.loc[mask, "chapter_name"] = row.chapter_name
    return mapped


def build_risk_profiles(
    diagnoses: pd.DataFrame,
    lab_results: pd.DataFrame,
    genomics_variants: pd.DataFrame,
    gene_reference: dict[str, Any],
    icd10_chapters: pd.DataFrame,
    lab_test_ranges: dict[str, Any],
) -> dict[str, Any]:
    mapped_diagnoses = map_icd10_chapters(diagnoses, icd10_chapters)

    diabetes_thresholds = {
        test_name.upper(): config["critical_high"]
        for test_name, config in lab_test_ranges.items()
        if test_name.upper() in DIABETES_RELATED_TESTS
    }
    diabetes_labs = lab_results.loc[lab_results["test_name"].isin(diabetes_thresholds)].copy()
    diabetes_labs["critical_high"] = diabetes_labs["test_name"].map(diabetes_thresholds)
    diabetes_high_risk = diabetes_labs.loc[
        diabetes_labs["test_value"] > diabetes_labs["critical_high"]
    ].copy()

    oncology_variants = genomics_variants.loc[
        genomics_variants["clinical_significance"].isin(ONCOLOGY_SIGNIFICANCE)
        & genomics_variants["gene"].isin(gene_reference.keys())
    ].copy()
    oncology_variants["associated_cancers"] = oncology_variants["gene"].map(
        lambda gene: gene_reference.get(gene, {}).get("associated_cancers", [])
    )

    patient_chapters = (
        mapped_diagnoses.groupby("patient_id", dropna=False)["chapter_name"]
        .agg(lambda values: sorted({str(value) for value in values if pd.notna(value)}))
        .reset_index()
    )

    return {
        "icd10_mapping": {
            "chapter_distribution": {
                str(key): int(value)
                for key, value in mapped_diagnoses["chapter_name"].value_counts().sort_index().items()
            },
            "patient_chapters": patient_chapters.to_dict(orient="records"),
        },
        "high_risk_diabetes": {
            "rule": (
                "Patient has at least one diabetes-related lab result above the configured "
                "critical high threshold in lab_test_ranges.json."
            ),
            "affected_patient_count": int(diabetes_high_risk["patient_id"].nunique()),
            "patients": (
                diabetes_high_risk.groupby("patient_id", dropna=False)
                .agg(
                    triggered_tests=("test_name", lambda values: sorted(set(values))),
                    max_observed_value=("test_value", "max"),
                )
                .reset_index()
                .to_dict(orient="records")
            ),
        },
        "high_risk_oncology": {
            "rule": (
                "Patient has at least one reliable genomic variant marked Pathogenic or "
                "Likely Pathogenic in a gene tracked by gene_reference.json."
            ),
            "affected_patient_count": int(oncology_variants["patient_id"].nunique()),
            "patients": (
                oncology_variants.groupby("patient_id", dropna=False)
                .agg(
                    pathogenic_genes=("gene", lambda values: sorted(set(values))),
                    variant_count=("variant_id", "nunique"),
                    associated_cancers=(
                        "associated_cancers",
                        lambda values: sorted({item for sublist in values for item in sublist}),
                    ),
                )
                .reset_index()
                .to_dict(orient="records")
            ),
        },
    }
