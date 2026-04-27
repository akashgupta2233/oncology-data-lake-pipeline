from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.utils.io import DATA_DIR, REFERENCE_DIR, read_dataset


SOURCE_DATASET_NAMES = {
    "site_alpha_patients",
    "site_beta_patients",
    "site_gamma_lab_results",
    "diagnoses_icd10",
    "medications_log",
    "genomics_variants",
    "clinical_notes_metadata",
}


def _discover_supported_files(directory: Path) -> dict[str, Path]:
    return {
        path.stem: path
        for path in sorted(directory.iterdir())
        if path.is_file() and path.suffix.lower() in {".csv", ".json", ".parquet"}
    }


def load_data_sources() -> dict[str, pd.DataFrame]:
    files = _discover_supported_files(DATA_DIR)
    return {name: read_dataset(path) for name, path in files.items()}


def load_reference_sources() -> dict[str, pd.DataFrame]:
    files = _discover_supported_files(REFERENCE_DIR)
    return {name: read_dataset(path) for name, path in files.items()}
