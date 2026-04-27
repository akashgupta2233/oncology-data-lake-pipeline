from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
REFERENCE_DIR = DATA_DIR / "reference"
RAW_DIR = PROJECT_ROOT / "datalake" / "raw"
REFINED_DIR = PROJECT_ROOT / "datalake" / "refined"
CONSUMPTION_DIR = PROJECT_ROOT / "datalake" / "consumption"
SUPPORTED_SUFFIXES = {".csv", ".json", ".parquet"}


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REFINED_DIR.mkdir(parents=True, exist_ok=True)
    CONSUMPTION_DIR.mkdir(parents=True, exist_ok=True)


def read_dataset(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        return pd.read_json(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path}")


def list_data_files(directory: Path) -> list[Path]:
    return sorted(
        path for path in directory.rglob("*") if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
    )


def deterministic_sort(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    sort_columns = [column for column in columns if column in frame.columns]
    if not sort_columns:
        return frame.reset_index(drop=True)
    return (
        frame.sort_values(by=sort_columns, kind="mergesort", na_position="last")
        .reset_index(drop=True)
    )


def write_parquet(frame: pd.DataFrame, output_path: Path, sort_by: Iterable[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    deterministic_sort(frame.copy(), sort_by).to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )


def replace_directory_contents(target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)


def mirror_raw_sources(source_dir: Path = DATA_DIR, target_dir: Path = RAW_DIR) -> list[Path]:
    copied_paths: list[Path] = []
    for source_path in list_data_files(source_dir):
        relative_path = source_path.relative_to(source_dir)
        destination_path = target_dir / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if not destination_path.exists():
            shutil.copy2(source_path, destination_path)
        copied_paths.append(destination_path)
    return copied_paths


def read_partitioned_parquet(directory: Path) -> pd.DataFrame:
    partitions = sorted(directory.rglob("*.parquet"))
    if not partitions:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(path) for path in partitions), ignore_index=True)
