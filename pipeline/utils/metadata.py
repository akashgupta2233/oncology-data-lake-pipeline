from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.utils.io import list_data_files, read_dataset
from pipeline.utils.reporting import to_jsonable, utc_timestamp


def sha256_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_schema(dataframe: pd.DataFrame | None = None, file_path: Path | None = None) -> list[dict[str, str]]:
    if dataframe is None and file_path is None:
        raise ValueError("Either dataframe or file_path must be provided.")
    frame = dataframe if dataframe is not None else read_dataset(file_path)  # type: ignore[arg-type]
    return [{"name": column, "dtype": str(dtype)} for column, dtype in frame.dtypes.items()]


def _file_manifest_entry(path: Path, zone_root: Path, processing_timestamp: str) -> dict[str, Any]:
    dataframe = read_dataset(path)
    return {
        "file_name": path.relative_to(zone_root).as_posix(),
        "row_count": int(len(dataframe)),
        "schema": extract_schema(dataframe=dataframe),
        "processing_timestamp": processing_timestamp,
        "sha256_checksum": sha256_checksum(path),
    }


def generate_manifest(zone_root: Path, *, processing_timestamp: str | None = None) -> dict[str, Any]:
    timestamp = processing_timestamp or utc_timestamp()
    files = [
        _file_manifest_entry(path, zone_root=zone_root, processing_timestamp=timestamp)
        for path in list_data_files(zone_root)
        if path.name != "manifest.json"
    ]
    return {
        "zone": zone_root.name,
        "generated_at": timestamp,
        "files": files,
    }


def write_manifest(zone_root: Path, *, processing_timestamp: str | None = None) -> Path:
    manifest_path = zone_root / "manifest.json"
    payload = generate_manifest(zone_root, processing_timestamp=processing_timestamp)
    manifest_path.write_text(json.dumps(payload, indent=2, default=to_jsonable), encoding="utf-8")
    return manifest_path
