from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def to_jsonable(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, dict):
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    return value


@dataclass
class DatasetAudit:
    dataset: str
    rows_in: int
    rows_out: int
    issues_found: dict[str, int]
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_log(self) -> dict[str, Any]:
        payload = {
            "dataset": self.dataset,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "issues_found": self.issues_found,
            "processing_timestamp": utc_timestamp(),
        }
        if self.metadata:
            payload["metadata"] = to_jsonable(self.metadata)
        return payload


def emit_dataset_log(audit: DatasetAudit) -> None:
    print(json.dumps(audit.as_log(), default=to_jsonable, sort_keys=True))


def write_quality_report(
    output_path: Path,
    audits: list[DatasetAudit],
    reliable_call_definition: dict[str, Any],
) -> None:
    totals = {
        "rows_in": sum(audit.rows_in for audit in audits),
        "rows_out": sum(audit.rows_out for audit in audits),
        "duplicates": sum(audit.issues_found.get("duplicates", 0) for audit in audits),
        "nulls": sum(audit.issues_found.get("nulls", 0) for audit in audits),
        "encoding": sum(audit.issues_found.get("encoding", 0) for audit in audits),
    }
    payload = {
        "generated_at": utc_timestamp(),
        "reliable_call_definition": reliable_call_definition,
        "datasets": [audit.as_log() for audit in audits],
        "totals": totals,
    }
    output_path.write_text(
        json.dumps(payload, indent=2, default=to_jsonable), encoding="utf-8"
    )
