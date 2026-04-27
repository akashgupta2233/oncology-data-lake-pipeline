from __future__ import annotations

from typing import Any

import pandas as pd


def summarize_lab_statistics(
    lab_results: pd.DataFrame,
    lab_test_ranges: dict[str, Any],
) -> dict[str, Any]:
    numeric_labs = lab_results.dropna(subset=["test_name", "test_value"]).copy()
    range_lookup = {key.upper(): value for key, value in lab_test_ranges.items()}

    summary: dict[str, Any] = {}
    for test_name, test_frame in numeric_labs.groupby("test_name", dropna=False):
        range_config = range_lookup.get(str(test_name))
        valid_frame = test_frame
        excluded_row_count = 0

        if range_config is not None:
            valid_mask = test_frame["test_value"].between(
                range_config["critical_low"],
                range_config["critical_high"],
                inclusive="both",
            )
            excluded_row_count = int((~valid_mask).sum())
            valid_frame = test_frame.loc[valid_mask]

        values = valid_frame["test_value"]
        if values.empty:
            continue
        summary[str(test_name)] = {
            "row_count": int(test_frame["test_value"].count()),
            "valid_row_count": int(values.count()),
            "excluded_row_count": excluded_row_count,
            "mean": round(float(values.mean()), 4),
            "median": round(float(values.median()), 4),
            "std_dev": round(float(values.std(ddof=1)), 4)
            if values.count() > 1
            else 0.0,
            "percentile_10": round(float(values.quantile(0.10)), 4),
            "percentile_90": round(float(values.quantile(0.90)), 4),
        }
    return summary
