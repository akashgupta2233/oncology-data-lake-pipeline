from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.utils.io import deterministic_sort, replace_directory_contents, write_parquet


def write_partitioned_lab_results(frame: pd.DataFrame, output_dir: Path) -> None:
    # The lab dataset only contains one site, so partitioning by site would not prune scans.
    # Partitioning by collection year supports the common query pattern of time-bounded lab
    # analysis while keeping the local lake layout S3-style and deterministic across reruns.
    partitioned = frame.copy()
    partitioned["collection_year"] = (
        partitioned["collection_date"].dt.year.astype("Int64").astype("string").fillna("unknown")
    )
    partitioned = deterministic_sort(
        partitioned,
        ["collection_year", "collection_date", "lab_result_id"],
    )

    replace_directory_contents(output_dir)
    for collection_year, partition_frame in partitioned.groupby("collection_year", sort=True, dropna=False):
        partition_path = output_dir / f"collection_year={collection_year}" / "lab_results.parquet"
        write_parquet(
            partition_frame.reset_index(drop=True),
            partition_path,
            sort_by=["collection_date", "lab_result_id"],
        )
