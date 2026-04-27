import sys
import json
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipeline.cleaning import (
    RELIABLE_CALL_DEFINITION,
    clean_clinical_notes_metadata,
    clean_diagnoses,
    clean_genomics_variants,
    clean_lab_results,
    clean_medications,
    detect_anomalies,
    standardize_alpha_patients,
    standardize_beta_patients,
    unify_patients,
)
from pipeline.ingestion import load_data_sources, load_reference_sources
from pipeline.stats import (
    summarize_demographics,
    summarize_lab_statistics,
    generate_all_visualizations,
)
from pipeline.transformation import (
    build_patient_master,
    build_risk_profiles,
    load_gene_reference,
    load_icd10_chapters,
    load_lab_ranges,
    write_partitioned_lab_results,
)
from pipeline.utils.io import (
    CONSUMPTION_DIR,
    PROJECT_ROOT,
    RAW_DIR,
    REFINED_DIR,
    ensure_directories,
    mirror_raw_sources,
    read_partitioned_parquet,
    write_parquet,
)
from pipeline.utils.metadata import write_manifest
from pipeline.utils.reporting import (
    emit_dataset_log,
    to_jsonable,
    utc_timestamp,
    write_quality_report,
)


def main() -> None:
    ensure_directories()
    mirror_raw_sources()
    data_sources = load_data_sources()
    load_reference_sources()

    alpha_patients, alpha_audit = standardize_alpha_patients(
        data_sources["site_alpha_patients"]
    )
    beta_patients, beta_audit = standardize_beta_patients(
        data_sources["site_beta_patients"]
    )
    patients, patients_audit = unify_patients(alpha_patients, beta_patients)
    lab_results, lab_audit = clean_lab_results(data_sources["site_gamma_lab_results"])
    diagnoses, diagnoses_audit = clean_diagnoses(data_sources["diagnoses_icd10"])
    medications, medications_audit = clean_medications(data_sources["medications_log"])
    genomics, genomics_audit = clean_genomics_variants(
        data_sources["genomics_variants"]
    )
    clinical_notes, notes_audit = clean_clinical_notes_metadata(
        data_sources["clinical_notes_metadata"]
    )
    patient_master, patient_master_audit = build_patient_master(
        patients,
        lab_results,
        diagnoses,
        medications,
        genomics,
    )

    source_audits = [
        alpha_audit,
        beta_audit,
        lab_audit,
        diagnoses_audit,
        medications_audit,
        genomics_audit,
        notes_audit,
    ]
    for audit in [*source_audits, patients_audit, patient_master_audit]:
        emit_dataset_log(audit)

    write_parquet(patients, REFINED_DIR / "patients.parquet", sort_by=["patient_id"])
    write_parquet(
        diagnoses, REFINED_DIR / "diagnoses.parquet", sort_by=["diagnosis_id"]
    )
    write_parquet(
        medications, REFINED_DIR / "medications.parquet", sort_by=["medication_id"]
    )
    write_parquet(
        genomics, REFINED_DIR / "genomics_variants.parquet", sort_by=["variant_id"]
    )
    write_parquet(
        clinical_notes,
        REFINED_DIR / "clinical_notes_metadata.parquet",
        sort_by=["note_id"],
    )
    write_parquet(
        patient_master, REFINED_DIR / "patient_master.parquet", sort_by=["patient_id"]
    )
    write_partitioned_lab_results(lab_results, REFINED_DIR / "lab_results")

    legacy_lab_results_file = REFINED_DIR / "lab_results.parquet"
    if legacy_lab_results_file.exists():
        legacy_lab_results_file.unlink()

    write_quality_report(
        PROJECT_ROOT / "data_quality_report.json",
        source_audits,
        RELIABLE_CALL_DEFINITION,
    )

    refined_patients = pd.read_parquet(REFINED_DIR / "patients.parquet")
    refined_patient_master = pd.read_parquet(REFINED_DIR / "patient_master.parquet")
    refined_diagnoses = pd.read_parquet(REFINED_DIR / "diagnoses.parquet")
    refined_genomics = pd.read_parquet(REFINED_DIR / "genomics_variants.parquet")
    refined_labs = read_partitioned_parquet(REFINED_DIR / "lab_results")

    gene_reference = load_gene_reference(
        PROJECT_ROOT / "data" / "reference" / "gene_reference.json"
    )
    icd10_chapters = load_icd10_chapters(
        PROJECT_ROOT / "data" / "reference" / "icd10_chapters.csv"
    )
    lab_ranges = load_lab_ranges(
        PROJECT_ROOT / "data" / "reference" / "lab_test_ranges.json"
    )

    analytics_summary = {
        "generated_at": utc_timestamp(),
        "demographics": summarize_demographics(refined_patient_master),
        "lab_statistics": summarize_lab_statistics(refined_labs, lab_ranges),
        "risk_profiles": build_risk_profiles(
            diagnoses=refined_diagnoses,
            lab_results=refined_labs,
            genomics_variants=refined_genomics,
            gene_reference=gene_reference,
            icd10_chapters=icd10_chapters,
            lab_test_ranges=lab_ranges,
        ),
        "anomalies": detect_anomalies(refined_patient_master, refined_labs, lab_ranges),
        "source_summary": {
            "patients_rows": int(len(refined_patients)),
            "patient_master_rows": int(len(refined_patient_master)),
            "lab_result_rows": int(len(refined_labs)),
            "diagnosis_rows": int(len(refined_diagnoses)),
            "genomics_rows": int(len(refined_genomics)),
        },
    }
    (CONSUMPTION_DIR / "analytics_summary.json").write_text(
        json.dumps(analytics_summary, indent=2, default=to_jsonable),
        encoding="utf-8",
    )

    # Generate visualizations
    generate_all_visualizations(
        patient_master_path=REFINED_DIR / "patient_master.parquet",
        lab_results_path=REFINED_DIR / "lab_results",
        genomics_variants_path=REFINED_DIR / "genomics_variants.parquet",
        analytics_summary_path=CONSUMPTION_DIR / "analytics_summary.json",
        output_dir=CONSUMPTION_DIR / "plots",
    )

    manifest_timestamp = utc_timestamp()
    write_manifest(RAW_DIR, processing_timestamp=manifest_timestamp)
    write_manifest(REFINED_DIR, processing_timestamp=manifest_timestamp)


if __name__ == "__main__":
    main()
