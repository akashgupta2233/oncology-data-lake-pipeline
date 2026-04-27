# Oncology Data Lake Pipeline

A production-ready Python-based ETL pipeline for processing multi-source clinical data, including patient demographics, laboratory results, diagnoses, medications, and genomics variants. The pipeline implements a three-tier data lake architecture (Raw → Refined → Consumption) with comprehensive data quality controls, anomaly detection, and analytical visualizations.

---

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Design Decisions](#design-decisions)
- [Data Lake Structure](#data-lake-structure)
- [Pipeline Stages](#pipeline-stages)
- [Visualizations](#visualizations)
- [Development](#development)
- [CI/CD](#cicd)
- [Project Structure](#project-structure)

---

## ✨ Features

- **Multi-Source Data Integration**: Harmonizes data from CSV, JSON, and Parquet formats
- **Three-Tier Data Lake**: Raw → Refined → Consumption architecture
- **Data Quality Framework**: Automated duplicate detection, encoding fixes, and null handling
- **Genomics Quality Control**: Reliable variant calling with configurable thresholds
- **Partitioned Storage**: Year-based partitioning for efficient lab result queries
- **Anomaly Detection**: Rule-based flagging of impossible or suspicious values
- **Risk Profiling**: ICD-10 chapter mapping and genomics-based risk assessment
- **Analytical Visualizations**: Six publication-quality plots (300 DPI)
- **Containerized Deployment**: Docker and Docker Compose support
- **CI/CD Pipeline**: Automated linting and Docker build verification

---

## 🏗️ Architecture

### Data Lake Zones

```
datalake/
├── raw/                    # Zone 1: Immutable source data mirror
│   ├── site_alpha_patients.csv
│   ├── site_beta_patients.json
│   ├── site_gamma_lab_results.parquet
│   ├── diagnoses_icd10.csv
│   ├── medications_log.json
│   ├── genomics_variants.parquet
│   ├── clinical_notes_metadata.csv
│   ├── reference/
│   └── manifest.json
│
├── refined/                # Zone 2: Cleaned, standardized, partitioned data
│   ├── patients.parquet
│   ├── patient_master.parquet
│   ├── diagnoses.parquet
│   ├── medications.parquet
│   ├── genomics_variants.parquet
│   ├── clinical_notes_metadata.parquet
│   ├── lab_results/        # Partitioned by collection_year
│   │   ├── collection_year=2022/
│   │   ├── collection_year=2023/
│   │   ├── collection_year=2024/
│   │   ├── collection_year=2025/
│   │   └── collection_year=unknown/
│   └── manifest.json
│
└── consumption/            # Zone 3: Analytics-ready outputs
    ├── analytics_summary.json
    └── plots/
        ├── 01_age_distribution.png
        ├── 02_diagnosis_frequency.png
        ├── 03_lab_distribution.png
        ├── 04_genomics_quality.png
        ├── 05_site_comparison.png
        ├── 06_clinical_correlation.png
        └── plots_README.md
```

### Pipeline Flow

```
┌─────────────────┐
│  Raw Data       │
│  (CSV/JSON/     │
│   Parquet)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ingestion      │
│  - Load sources │
│  - Mirror to    │
│    raw zone     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Cleaning       │
│  - Normalize    │
│  - Deduplicate  │
│  - Parse dates  │
│  - Fix encoding │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transformation │
│  - Integrate    │
│  - Partition    │
│  - Build master │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Analytics      │
│  - Demographics │
│  - Lab stats    │
│  - Risk profiles│
│  - Anomalies    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Visualization  │
│  - 6 PNG plots  │
│  - 300 DPI      │
└─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed
- OR **Python 3.12+** with pip

### Option 1: Run with Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/akashgupta2233/oncology-data-lake-pipeline
cd clinical-data-pipeline

# Run the pipeline
docker-compose up --build

# Outputs will be generated in:
# - datalake/ (all zones)
# - data_quality_report.json
```

The pipeline will:
1. Build the Docker image
2. Execute the complete ETL workflow
3. Generate all outputs in mounted volumes
4. Exit automatically when complete

### Option 2: Run Locally

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python pipeline/main.py

# Outputs will be generated in:
# - datalake/
# - data_quality_report.json
```

### Verify Outputs

After running the pipeline, check:

```bash
# Data lake structure
ls -R datalake/

# Quality report
cat data_quality_report.json

# Visualizations
ls datalake/consumption/plots/
```

---

## 🎯 Design Decisions

### 1. Reliable Call Definition (Genomics Quality Control)

**Problem**: Genomic variant calls vary in quality. Low-depth or extreme allele frequency variants may be sequencing artifacts.

**Solution**: Implemented a "Reliable Call" filter with the following criteria:

```python
RELIABLE_CALL_DEFINITION = {
    "rule_version": "v1",
    "criteria": {
        "read_depth_min": 30,              # Minimum coverage for confidence
        "allele_frequency_min": 0.05,      # Exclude rare artifacts
        "allele_frequency_max": 0.95,      # Exclude homozygous reference errors
        "required_fields": [                # Must have complete annotations
            "patient_id", "gene", "chromosome", "position",
            "ref_allele", "alt_allele", "variant_type",
            "clinical_significance", "sample_date"
        ],
        "valid_chromosomes": [              # Standard human chromosomes only
            "chr1", ..., "chr22", "chrX", "chrY", "chrM"
        ]
    }
}
```

**Rationale**:
- **Read Depth ≥ 30**: Industry standard for high-confidence variant calling
- **VAF 0.05-0.95**: Filters out sequencing noise and reference bias
- **Complete Annotations**: Ensures clinical interpretability
- **Valid Chromosomes**: Excludes contigs and unplaced scaffolds

**Impact**: Reduces false positives in downstream genomics analysis and risk profiling.

---

### 2. Lab Results Partitioning Strategy

**Problem**: Lab results are frequently queried by time ranges (e.g., "all labs from 2024"). Without partitioning, every query scans the entire dataset.

**Solution**: Partition by `collection_year` using Hive-style directory structure:

```
lab_results/
├── collection_year=2022/lab_results.parquet
├── collection_year=2023/lab_results.parquet
├── collection_year=2024/lab_results.parquet
├── collection_year=2025/lab_results.parquet
└── collection_year=unknown/lab_results.parquet
```

**Rationale**:
- **Query Performance**: Time-bounded queries only read relevant partitions
- **S3 Compatibility**: Hive-style partitioning works with AWS Athena, Spark, Presto
- **Deterministic**: Consistent partition structure across pipeline runs
- **Handles Missing Dates**: Unknown year partition prevents data loss

**Why Not Site-Based Partitioning?**
- Lab data comes from a single site (Gamma), so site partitioning provides no query pruning benefit
- Year-based partitioning aligns with common analytical patterns (temporal trends, annual reports)

---

### 3. Anomaly Detection Rules

**Problem**: Clinical data may contain impossible or suspicious values that indicate data quality issues or require clinical review.

**Solution**: Implemented four rule-based anomaly detectors:

#### Rule 1: Age Out of Bounds
```python
Rule: Patient age must be between 0 and 120 years (inclusive)
Flags: Negative ages, ages > 120
Cause: Data entry errors, incorrect date formats
```

#### Rule 2: Discharge Before Admission
```python
Rule: Discharge date cannot occur before admission date
Flags: discharge_date < admission_date
Cause: Date field swaps, data entry errors
```

#### Rule 3: Negative Lab Values
```python
Rule: Lab measurements cannot be negative for supported clinical tests
Flags: test_value < 0
Cause: Data corruption, incorrect units, entry errors
```

#### Rule 4: Lab Outside Critical Bounds
```python
Rule: Lab values must fall within critical low/high bounds
Reference: lab_test_ranges.json (e.g., Glucose: 0-600 mg/dL)
Flags: Values outside physiologically plausible ranges
Cause: Instrument errors, transcription errors, extreme outliers
```

**Output**: All anomalies are logged in `analytics_summary.json` under the `anomalies` key with:
- Rule description
- Count of flagged records
- Full record details for investigation

**Rationale**:
- **Transparency**: Anomalies are flagged but not removed, preserving data lineage
- **Clinical Review**: Provides actionable lists for data stewards
- **Quality Metrics**: Anomaly counts serve as data quality KPIs

---

### 4. Patient Master Table Design

**Problem**: Clinical data is fragmented across multiple sources. Analysts need a single unified view of each patient.

**Solution**: Built a `patient_master.parquet` table that joins:
- Patient demographics (from unified patient table)
- Lab result summaries (count, distinct tests, latest date)
- Diagnosis summaries (count, primary diagnoses, ICD codes)
- Medication summaries (count, active meds, latest start date)
- Genomics summaries (reliable variant count, pathogenic variants, genes)

**Schema**:
```
patient_id, first_name, last_name, date_of_birth, sex, site,
lab_result_count, distinct_lab_tests, avg_lab_value, latest_lab_date,
diagnosis_count, primary_diagnosis_count, diagnosis_codes,
medication_count, active_medication_count, medications,
reliable_variant_count, pathogenic_variant_count, genes_detected,
has_patient_demographics
```

**Rationale**:
- **Single Source of Truth**: One table for patient-level analytics
- **Pre-Aggregated**: Avoids expensive joins in downstream queries
- **Completeness Flag**: `has_patient_demographics` indicates data quality

---

## 📊 Data Lake Structure

### Zone 1: Raw (Immutable Source Mirror)

**Purpose**: Preserve original data exactly as received

**Characteristics**:
- Immutable (never modified after ingestion)
- Mirrors source file formats (CSV, JSON, Parquet)
- Includes manifest.json with ingestion timestamp
- Serves as audit trail and disaster recovery baseline

**Files**:
- `site_alpha_patients.csv` (350 patients)
- `site_beta_patients.json` (300 patients)
- `site_gamma_lab_results.parquet` (2,024 lab results)
- `diagnoses_icd10.csv` (1,628 diagnoses)
- `medications_log.json` (1,899 medications)
- `genomics_variants.parquet` (779 reliable variants)
- `clinical_notes_metadata.csv` (1,119 notes)
- `reference/` (ICD-10 chapters, gene reference, lab ranges)

---

### Zone 2: Refined (Cleaned & Standardized)

**Purpose**: Production-ready data for analytics and ML

**Characteristics**:
- Standardized schemas across sources
- Deduplicated and cleaned
- Parquet format for performance
- Partitioned where beneficial (lab_results)
- Sorted by primary keys

**Transformations Applied**:
- ✅ Text normalization (NFKC, whitespace, null-like strings)
- ✅ Encoding repair (mojibake detection and fix)
- ✅ Date parsing (multiple formats: MM/DD/YYYY, DD-MM-YYYY, ISO 8601)
- ✅ Duplicate resolution (keep most complete record)
- ✅ Schema harmonization (Alpha CSV + Beta JSON → unified patient table)
- ✅ Genomics quality filtering (reliable call definition)

**Files**:
- `patients.parquet` (650 unified patients)
- `patient_master.parquet` (1,867 patient records with aggregations)
- `diagnoses.parquet` (1,628 diagnoses)
- `medications.parquet` (1,899 medications)
- `genomics_variants.parquet` (779 reliable variants)
- `clinical_notes_metadata.parquet` (1,119 notes)
- `lab_results/` (2,024 results, partitioned by year)

---

### Zone 3: Consumption (Analytics-Ready Outputs)

**Purpose**: Pre-computed analytics and visualizations for end users

**Characteristics**:
- JSON summaries for dashboards and APIs
- PNG visualizations for reports
- Pre-aggregated statistics
- Human-readable formats

**Files**:

#### `analytics_summary.json`
Contains:
- **Demographics**: Total patients, age distribution, gender/site breakdown
- **Lab Statistics**: Mean, median, std dev, percentiles for 16 lab tests
- **Risk Profiles**: ICD-10 chapter distribution, patient-level chapter mappings
- **Anomalies**: Flagged records by rule type
- **Source Summary**: Row counts for all refined tables

#### `plots/` (Six Visualizations)
1. **01_age_distribution.png**: Histogram of patient ages
2. **02_diagnosis_frequency.png**: Bar chart of ICD-10 chapters
3. **03_lab_distribution.png**: Boxplots of key lab tests
4. **04_genomics_quality.png**: Scatter plot (VAF vs Read Depth)
5. **05_site_comparison.png**: Patient distribution by hospital site
6. **06_clinical_correlation.png**: Heatmap (Age vs Lab Values)

See `datalake/consumption/plots/plots_README.md` for detailed descriptions.

---

## 🔄 Pipeline Stages

### Stage 1: Ingestion
**Module**: `pipeline/ingestion/loaders.py`

- Loads all source files from `data/` directory
- Handles CSV, JSON, and Parquet formats
- Mirrors raw data to `datalake/raw/` (immutable copy)
- Generates manifest with ingestion timestamp

### Stage 2: Cleaning
**Module**: `pipeline/cleaning/quality.py`

- **Text Normalization**: NFKC normalization, whitespace cleanup
- **Encoding Repair**: Detects and fixes mojibake (e.g., "Ã©" → "é")
- **Date Parsing**: Handles MM/DD/YYYY, DD-MM-YYYY, ISO 8601 formats
- **Duplicate Resolution**: Keeps record with highest completeness score
- **Schema Standardization**: Harmonizes Alpha (CSV) and Beta (JSON) patients
- **Genomics Filtering**: Applies reliable call definition

**Audit Logging**: Each cleaning step emits a JSON audit log:
```json
{
  "dataset": "site_alpha_patients",
  "rows_in": 370,
  "rows_out": 350,
  "issues_found": {
    "duplicates": 20,
    "nulls": 31,
    "encoding": 0
  },
  "processing_timestamp": "2026-04-27T01:44:33+00:00"
}
```

### Stage 3: Transformation
**Module**: `pipeline/transformation/`

- **Patient Master**: Joins demographics with lab/diagnosis/medication/genomics summaries
- **Lab Partitioning**: Partitions lab results by collection year
- **Risk Profiling**: Maps diagnoses to ICD-10 chapters, identifies pathogenic variants
- **Storage**: Writes refined Parquet files with deterministic sorting

### Stage 4: Analytics
**Module**: `pipeline/stats/`

- **Demographics Summary**: Age distribution, gender/site breakdown
- **Lab Statistics**: Mean, median, std dev, percentiles (with outlier exclusion)
- **Risk Profiles**: ICD-10 chapter distribution, genomics risk assessment
- **Anomaly Detection**: Flags impossible/suspicious values

### Stage 5: Visualization
**Module**: `pipeline/stats/visualizations.py`

- Generates six publication-quality PNG plots (300 DPI)
- Uses matplotlib and seaborn with professional styling
- Saves to `datalake/consumption/plots/`

---

## 📈 Visualizations

All visualizations are automatically generated and saved to `datalake/consumption/plots/`.

### 1. Patient Age Distribution
- **Type**: Histogram (30 bins)
- **Features**: Mean and median lines
- **Insights**: Demographic spread of cohort

### 2. Diagnosis Frequency by ICD-10 Chapter
- **Type**: Horizontal bar chart
- **Features**: Sorted by frequency, value labels
- **Insights**: Most common disease categories

### 3. Lab Result Distribution
- **Type**: Boxplots
- **Features**: 6 key tests (Glucose, HbA1c, Cholesterol, Creatinine, Hemoglobin, TSH)
- **Insights**: Typical ranges and variability

### 4. Genomics Quality: VAF vs Read Depth
- **Type**: Scatter plot
- **Features**: Pathogenic variants highlighted, quality thresholds
- **Insights**: Sequencing quality assessment

### 5. Site Comparison
- **Type**: Vertical bar chart
- **Features**: Counts and percentages
- **Insights**: Patient distribution across hospitals

### 6. Clinical Correlation
- **Type**: Correlation heatmap
- **Features**: Age vs lab values, annotated coefficients
- **Insights**: Age-related patterns in lab results

See `datalake/consumption/plots/plots_README.md` for full documentation.

---

## 🛠️ Development

### Local Setup

```bash
# Clone repository
git clone https://github.com/akashgupta2233/oncology-data-lake-pipeline
cd clinical-data-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python pipeline/main.py
```

### Code Quality

```bash
# Run linter
ruff check .

# Run formatter
ruff format .

# Check formatting without changes
ruff format --check .
```

### Project Dependencies

```
pandas          # Data manipulation
pyarrow         # Parquet I/O
fastparquet     # Alternative Parquet engine
matplotlib      # Plotting
seaborn         # Statistical visualizations
ruff            # Linting and formatting
```

---

## 🔄 CI/CD

### GitHub Actions Workflow

**File**: `.github/workflows/ci.yml`

**Triggers**:
- Push to `main` branch
- Pull requests to `main`

**Jobs**:

#### 1. Code Quality Check
- Sets up Python 3.12
- Installs dependencies
- Runs `ruff check .` (linting)
- Runs `ruff format --check .` (formatting)

#### 2. Docker Build Verification
- Sets up Docker Buildx
- Builds Docker image
- Verifies image creation

**Status Badge**: 
```markdown
[![CI](https://github.com/akashgupta2233/oncology-data-lake-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/akashgupta2233/oncology-data-lake-pipeline/actions/workflows/ci.yml)
```

---

## 📁 Project Structure

```
clinical-data-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                  # CI/CD pipeline
├── data/                           # Source data (not in repo)
│   ├── site_alpha_patients.csv
│   ├── site_beta_patients.json
│   ├── site_gamma_lab_results.parquet
│   ├── diagnoses_icd10.csv
│   ├── medications_log.json
│   ├── genomics_variants.parquet
│   ├── clinical_notes_metadata.csv
│   └── reference/
│       ├── gene_reference.json
│       ├── icd10_chapters.csv
│       └── lab_test_ranges.json
├── datalake/                       # Generated outputs (gitignored)
│   ├── raw/
│   ├── refined/
│   └── consumption/
├── pipeline/
│   ├── cleaning/
│   │   ├── anomalies.py           # Anomaly detection rules
│   │   ├── quality.py             # Data cleaning and standardization
│   │   └── __init__.py
│   ├── ingestion/
│   │   ├── loaders.py             # Source data loading
│   │   └── __init__.py
│   ├── stats/
│   │   ├── demographics.py        # Demographics summaries
│   │   ├── labs.py                # Lab statistics
│   │   ├── visualizations.py      # Plot generation
│   │   └── __init__.py
│   ├── transformation/
│   │   ├── integrate.py           # Patient master table
│   │   ├── risk_profiles.py       # Risk profiling logic
│   │   ├── storage.py             # Partitioning and storage
│   │   └── __init__.py
│   ├── utils/
│   │   ├── io.py                  # File I/O utilities
│   │   ├── metadata.py            # Manifest generation
│   │   ├── reporting.py           # Audit logging
│   │   └── __init__.py
│   ├── main.py                    # Pipeline entry point
│   └── __init__.py
├── .dockerignore                   # Docker build exclusions
├── .gitignore                      # Git exclusions
├── docker-compose.yml              # Docker Compose configuration
├── Dockerfile                      # Docker image definition
├── README.md                       # This file
├── requirements.txt                # Python dependencies
└── data_quality_report.json        # Generated quality report
```

---

## 📝 Notes

### Relative Paths
All file paths in the pipeline use relative references. No hardcoded absolute paths (e.g., `C:\` or `D:\`).

### Idempotency
The pipeline is idempotent. Running it multiple times produces the same outputs (overwrites previous results).

### Data Not Included
The `data/` directory is not included in the repository. Place your source files in `data/` before running the pipeline.

### Output Persistence
When using Docker Compose, outputs are persisted to the host via volume mounts:
- `./datalake:/app/datalake`
- `./data_quality_report.json:/app/data_quality_report.json`

---



