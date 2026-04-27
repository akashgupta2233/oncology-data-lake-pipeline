"""
Visualization module for clinical data analytics.

This module generates six static PNG plots for the clinical data pipeline:
1. Patient Age Distribution (histogram)
2. Diagnosis Frequency by ICD-10 Chapter (bar chart)
3. Lab Result Distribution (boxplots)
4. Genomics Quality (scatter plot)
5. Site Comparison (bar chart)
6. Clinical Correlation (heatmap)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def _age_series(date_of_birth: pd.Series) -> pd.Series:
    """Calculate age in years from date of birth."""
    as_of_date = datetime.now(timezone.utc).date()
    ages = ((pd.Timestamp(as_of_date) - date_of_birth).dt.days / 365.25).astype("float64")
    return ages.where(date_of_birth.notna())


def plot_age_distribution(patient_master: pd.DataFrame, output_path: Path) -> None:
    """
    Generate a histogram showing patient age distribution.
    
    Args:
        patient_master: DataFrame containing patient demographics
        output_path: Path to save the PNG file
    """
    demographics = patient_master.loc[patient_master["has_patient_demographics"]].copy()
    demographics["age_years"] = _age_series(demographics["date_of_birth"])
    
    plt.figure(figsize=(10, 6))
    plt.hist(demographics["age_years"].dropna(), bins=30, color="#4A90E2", edgecolor="black", alpha=0.7)
    plt.xlabel("Age (years)", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Patients", fontsize=12, fontweight="bold")
    plt.title("Patient Age Distribution", fontsize=14, fontweight="bold", pad=20)
    plt.grid(axis="y", alpha=0.3, linestyle="--")
    
    # Add statistics annotation
    mean_age = demographics["age_years"].mean()
    median_age = demographics["age_years"].median()
    plt.axvline(mean_age, color="red", linestyle="--", linewidth=2, label=f"Mean: {mean_age:.1f}")
    plt.axvline(median_age, color="green", linestyle="--", linewidth=2, label=f"Median: {median_age:.1f}")
    plt.legend(fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def plot_diagnosis_frequency(analytics_summary: dict[str, Any], output_path: Path) -> None:
    """
    Generate a bar chart showing diagnosis frequency by ICD-10 chapter.
    
    Args:
        analytics_summary: Dictionary containing analytics data
        output_path: Path to save the PNG file
    """
    chapter_dist = analytics_summary["risk_profiles"]["icd10_mapping"]["chapter_distribution"]
    
    # Sort by frequency
    sorted_chapters = sorted(chapter_dist.items(), key=lambda x: x[1], reverse=True)
    chapters, counts = zip(*sorted_chapters)
    
    plt.figure(figsize=(14, 8))
    colors = sns.color_palette("viridis", len(chapters))
    bars = plt.barh(range(len(chapters)), counts, color=colors, edgecolor="black", linewidth=0.5)
    
    plt.yticks(range(len(chapters)), chapters, fontsize=10)
    plt.xlabel("Number of Diagnoses", fontsize=12, fontweight="bold")
    plt.ylabel("ICD-10 Chapter", fontsize=12, fontweight="bold")
    plt.title("Diagnosis Frequency by ICD-10 Chapter", fontsize=14, fontweight="bold", pad=20)
    plt.grid(axis="x", alpha=0.3, linestyle="--")
    
    # Add value labels on bars
    for i, (bar, count) in enumerate(zip(bars, counts)):
        plt.text(count + max(counts) * 0.01, i, str(count), va="center", fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def plot_lab_distribution(analytics_summary: dict[str, Any], output_path: Path) -> None:
    """
    Generate boxplots showing distribution of key lab tests.
    
    Args:
        analytics_summary: Dictionary containing analytics data
        output_path: Path to save the PNG file
    """
    lab_stats = analytics_summary["lab_statistics"]
    
    # Select key tests for visualization
    key_tests = ["GLUCOSE_FASTING", "HBA1C", "TOTAL_CHOLESTEROL", "CREATININE", "HEMOGLOBIN", "TSH"]
    available_tests = [test for test in key_tests if test in lab_stats]
    
    if not available_tests:
        print("⚠ Warning: No key lab tests found for visualization")
        return
    
    # Prepare data for boxplot (using percentiles to approximate distribution)
    data_for_plot = []
    labels = []
    
    for test in available_tests:
        stats = lab_stats[test]
        # Create synthetic data points from statistics for visualization
        # Using percentiles and mean/median to approximate distribution
        values = [
            stats["percentile_10"],
            stats["median"] - (stats["median"] - stats["percentile_10"]) / 2,
            stats["median"],
            stats["median"] + (stats["percentile_90"] - stats["median"]) / 2,
            stats["percentile_90"],
        ]
        data_for_plot.append(values)
        labels.append(test.replace("_", " ").title())
    
    plt.figure(figsize=(12, 7))
    bp = plt.boxplot(data_for_plot, labels=labels, patch_artist=True, notch=True, showmeans=True)
    
    # Customize colors
    colors = sns.color_palette("Set2", len(available_tests))
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    plt.ylabel("Test Value", fontsize=12, fontweight="bold")
    plt.xlabel("Lab Test", fontsize=12, fontweight="bold")
    plt.title("Distribution of Key Lab Test Results", fontsize=14, fontweight="bold", pad=20)
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", alpha=0.3, linestyle="--")
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def plot_genomics_quality(genomics_variants: pd.DataFrame, output_path: Path) -> None:
    """
    Generate a scatter plot of Variant Allele Frequency vs Read Depth.
    
    Args:
        genomics_variants: DataFrame containing genomics variant data
        output_path: Path to save the PNG file
    """
    if genomics_variants.empty:
        print("⚠ Warning: No genomics data available for visualization")
        return
    
    # Filter for valid data
    valid_data = genomics_variants.dropna(subset=["allele_frequency", "read_depth"]).copy()
    
    if valid_data.empty:
        print("⚠ Warning: No valid genomics quality data available")
        return
    
    # Identify pathogenic variants
    valid_data["is_pathogenic"] = valid_data["clinical_significance"].str.contains(
        "pathogenic", case=False, na=False
    )
    
    plt.figure(figsize=(12, 8))
    
    # Plot non-pathogenic variants
    non_pathogenic = valid_data[~valid_data["is_pathogenic"]]
    plt.scatter(
        non_pathogenic["read_depth"],
        non_pathogenic["allele_frequency"],
        alpha=0.5,
        s=30,
        c="#4A90E2",
        label="Benign/VUS",
        edgecolors="none",
    )
    
    # Plot pathogenic variants (highlighted)
    pathogenic = valid_data[valid_data["is_pathogenic"]]
    if not pathogenic.empty:
        plt.scatter(
            pathogenic["read_depth"],
            pathogenic["allele_frequency"],
            alpha=0.8,
            s=80,
            c="#E74C3C",
            label="Pathogenic",
            edgecolors="black",
            linewidths=1,
            marker="D",
        )
    
    plt.xlabel("Read Depth", fontsize=12, fontweight="bold")
    plt.ylabel("Variant Allele Frequency (VAF)", fontsize=12, fontweight="bold")
    plt.title("Genomics Quality: VAF vs Read Depth", fontsize=14, fontweight="bold", pad=20)
    plt.legend(fontsize=10, loc="upper right")
    plt.grid(alpha=0.3, linestyle="--")
    
    # Add quality threshold lines (common thresholds)
    plt.axhline(y=0.05, color="gray", linestyle=":", linewidth=1, alpha=0.5)
    plt.axvline(x=20, color="gray", linestyle=":", linewidth=1, alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def plot_site_comparison(analytics_summary: dict[str, Any], output_path: Path) -> None:
    """
    Generate a bar chart comparing patient distribution across hospital sites.
    
    Args:
        analytics_summary: Dictionary containing analytics data
        output_path: Path to save the PNG file
    """
    site_dist = analytics_summary["demographics"]["site_distribution"]
    
    sites = list(site_dist.keys())
    counts = list(site_dist.values())
    
    plt.figure(figsize=(10, 6))
    colors = sns.color_palette("pastel", len(sites))
    bars = plt.bar(sites, counts, color=colors, edgecolor="black", linewidth=1.5, alpha=0.8)
    
    plt.xlabel("Hospital Site", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Patients", fontsize=12, fontweight="bold")
    plt.title("Patient Distribution Across Hospital Sites", fontsize=14, fontweight="bold", pad=20)
    plt.grid(axis="y", alpha=0.3, linestyle="--")
    
    # Add value labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{count}\n({count/sum(counts)*100:.1f}%)",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
        )
    
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def plot_clinical_correlation(patient_master: pd.DataFrame, lab_results: pd.DataFrame, output_path: Path) -> None:
    """
    Generate a correlation heatmap between age and key lab values.
    
    Args:
        patient_master: DataFrame containing patient demographics
        lab_results: DataFrame containing lab results
        output_path: Path to save the PNG file
    """
    # Calculate age
    demographics = patient_master.loc[patient_master["has_patient_demographics"]].copy()
    demographics["age_years"] = _age_series(demographics["date_of_birth"])
    
    # Select key lab tests
    key_tests = ["GLUCOSE_FASTING", "HBA1C", "TOTAL_CHOLESTEROL", "CREATININE", "HEMOGLOBIN", "TSH"]
    
    # Pivot lab results to wide format
    lab_pivot = lab_results[lab_results["test_name"].isin(key_tests)].pivot_table(
        index="patient_id",
        columns="test_name",
        values="test_value",
        aggfunc="mean",
    )
    
    # Merge with age data
    correlation_data = demographics[["patient_id", "age_years"]].merge(
        lab_pivot, left_on="patient_id", right_index=True, how="inner"
    )
    
    # Calculate correlation matrix
    correlation_data = correlation_data.drop(columns=["patient_id"])
    correlation_data.columns = [col.replace("_", " ").title() if col != "age_years" else "Age (Years)" 
                                 for col in correlation_data.columns]
    
    corr_matrix = correlation_data.corr()
    
    if corr_matrix.empty or len(corr_matrix) < 2:
        print("⚠ Warning: Insufficient data for correlation analysis")
        return
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(
        corr_matrix,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        center=0,
        square=True,
        linewidths=1,
        cbar_kws={"shrink": 0.8, "label": "Correlation Coefficient"},
        vmin=-1,
        vmax=1,
    )
    
    plt.title("Clinical Correlation: Age vs Lab Values", fontsize=14, fontweight="bold", pad=20)
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✓ Saved: {output_path}")


def generate_all_visualizations(
    patient_master_path: Path,
    lab_results_path: Path,
    genomics_variants_path: Path,
    analytics_summary_path: Path,
    output_dir: Path,
) -> None:
    """
    Generate all six required visualizations.
    
    Args:
        patient_master_path: Path to patient_master.parquet
        lab_results_path: Path to lab_results directory (partitioned)
        genomics_variants_path: Path to genomics_variants.parquet
        analytics_summary_path: Path to analytics_summary.json
        output_dir: Directory to save all plots
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 60)
    print("GENERATING ANALYTICAL VISUALIZATIONS")
    print("=" * 60 + "\n")
    
    # Load data
    print("Loading data...")
    patient_master = pd.read_parquet(patient_master_path)
    genomics_variants = pd.read_parquet(genomics_variants_path)
    
    # Load partitioned lab results
    from pipeline.utils.io import read_partitioned_parquet
    lab_results = read_partitioned_parquet(lab_results_path)
    
    # Load analytics summary
    with open(analytics_summary_path, "r", encoding="utf-8") as f:
        analytics_summary = json.load(f)
    
    print(f"✓ Loaded {len(patient_master)} patients")
    print(f"✓ Loaded {len(lab_results)} lab results")
    print(f"✓ Loaded {len(genomics_variants)} genomics variants")
    print("✓ Loaded analytics summary\n")
    
    # Generate plots
    print("Generating visualizations...\n")
    
    plot_age_distribution(patient_master, output_dir / "01_age_distribution.png")
    plot_diagnosis_frequency(analytics_summary, output_dir / "02_diagnosis_frequency.png")
    plot_lab_distribution(analytics_summary, output_dir / "03_lab_distribution.png")
    plot_genomics_quality(genomics_variants, output_dir / "04_genomics_quality.png")
    plot_site_comparison(analytics_summary, output_dir / "05_site_comparison.png")
    plot_clinical_correlation(patient_master, lab_results, output_dir / "06_clinical_correlation.png")
    
    print("\n" + "=" * 60)
    print("✓ ALL VISUALIZATIONS GENERATED SUCCESSFULLY")
    print(f"✓ Output directory: {output_dir}")
    print("=" * 60 + "\n")
