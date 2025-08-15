"""
quality_checks.py
-----------------
Schema enforcement, null fraction analysis, duplicate detection, and metadata reporting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


# ====================================================
# Function: load_config
# Purpose : Load YAML config
# Inputs  : config_path (str)
# Outputs : dict
# ====================================================
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@dataclass
class QualityReport:
    schema_ok: bool
    null_fractions: Dict[str, float]
    duplicate_count: int
    row_count: int
    issues: List[str]


# ====================================================
# Function: enforce_schema
# Purpose : Cast columns to expected types; add missing columns as nulls.
# Inputs  : df (DataFrame), expected_map (dict)
# Outputs : (DataFrame, bool, List[str])
# ====================================================
def enforce_schema(df: DataFrame, expected_map: dict, fail_on_mismatch: bool = False) -> Tuple[DataFrame, bool, List[str]]:
    issues: List[str] = []
    # cast or add missing columns
    for col, spark_type in expected_map.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(spark_type))
            issues.append(f"Added missing column: {col}")
        else:
            df = df.withColumn(col, F.col(col).cast(spark_type))

    # check extras
    extras = [c for c in df.columns if c not in expected_map]
    if extras:
        issues.append(f"Extra columns present: {extras}")

    schema_ok = not extras if fail_on_mismatch else True
    # reorder columns to expected
    df = df.select(list(expected_map.keys()))
    return df, schema_ok, issues


# ====================================================
# Function: compute_null_fractions
# Purpose : Compute fraction of null/empty values per column.
# Inputs  : df (DataFrame)
# Outputs : Dict[col -> fraction]
# ====================================================
def compute_null_fractions(df: DataFrame) -> Dict[str, float]:
    total = df.count() or 0
    fractions = {}
    for c in df.columns:
        nnull = df.filter(F.col(c).isNull() | (F.col(c) == "")).count()
        fractions[c] = (nnull / total) if total else 0.0
    return fractions


# ====================================================
# Function: count_duplicates
# Purpose : Count duplicate rows (entire row) or subset if provided.
# Inputs  : df (DataFrame), subset (List[str] | None)
# Outputs : int (duplicate count)
# ====================================================
def count_duplicates(df: DataFrame, subset: List[str] | None = None) -> int:
    if subset:
        dup_count = df.groupBy([F.col(c) for c in subset]).count().filter(F.col("count") > 1).count()
    else:
        # naive full-row duplication check
        total = df.count()
        distinct = df.distinct().count()
        dup_count = total - distinct
    return int(dup_count)


# ====================================================
# Function: run_quality_checks
# Purpose : Run all quality checks and optionally drop duplicates.
# Inputs  : df (DataFrame), cfg (dict)
# Outputs : QualityReport; also registers typed df as temp view 'typed_input'
# ====================================================
def run_quality_checks(df: DataFrame, cfg: dict, dataset_key: str) -> QualityReport:
    expected = cfg["schema"].get(dataset_key, {})
    df_typed, schema_ok, issues = enforce_schema(df, expected, cfg["quality_thresholds"].get("fail_on_schema_mismatch", False))
    null_fracs = compute_null_fractions(df_typed)
    duplicate_count = count_duplicates(df_typed, None)

    if duplicate_count > 0 and cfg["quality_thresholds"].get("drop_duplicates", True):
        before = df_typed.count()
        df_typed = df_typed.dropDuplicates()
        after = df_typed.count()
        LOGGER.info("Dropped duplicates for %s: %d -> %d", dataset_key, before, after)

    # log columns that exceed null threshold
    max_null = cfg["quality_thresholds"].get("max_null_fraction_per_column", 0.5)
    for col, frac in null_fracs.items():
        if frac > max_null:
            issues.append(f"High null fraction in {col}: {frac:.2%}")

    # cache and register
    df_typed.cache()
    df_typed.createOrReplaceTempView(f"typed_{dataset_key}")

    row_count = df_typed.count()
    LOGGER.info("Quality checks for %s completed. rows=%d, dup=%d", dataset_key, row_count, duplicate_count)

    return QualityReport(schema_ok, null_fracs, duplicate_count, row_count, issues)
