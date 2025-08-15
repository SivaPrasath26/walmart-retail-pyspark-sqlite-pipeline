"""
transform_spark.py
------------------
Transformations and enrichment for the Walmart retail dataset.
Performs renaming (from YAML), parsing dates, joins, and creates curated aggregates.
"""

from __future__ import annotations

import logging
from typing import Dict, List

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOGGER = logging.getLogger(__name__)


# ====================================================
# Function: load_config
# Purpose : Load YAML config file
# Inputs  : config_path (str)
# Outputs : dict
# ====================================================
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ====================================================
# Function: rename_columns_df
# Purpose : Rename a DataFrame's columns using mapping from config
# Inputs  : df (DataFrame), mapping (dict)
# Outputs : DataFrame with renamed columns
# ====================================================
def rename_columns_df(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    select_exprs = []
    for old, new in mapping.items():
        if old in df.columns:
            select_exprs.append(F.col(old).alias(new))
        else:
            select_exprs.append(F.lit(None).cast("string").alias(new))
    return df.select(*select_exprs)


# ====================================================
# Function: parse_and_enrich
# Purpose : Parse dates, add year/month/week, cast booleans
# Inputs  : df (DataFrame)
# Outputs : enriched DataFrame
# ====================================================
def parse_and_enrich(df: DataFrame) -> DataFrame:
    # parse date strings into proper date
    df2 = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    df2 = df2.withColumn("year", F.year("date")).withColumn("month", F.month("date")).withColumn("week", F.weekofyear("date"))
    # ensure booleans
    if "is_holiday" in df2.columns:
        df2 = df2.withColumn("is_holiday", F.col("is_holiday").cast(T.BooleanType()))
    return df2


# ====================================================
# Function: build_curated_tables
# Purpose : Join train/features/stores, compute curated and aggregate tables
# Inputs  : spark (SparkSession), cfg (dict)
# Outputs : Dict[str, DataFrame] keyed by table name
# ====================================================
def build_curated_tables(spark: SparkSession, cfg: dict) -> Dict[str, DataFrame]:
    # Load typed temp views produced by quality_checks
    train = spark.table("typed_train")
    features = spark.table("typed_features")
    stores = spark.table("typed_stores")

    # LOG column names to debug
    LOGGER.info("typed_train columns: %s", train.columns)
    LOGGER.info("typed_features columns: %s", features.columns)
    LOGGER.info("typed_stores columns: %s", stores.columns)

    # Rename to canonical names using YAML mapping (defensive)
    rename_map_train = cfg["rename_columns"]["train"]
    LOGGER.info("train columns after rename: %s", train.columns)
    rename_map_features = cfg["rename_columns"]["features"]
    rename_map_stores = cfg["rename_columns"]["stores"]

    train = rename_columns_df(train, rename_map_train)
    features = rename_columns_df(features, rename_map_features)
    stores = rename_columns_df(stores, rename_map_stores)

    # Parse dates and enrich
    train = parse_and_enrich(train)
    features = parse_and_enrich(features)

    # Inner join: train + features on store_id & date, then join stores metadata
    joined = (
        train.alias("t")
        .join(features.alias("f"), on=[F.col("t.store_id") == F.col("f.store_id"), F.col("t.date") == F.col("f.date")], how="left")
        .select("t.*", *[c for c in features.columns if c not in train.columns])
    )

    full = joined.join(stores, on="store_id", how="left")

    # Curated wide table
    curated = (
        full.select(
            "date", "year", "month", "week",
            F.col("store_id"),
            F.col("department_id"),
            F.col("weekly_sales"),
            F.col("is_holiday"),
            F.col("temperature_f"),
            F.col("fuel_price"),
            F.col("markdown_1"),
            F.col("markdown_2"),
            F.col("markdown_3"),
            F.col("markdown_4"),
            F.col("markdown_5"),
            F.col("cpi"),
            F.col("unemployment_rate"),
            F.col("store_type"),
            F.col("store_size"),
        )
    )

    # Aggregates: store x department weekly/monthly
    agg_store_dept = (
        curated.groupBy("store_id", "department_id", "year", "month")
        .agg(
            F.count("*").alias("num_weeks"),
            F.sum("weekly_sales").alias("sum_weekly_sales"),
            F.avg("weekly_sales").alias("avg_weekly_sales"),
            F.max("weekly_sales").alias("max_weekly_sales"),
        )
        .orderBy("store_id", "department_id", "year", "month")
    )

    # Aggregates: store_type x year
    agg_store_type_year = (
        curated.groupBy("store_type", "year")
        .agg(
            F.sum("weekly_sales").alias("total_sales"),
            F.avg("weekly_sales").alias("avg_weekly_sales"),
            F.countDistinct("store_id").alias("num_stores"),
        )
        .orderBy(F.col("total_sales").desc())
    )

    # Holiday vs normal comparisons
    holidays_vs_normal = (
        curated.groupBy("year", "is_holiday")
        .agg(
            F.sum("weekly_sales").alias("total_sales"),
            F.avg("weekly_sales").alias("avg_weekly_sales"),
            F.count("*").alias("rows")
        )
        .orderBy("year", "is_holiday")
    )

    # Register for convenience
    curated.createOrReplaceTempView("curated_sales")

    tables = {
        cfg["output"]["tables"]["sales_curated"]: curated,
        cfg["output"]["tables"]["agg_store_dept"]: agg_store_dept,
        cfg["output"]["tables"]["agg_store_type_year"]: agg_store_type_year,
        cfg["output"]["tables"]["holidays_vs_normal"]: holidays_vs_normal,
    }

    LOGGER.info("Built curated tables: %s", list(tables.keys()))
    return tables
