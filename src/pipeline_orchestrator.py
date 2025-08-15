"""
pipeline_orchestrator.py
------------------------
Orchestration entrypoint: ingestion -> quality checks -> transforms -> write to SQLite.
Run locally: python src/pipeline_orchestrator.py
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict

import yaml
from pyspark.sql import SparkSession, DataFrame

from ingestion_kaggle import download_from_kaggle
from quality_checks import load_config as qc_load_config, run_quality_checks
from transform_spark import build_curated_tables, load_config as tr_load_config

LOGGER = logging.getLogger(__name__)


# ====================================================
# Function: load_config
# Purpose : Load YAML config file
# Inputs  : config_path (str)
# Outputs : dict
# ====================================================
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ====================================================
# Function: build_spark
# Purpose : Create SparkSession with SQLite JDBC package configured
# Inputs  : cfg (dict)
# Outputs : SparkSession
# ====================================================
def build_spark(cfg: dict) -> SparkSession:
    # Set Spark local IP if provided
    local_ip = cfg["spark"].get("local_ip")
    if local_ip:
        os.environ["SPARK_LOCAL_IP"] = local_ip

    builder = SparkSession.builder.appName(cfg["spark"]["app_name"]).master(cfg["spark"]["master"])

    packages = cfg["spark"].get("packages", [])
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    builder = builder.config("spark.sql.shuffle.partitions", cfg["spark"].get("shuffle_partitions", 8))

    driver_bind_address = cfg["spark"].get("driver_bind_address")
    if driver_bind_address:
        builder = builder.config("spark.driver.bindAddress", driver_bind_address)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ====================================================
# Function: read_csvs
# Purpose : Read a list of CSV files into a single DataFrame (header-aware)
# Inputs  : spark (SparkSession), files (List[Path])
# Outputs : DataFrame
# ====================================================
def read_csvs(spark: SparkSession, files) -> DataFrame:
    # Use a robust CSV reader (no inferSchema; we cast later)
    return spark.read.option("header", True).option("multiLine", True).option("escape", "\"").csv([str(f) for f in files])


# ====================================================
# Function: write_sqlite
# Purpose : Write provided DataFrames to SQLite via JDBC
# Inputs  : dfs: Dict[str, DataFrame], cfg: dict
# Outputs : None
# ====================================================
def write_sqlite(dfs: Dict[str, DataFrame], cfg: dict) -> None:
    url = cfg["output"]["jdbc_url"]
    driver = cfg["output"]["driver"]
    mode = cfg["output"]["mode"]

    LOGGER.info("Writing to SQLite URL: %s", url)
    for table_name, df in dfs.items():
        LOGGER.info("Writing table '%s' (rows=%d) ...", table_name, df.count())
        (
            df.write.format("jdbc")
            .option("url", url)
            .option("dbtable", table_name)
            .option("driver", driver)
            .mode(mode)
            .save()
        )
        LOGGER.info("Wrote table '%s' to SQLite.", table_name)


# ====================================================
# Function: main
# Purpose : Orchestrate full pipeline run
# Inputs  : None
# Outputs : None
# ====================================================
def main():
    cfg_path = "config/pipeline_config.yaml"
    cfg = load_config(cfg_path)

    # Ensure directories exist
    Path(cfg["paths"]["raw_dir"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["paths"]["curated_dir"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["paths"]["log_file"]).parent.mkdir(parents=True, exist_ok=True)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(cfg["paths"]["log_file"], mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    LOGGER.info("Starting Retail Analytics Pipeline")

    # 1) Ingest
    files = download_from_kaggle(cfg_path)
    if not files:
        LOGGER.error("No files downloaded. Exiting.")
        return

    # 2) Start Spark
    spark = build_spark(cfg)

    # 3) Read specific CSVs into DataFrames (group by filename)
    file_map = {p.name: p for p in files}
    dfs = {}
    for key in ["train.csv", "features.csv", "stores.csv"]:
        if key in file_map:
            LOGGER.info("Loading %s", key)
            dfs[key] = read_csvs(spark, [file_map[key]])
        else:
            LOGGER.warning("Expected file %s not found in raw dir.", key)

    # 4) Run quality checks per dataset and register typed views
    # Map keys in config: 'train', 'features', 'stores'
    name_map = {"train.csv": "train", "features.csv": "features", "stores.csv": "stores"}
    for fname, df in dfs.items():
        dataset_key = name_map.get(fname)
        if dataset_key:
            LOGGER.info("Running quality checks for %s", dataset_key)
            report = run_quality_checks(df, qc_load_config(cfg_path), dataset_key)
            if report.issues:
                for issue in report.issues:
                    LOGGER.warning("QC issue (%s): %s", dataset_key, issue)

    # 5) Transformations & curated tables
    curated_tables = build_curated_tables(spark, tr_load_config(cfg_path))

    # 6) Write to SQLite
    write_sqlite(curated_tables, cfg)

    # 7) Final check & path info
    sqlite_path = cfg["paths"]["curated_dir"] + "/retail_pipeline.db"
    if Path(sqlite_path).exists():
        LOGGER.info("Pipeline completed. SQLite DB available at: %s", sqlite_path)
    else:
        LOGGER.warning("Pipeline finished but DB not found at: %s (it may still be created by JDBC driver)", sqlite_path)

    spark.stop()


if __name__ == "__main__":
    main()
