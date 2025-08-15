"""
ingestion_kaggle.py
-------------------
Download dataset files from Kaggle into data/raw/ using the Kaggle CLI and config-driven patterns.
Includes robust extraction and returns list of retained file paths.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import List

import yaml
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)


# ====================================================
# Function: load_config
# Purpose : Load YAML config file.
# Inputs  : config_path (str)
# Outputs : dict (configuration)
# ====================================================
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ====================================================
# Function: setup_logging
# Purpose : Initialize logging to file & console.
# Inputs  : log_file (str)
# Outputs : None
# ====================================================
def setup_logging(log_file: str) -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


# ====================================================
# Function: _is_archive
# Purpose : Detect archive file types.
# Inputs  : Path
# Outputs : bool
# ====================================================
def _is_archive(p: Path) -> bool:
    return p.suffix.lower() in {".zip", ".tar", ".gz", ".tgz", ".tar.gz"}


# ====================================================
# Function: _extract_zip
# Purpose : Extract zip archives to target dir.
# Inputs  : archive_path (Path), out_dir (Path)
# Outputs : List[Path] extracted file paths
# ====================================================
def _extract_zip(archive_path: Path, out_dir: Path) -> List[Path]:
    extracted = []
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(out_dir)
        extracted = [out_dir / n for n in zf.namelist()]
    return extracted


# ====================================================
# Function: download_from_kaggle
# Purpose : Download dataset from Kaggle and retain configured CSVs.
# Inputs  : config_path (str)
# Outputs : List[Path] kept file paths
# ====================================================
def download_from_kaggle(config_path: str) -> List[Path]:
    cfg = load_config(config_path)
    setup_logging(cfg["paths"]["log_file"])

    raw_dir = Path(cfg["paths"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    patterns = cfg["kaggle"].get("file_patterns", ["*.csv"])
    timeout = int(cfg["kaggle"].get("download_timeout_sec", 600))

    load_dotenv()  # loads .env
    if not (os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")):
        LOGGER.error("Kaggle credentials not found. Please populate .env with KAGGLE_USERNAME & KAGGLE_KEY")
        raise EnvironmentError("Missing Kaggle API credentials (.env).")


    try:
        
        # Determine Kaggle source
        if "competition" in cfg["kaggle"]:
            source_type = "competition"
            source_name = cfg["kaggle"]["competition"]
        elif "dataset" in cfg["kaggle"]:
            source_type = "dataset"
            source_name = cfg["kaggle"]["dataset"]
        else:
            raise KeyError("Kaggle config must contain either 'competition' or 'dataset'.")

        LOGGER.info("Downloading Kaggle dataset '%s' into %s", source_name, raw_dir)

        # Download based on type
        if source_type == "competition":
            subprocess.run(
                ["kaggle", "competitions", "download", "-c", source_name, "-p", str(raw_dir), "-o"],
                check=True,
                timeout=timeout,
            )
        else:
            subprocess.run(
                ["kaggle", "datasets", "download", "-d", source_name, "-p", str(raw_dir), "-o"],
                check=True,
                timeout=timeout,
            )
            
    except subprocess.CalledProcessError as e:
        LOGGER.error("Kaggle CLI returned non-zero: %s", e)
        raise
    except subprocess.TimeoutExpired:
        LOGGER.error("Kaggle download timed out after %s seconds", timeout)
        raise

    # Extract any zip files produced
    for p in list(raw_dir.iterdir()):
        if p.is_file() and _is_archive(p):
            LOGGER.info("Extracting archive: %s", p.name)
            try:
                _extract_zip(p, raw_dir)
            except Exception:
                LOGGER.exception("Failed to extract archive %s", p)
            # keep archive (optional), or remove to save space

    # Determine files to keep based on patterns
    keep: List[Path] = []
    for pat in patterns:
        keep.extend(list(raw_dir.glob(pat)))
    keep = sorted({p.resolve() for p in keep if p.exists()})

    # Move unrelated files to _ignored
    ignored_dir = raw_dir / "_ignored"
    ignored_dir.mkdir(exist_ok=True)
    for p in raw_dir.iterdir():
        if p.is_file() and p.resolve() not in keep and not _is_archive(p):
            LOGGER.info("Moving unexpected file %s to %s", p.name, ignored_dir)
            shutil.move(str(p), ignored_dir / p.name)

    LOGGER.info("Ingestion finished. Kept files: %s", [p.name for p in keep])
    return keep
