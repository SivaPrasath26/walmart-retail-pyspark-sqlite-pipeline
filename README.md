#  Retail PySpark SQLite Pipeline

##  Overview
This project demonstrates an **enterprise-grade PySpark data pipeline** that ingests the **Retail Data Analytics (Walmart)** dataset from Kaggle, performs data quality checks, transformations, and outputs a curated dataset into a **local SQLite database** — ready for **Power BI** or other BI tools.

It is designed with **modular architecture**, **YAML-based configuration**, and **production best practices**.

---

##  Folder Structure

project_root/
│
├── config/
│ ├── pipeline_config.yaml # All pipeline parameters, schema, paths, renaming rules
│
├── data/
│ ├── raw/ # Unprocessed datasets from Kaggle
│ ├── curated/ # Final SQLite DB output
│
├── logs/
│ ├── pipeline.log # Pipeline logs
│
├── src/
│ ├── init.py
│ ├── ingestion_kaggle.py # Kaggle download & extraction
│ ├── quality_checks.py # Schema, nulls, duplicates
│ ├── transform_spark.py # PySpark transformations & enrichment
│ ├── pipeline_orchestrator.py # Orchestrates the pipeline
│
├── requirements.txt # Python dependencies
├── README.md # Project documentation
└── .env # Kaggle API credentials

## Setup Instructions

### 1️. Clone Repo
```bash
git clone https://github.com/YOUR_USERNAME/retail-pyspark-sqlite-pipeline.git
cd retail-pyspark-sqlite-pipeline
```

### 2️. Create & Activate Virtual Environment
```bash
python -m venv venv
source venv/bin/activate     # Mac/Linux
venv\Scripts\activate        # Windows
```

### 3️. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4️. Kaggle API Setup
- Sign up / log in to [Kaggle](https://www.kaggle.com/).
- Generate API Token from **Account Settings** → **Create New API Token**.
- Save `kaggle.json` to `~/.kaggle/kaggle.json`  
  **OR** create a `.env` file in the project root:
```
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
```

### 5️. Configure Pipeline
Update `config/pipeline_config.yaml`:
- File paths  
- Column rename mappings  
- Schema definitions  
- Output SQLite table names  

---

## 6. Run Pipeline
```bash
python src/pipeline_orchestrator.py
```

**Flow**:
1. **Ingestion** → Downloads Walmart dataset from Kaggle into `data/raw/`
2. **Quality Checks** → Validates schema, nulls, duplicates
3. **Transformation** → Merges datasets, renames columns, enriches data
4. **SQLite Output** → Saves final tables into `data/curated/retail_pipeline.db`

---

## Power BI Integration
1. Open **Power BI Desktop**
2. Select **Get Data** → **SQLite Database**
3. Connect to `data/curated/retail_pipeline.db`
4. Build dashboards using curated tables.

---

## Key Features
- **PySpark-based** transformations for scalability
- **YAML config** for flexible parameters
- **Logging** to `logs/pipeline.log`
- **SQLite** output for BI-friendly analytics
- **Modular, production-grade design**

---

## 🏗 Tech Stack
- Python 3.10+
- PySpark
- Pandas
- SQLite
- YAML
- Logging
