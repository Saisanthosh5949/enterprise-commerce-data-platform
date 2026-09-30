# Enterprise E-Commerce Data Platform

A portfolio-grade PySpark data engineering project implementing a local Medallion Architecture (Raw → Bronze → Silver → Gold) and scaffolding for AWS S3, Snowflake, dbt, Airflow, Docker and CI/CD.

## What it demonstrates
- PySpark DataFrame generation and transformations
- 100K customers, 20K products, 1M orders, 3M order items
- Parquet-based lake layers
- Relational keys across customers/orders/items/products/payments/inventory
- Deduplication and data-quality rules
- Gold fact and daily aggregate datasets
- Airflow DAG scaffolding
- dbt/Snowflake scaffolding
- GitHub Actions CI

## Prerequisites
- Python 3.11
- Java 17
- Git

## Setup (Windows PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run the whole local pipeline
```powershell
python run_pipeline.py
```

Because the default generator creates millions of rows, your first run can take several minutes depending on RAM/CPU.

## Run step by step
```powershell
python -m src.generators.generate_all
python -m src.ingestion.local_to_bronze
python -m src.transformations.bronze_to_silver
python -m src.quality.run_quality_checks
python -m src.transformations.build_gold
```

## Architecture
```text
Synthetic operational data
        ↓
     RAW Parquet
        ↓
PySpark ingestion
        ↓
      BRONZE
        ↓
PySpark cleaning / validation
        ↓
      SILVER
        ↓
PySpark joins / aggregation
        ↓
       GOLD
        ↓
Sales fact + daily sales marts

Next cloud stage: AWS S3 → Snowflake → dbt
Orchestration: Airflow
CI: GitHub Actions
```

## Important
Never commit `.env`, AWS keys, or Snowflake passwords. `.env.example` contains placeholders only.

## Next implementation stage
After validating the local pipeline, connect Bronze/Silver to AWS S3, load curated data into Snowflake, complete dbt sources/tests/models, and run the workflow through Airflow.
