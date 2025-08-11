# English Premier League (EPL) Data Engineering Pipeline

**Author:** Souvind N K  
**Project:** EPL Data Engineering Pipeline (Azure + Databricks friendly)

## Project Overview
An end-to-end data engineering project for English Premier League (EPL) match & player data.
It demonstrates extraction, PySpark-based transformation, storage in Parquet (Delta-ready), and sample SQL analytics queries.

Designed for local evaluation and easy migration to Azure Databricks + ADLS + Synapse.

## Structure
- `extract.py` : prepares sample raw CSV datasets (matches, players, events/goals).
- `transform.py` : PySpark script to transform CSVs into cleaned Parquet datasets and compute aggregates.
- `databricks_notebook_football_transform.py` : Databricks-friendly notebook (source format).
- `load.py` : Example uploader to Azure Blob / ADLS Gen2.
- `sql_scripts/football_analytics_queries.sql` : SQL queries for common EPL analytics.
- `requirements.txt` : Python dependencies.
- `sample_data/` : Small sample datasets for local runs.
- `.gitignore` : Git ignore file.
- `pipeline_diagram.png` : Placeholder diagram (replace with your architecture image).

## Quick Start (local)
1. Create virtualenv and install dependencies:
```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

2. Extract (prepare sample data):
```bash
python extract.py
# This will copy sample CSVs to data/raw/
```

3. Transform (run PySpark locally):
```bash
python transform.py --matches data/raw/matches.csv --players data/raw/players.csv --events data/raw/events.csv --output data/processed
```

4. Load (upload to ADLS / Blob):
```bash
python load.py --local-path data/processed --container-name my-container --connection-string "$AZURE_STORAGE_CONNECTION_STRING"
```

## Notes for Databricks
- Import `databricks_notebook_football_transform.py` as a notebook or paste cells into a new notebook.
- Use cluster with Spark 3.x and Python 3.8+

## Contact
Souvind N K — souvind.souvi@gmail.com  
LinkedIn: https://www.linkedin.com/in/souvind-sajeev/
