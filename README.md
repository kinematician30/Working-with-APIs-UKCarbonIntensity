# Working-with-APIs-UKCarbonIntensity
# XTD Research Labs: UK Carbon Intensity Data Warehouse

## 🔬 About the Business

**XTD Research Labs** is a premier scientific institution focused on environmental sustainability. We leverage high-resolution data to provide actionable insights for energy stakeholders and policymakers, ensuring the global transition to green energy is backed by rigorous, data-driven evidence.

## 🛑 Problem Statement

Environmental researchers currently face a **data accessibility gap**. Existing collection methods for UK carbon intensity are fragmented and manual, failing to capture the granular 30-minute fluctuations necessary for accurate climate modeling. Without a centralized, high-integrity historical repository, XTD Labs cannot perform the longitudinal studies required to validate decarbonization policies.

## 🎯 Project Rationale

* **Empowering Research:** Providing a continuous, high-resolution dataset to pinpoint carbon "hotspots."
* **Structural Scalability:** Utilizing a partitioned Postgres warehouse to store years of data without performance loss.
* **Automation of Discovery:** Removing "data janitorial" tasks so scientists can focus 100% on hypothesis testing and interpretation.

## 🏗️ Technical Architecture (ETL)

1. **Extract:** Python `Requests` library fetches 48 intervals (30-min each) of regional data from the National Grid Carbon Intensity API.
2. **Transform:** `Pandas` flattens nested JSON, aggregates data into **daily regional averages**, and rounds metrics for research-grade precision.
3. **Load:** Data is pushed into a **PostgreSQL Star Schema** with built-in yearly partitions for optimized query performance.

### Data Schema

| Table | Description |
| --- | --- |
| **`dim_region`** | Static lookup for IDs, shortnames, and DNO regions. |
| **`fact_carbon_intensity`** | Daily averages for forecast intensity and qualitative index. |
| **`fact_generation_mix`** | Wide-table storage of fuel type percentages (Wind, Solar, Gas, etc.). |

## 🚀 Getting Started

1. Ensure you have a `conn.yaml` file with your Postgres credentials.
2. Install dependencies: `pip install pandas sqlalchemy psycopg2 pyyaml requests`.
3. Run the ETL: `python xtd_carbon_etl.py`.

## 📈 Learning Opportunities

This project explores master-level concepts in **Data Engineering**, including API orchestration, Star Schema design, and advanced Postgres techniques like **Table Partitioning** and **Composite Keys**.
