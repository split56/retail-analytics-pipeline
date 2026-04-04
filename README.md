# Retail Analytics Pipeline

End-to-end data pipeline for e-commerce analytics using the modern data stack.

## Architecture


CSV Files (Kaggle Olist Dataset)
|
v
Python Ingestion Script
|
v
DuckDB (retail.db - raw schema)
|
v
dbt (staging -> intermediate -> mart)
|
v
DuckDB (analytics schema)
|
v
Streamlit Dashboard (localhost:8501)

All orchestrated by Apache Airflow


## Tech Stack

| Layer          | Tool              | Purpose                          |
|----------------|-------------------|----------------------------------|
| Orchestration  | Apache Airflow    | Schedule and monitor pipeline    |
| Data Warehouse | DuckDB            | Fast local analytical database   |
| Transformation | dbt               | SQL models, tests, documentation |
| Dashboard      | Streamlit + Plotly| Interactive data visualization   |
| Language       | Python 3.11       | Ingestion scripts                |
| Containers     | Docker            | Local Airflow environment        |

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- 100,000+ orders from 2016-2018
- 9 relational tables covering orders, customers, products, sellers, payments, reviews

## Data Model

### dbt Lineage (3 Layers)

Sources (raw.*)
|
v
Staging: stg_orders, stg_customers, stg_order_items,
stg_order_payments, stg_order_reviews,
stg_products, stg_sellers
|
v
Intermediate: int_orders_enriched, int_customer_summary
|
v
Mart: fct_orders, fct_order_items, dim_customers


## Dashboard

4-page interactive dashboard:
- **Overview** — Revenue KPIs, trends, revenue by state
- **Delivery** — On-time rate, delivery days by state
- **Products** — Revenue by category, top categories over time
- **Customers** — Segments, repeat rate, lifetime value

## How to Run

### Prerequisites
- Python 3.11
- Docker Desktop

### Setup
```bash
git clone https://github.com/YOUR_USERNAME/retail-analytics-pipeline.git
cd retail-analytics-pipeline
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

Download Data
Download Olist dataset
and extract CSV files to data/raw/

Run Pipeline
# 1. Ingest
python ingestion/load_to_duckdb.py

# 2. Transform
cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir . && cd ..

# 3. Dashboard
streamlit run dashboard/app.py

# 4. Airflow (optional)
docker-compose up -d
# Open http://localhost:8080 (admin/admin)

Project Structure
retail-analytics-pipeline/
├── dags/                    # Airflow DAGs
│   ├── ingestion_dag.py
│   └── dbt_dag.py
├── ingestion/               # Python ingestion scripts
│   └── load_to_duckdb.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/         # 7 staging models
│   │   ├── intermediate/    # 2 intermediate models
│   │   └── mart/            # 3 mart models (fact + dim)
│   └── tests/               # 3 custom SQL tests
├── dashboard/
│   └── app.py               # 4-page Streamlit dashboard
├── docker-compose.yml
└── requirements.txt