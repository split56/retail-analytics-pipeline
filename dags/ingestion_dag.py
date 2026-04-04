"""
Orchestrates loading CSV files into DuckDB.
Runs daily at 6 AM. Retries 3 times on failure.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

TABLE_MAP = {
    "olist_orders_dataset":              "orders",
    "olist_customers_dataset":           "customers",
    "olist_order_items_dataset":         "order_items",
    "olist_order_payments_dataset":      "order_payments",
    "olist_order_reviews_dataset":       "order_reviews",
    "olist_products_dataset":            "products",
    "olist_sellers_dataset":             "sellers",
    "product_category_name_translation": "product_category_translation",
}

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def load_single_table(filename, table_name):
    """Load one CSV file into DuckDB raw schema."""
    import os
    from pathlib import Path
    import duckdb

    db_path   = os.getenv("DB_PATH",   "/opt/airflow/retail.db")
    data_path = os.getenv("DATA_PATH", "/opt/airflow/data/raw")
    csv_path  = str(Path(data_path) / f"{filename}.csv")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.{table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', ignore_errors=true)
    """)
    count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
    con.close()
    print(f"Loaded {count:,} rows into raw.{table_name}")


with DAG(
    dag_id="ingestion_dag",
    default_args=default_args,
    description="Load Olist CSV files into DuckDB",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "duckdb"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    tasks = [
        PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_single_table,
            op_kwargs={"filename": filename, "table_name": table_name},
        )
        for filename, table_name in TABLE_MAP.items()
    ]

    start >> tasks >> end
