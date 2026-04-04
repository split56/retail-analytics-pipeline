"""
ingestion/load_to_duckdb.py
- load raw CSV files into DuckDB's 'raw' schema.
- use CREATE OR REPLACE so this script is safe to run multiple times without creating duplicates.
"""

import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH   = os.getenv("DB_PATH",   "./retail.db")
DATA_PATH = os.getenv("DATA_PATH", "./data/raw")

# Maps CSV filename (no extension) to table name in DuckDB
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


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    connect to the DuckDB database file.
    """
    return duckdb.connect(DB_PATH)


def setup_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the raw schema if it doesn't exist.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    print("Schema raw is ready.")


def load_csv(
    con: duckdb.DuckDBPyConnection,
    csv_path: str,
    table_name: str
) -> None:
    """
    Load a single CSV file into DuckDB
    """
    full_table = f"raw.{table_name}"
    print(f"  Loading {Path(csv_path).name} → {full_table} ...", end="")

    con.execute(f"""
        CREATE OR REPLACE TABLE {full_table} AS
        SELECT * FROM read_csv_auto('{csv_path}', header = true ,ignore_errors=true)
    """)

    # Count rows to confirm load succeeded
    count = con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
    print(f" {count:>10,} rows ✓")


def validate_load(con: duckdb.DuckDBPyConnection) -> None:
    """
    Print a summary of all tables in the raw schema.
    Quick sanity check after ingestion.
    """
    print("\n── Validation ──────────────────────────")
    result = con.execute("""
        SELECT table_name, estimated_size as approx_rows
        FROM duckdb_tables()
        WHERE schema_name = 'raw'
        ORDER BY table_name
    """).fetchall()

    for table_name, approx_rows in result:
        print(f"  raw.{table_name:<40} ~{approx_rows:>10,} rows")


def run_ingestion() -> None:
    """Main function — runs the full ingestion process."""
    print("=" * 50)
    print("Starting ingestion...")
    print(f"Database: {DB_PATH}")
    print(f"Data path: {DATA_PATH}")
    print("=" * 50)

    con = get_connection()
    setup_schema(con)

    print("\n── Loading tables ──────────────────────")
    failed = []

    for filename, table_name in TABLE_MAP.items():
        csv_path = str(Path(DATA_PATH) / f"{filename}.csv")

        if not Path(csv_path).exists():
            print(f"  WARNING: {csv_path} not found — skipping.")
            failed.append(table_name)
            continue

        load_csv(con, csv_path, table_name)

    validate_load(con)
    con.close()

    print("\n── Summary ─────────────────────────────")
    if failed:
        print(f"  ⚠ {len(failed)} table(s) failed: {failed}")
    else:
        print("  ✓ All tables loaded successfully")
    print(f"  Database saved to: {DB_PATH}")


if __name__ == "__main__":
    run_ingestion()
