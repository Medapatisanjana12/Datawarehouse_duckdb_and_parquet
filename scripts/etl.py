import duckdb
import os

# Create DuckDB connection
con = duckdb.connect("warehouse.duckdb")

# -----------------------------
# 1. INGEST CSVs
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE raw_sales AS
SELECT * FROM read_csv_auto('data/sales.csv');
""")

con.execute("""
CREATE OR REPLACE TABLE raw_customers AS
SELECT * FROM read_csv_auto('data/customers.csv');
""")

con.execute("""
CREATE OR REPLACE TABLE raw_products AS
SELECT * FROM read_csv_auto('data/products.csv');
""")

con.execute("""
CREATE OR REPLACE TABLE raw_stores AS
SELECT * FROM read_csv_auto('data/stores.csv');
""")

# -----------------------------
# 2. DIMENSION TABLES
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    customer_id,
    customer_name,
    city,
    state
FROM raw_customers;
""")

con.execute("""
CREATE OR REPLACE TABLE dim_products AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    price
FROM raw_products;
""")

con.execute("""
CREATE OR REPLACE TABLE dim_stores AS
SELECT DISTINCT
    store_id,
    store_name,
    region
FROM raw_stores;
""")

# -----------------------------
# 3. FACT TABLE
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    s.sale_id,
    s.date,
    s.quantity,
    s.total_amount,
    s.customer_id,
    s.product_id,
    s.store_id
FROM raw_sales s;
""")

# -----------------------------
# 4. EXPORT TO PARQUET
# -----------------------------
os.makedirs("output", exist_ok=True)

con.execute("COPY dim_customers TO 'output/dim_customers.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_products TO 'output/dim_products.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_stores TO 'output/dim_stores.parquet' (FORMAT PARQUET);")
con.execute("COPY fact_sales TO 'output/fact_sales.parquet' (FORMAT PARQUET);")

print("✅ ETL Completed Successfully")
