import duckdb
import time

con = duckdb.connect()

queries = {
    "top_products": """
    SELECT p.product_name, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY revenue DESC
    LIMIT 5;
    """,

    "sales_by_region": """
    SELECT s.region, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_stores s ON f.store_id = s.store_id
    GROUP BY s.region;
    """,

    "customer_spend": """
    SELECT c.customer_name, SUM(f.total_amount) AS spend
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_name
    ORDER BY spend DESC
    LIMIT 5;
    """
}

results = {}

# -----------------------------
# CSV BENCHMARK
# -----------------------------
con.execute("CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM read_csv_auto('data/sales.csv')")
con.execute("CREATE OR REPLACE TABLE dim_products AS SELECT * FROM read_csv_auto('data/products.csv')")
con.execute("CREATE OR REPLACE TABLE dim_customers AS SELECT * FROM read_csv_auto('data/customers.csv')")
con.execute("CREATE OR REPLACE TABLE dim_stores AS SELECT * FROM read_csv_auto('data/stores.csv')")

for name, query in queries.items():
    start = time.time()
    con.execute(query).fetchall()
    results[name + "_csv"] = time.time() - start

# -----------------------------
# PARQUET BENCHMARK
# -----------------------------
con.execute("CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM read_parquet('output/fact_sales.parquet')")
con.execute("CREATE OR REPLACE TABLE dim_products AS SELECT * FROM read_parquet('output/dim_products.parquet')")
con.execute("CREATE OR REPLACE TABLE dim_customers AS SELECT * FROM read_parquet('output/dim_customers.parquet')")
con.execute("CREATE OR REPLACE TABLE dim_stores AS SELECT * FROM read_parquet('output/dim_stores.parquet')")

for name, query in queries.items():
    start = time.time()
    con.execute(query).fetchall()
    results[name + "_parquet"] = time.time() - start

print(results)
