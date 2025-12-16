## DuckDB Star Schema ETL & Benchmarking Project
### Project Overview

This project demonstrates how to build a local analytical data warehouse using DuckDB, following a star schema design, and comparing query performance between CSV and Parquet file formats.

The goal is to showcase:
- ETL pipeline creation
- Star schema modeling
- Columnar storage benefits
- Analytical query benchmarking

---
### Project Structure
```
duckdb-star-schema/
├── data/
│   ├── sales.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── stores.csv
├── scripts/
│   ├── etl.py
│   ├── benchmark.py
├── output/
│   ├── dim_customers.parquet
│   ├── dim_products.parquet
│   ├── dim_stores.parquet
│   ├── fact_sales.parquet
├── benchmarks/
│   └── results.txt
├── warehouse.duckdb
└── README.md
```

Dimension Tables
- dim_customers: customer details
- dim_products: product information
- dim_stores: store and region data
--- 
### ETL Process
- Ingest multiple CSV files using DuckDB
- Create raw staging tables
- Transform data into star schema format
- Export fact and dimension tables to Parquet
- Store data in a DuckDB database file
---
### Analytical Queries
The following analytical queries are executed:
- Top products by total revenue
- Sales by store region
- Top customers by spending
  
These queries are run on:
- Raw CSV-based tables
- Parquet-based tables

---
### Performance Benchmarking
Benchmarking compares execution time of identical queries on:
- CSV files
- Parquet files

  Run Benchmarking:
 ```
  python scripts/benchmark.py
```
Results are stored in:
```
benchmarks/results.txt
```
---
### Observations & Conclusion

- Parquet consistently outperforms CSV for analytical queries
- Columnar storage reduces I/O and improves aggregation speed
- DuckDB is highly efficient for local analytical workloads
- Star schema simplifies query logic and improves readability
