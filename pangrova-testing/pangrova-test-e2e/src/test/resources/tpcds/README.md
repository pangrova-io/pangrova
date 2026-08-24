# TPC-DS Reference Files

Standard TPC-DS benchmark queries (10 core queries) and precomputed expected results for Pangrova E2E correctness testing.

## Directory Structure

```
tpcds/
├── queries/       # Standard TPC-DS SQL files (10 core queries), unqualified table names
│   ├── q01.sql    # Store Returns by Customer
│   ├── q03.sql    # Item Revenue by Brand
│   ├── q07.sql    # Promotion Revenue
│   ├── q13.sql    # Store Sales Demographics
│   ├── q19.sql    # Store Sales by Manager
│   ├── q25.sql    # Store/Catalog Returns
│   ├── q43.sql    # Store Sales by Day of Week
│   ├── q52.sql    # Web Sales by Department
│   ├── q55.sql    # Brand Revenue by Month
│   └── q96.sql    # Store Sales by Household Demographics
├── expected/      # Precomputed JSON result files (SF=0.01, DuckDB reference)
│   ├── q01.json
│   ├── q03.json
│   ├── q07.json
│   ├── q13.json
│   ├── q19.json
│   ├── q25.json
│   ├── q43.json
│   ├── q52.json
│   ├── q55.json
│   └── q96.json
└── README.md
```

## SQL Files

- Use **unqualified table names** (e.g., `store_sales`, `date_dim`). The `TpcDsCorrectnessTest` handles catalog/schema prefixing at runtime.
- Standard TPC-DS table names: `store_sales`, `store_returns`, `catalog_sales`, `catalog_returns`, `web_sales`, `web_returns`, `customer`, `customer_address`, `customer_demographics`, `date_dim`, `household_demographics`, `income_band`, `item`, `promotion`, `reason`, `ship_mode`, `store`, `time_dim`, `warehouse`, `web_page`, `web_site`, `call_center`, `catalog_page`, `inventory`.
- SQL syntax targets Calcite-compatible dialect (Pangrova's SQL engine).
- Each file contains exactly one query.

## Expected Result Files

- JSON format: `{"note": "...", "query": "...", "scaleFactor": 0.01, "rows": [...]}`.
- Results generated using DuckDB at SF=0.01 as reference engine.
- For floating-point columns, use 2 decimal places precision.
- Results are ordered as specified by each query's ORDER BY clause.

## Query Selection Rationale

The 10 core queries (Q1, Q3, Q7, Q13, Q19, Q25, Q43, Q52, Q55, Q96) were selected to cover:
- Simple aggregations (Q96, Q55)
- Multi-table joins (Q1, Q7, Q13, Q19, Q25)
- Date dimension filtering (Q3, Q43, Q52)
- Subqueries (Q1)
- Various fact tables (store_sales, catalog_sales, web_sales, store_returns)

## Usage

`TpcDsCorrectnessTest` loads SQL from `queries/qNN.sql`, prefixes table names with the test catalog/schema, executes through the real pipeline, and compares results against `expected/qNN.json`.
