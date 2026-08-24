# TPC-H Query Files

Standard TPC-H benchmark queries (22 total) adapted for Pangrova SQL dialect.
These queries are used for SF=100 (100GB) correctness and performance testing.

## Query List

| Query | Name | Key Operations |
|-------|------|----------------|
| Q01 | Pricing Summary Report | Full scan + filter + aggregation |
| Q02 | Minimum Cost Supplier | Correlated subquery + multi-table join |
| Q03 | Shipping Priority | 3-table join + filter + top-N |
| Q04 | Order Priority Checking | Semi-join + date range filter |
| Q05 | Local Supplier Volume | 6-table join + aggregation |
| Q06 | Forecasting Revenue Change | Single table scan + range predicates |
| Q07 | Volume Shipping | Multi-table join + CASE expression |
| Q08 | National Market Share | 8-table join + division + CASE |
| Q09 | Product Type Profit Measure | 6-table join + expression evaluation |
| Q10 | Returned Item Reporting | 4-table join + filter on return flag |
| Q11 | Important Stock Identification | Subquery + HAVING threshold |
| Q12 | Shipping Modes and Order Priority | 2-table join + CASE aggregation |
| Q13 | Customer Distribution | Left outer join + nested aggregation |
| Q14 | Promotion Effect | 2-table join + conditional aggregation |
| Q15 | Top Supplier | CTE + max subquery |
| Q16 | Parts/Supplier Relationship | Anti-join + GROUP BY + COUNT DISTINCT |
| Q17 | Small-Quantity-Order Revenue | Correlated subquery + AVG |
| Q18 | Large Volume Customer | Subquery + GROUP BY HAVING + top-N |
| Q19 | Discounted Revenue | Complex OR predicates + join |
| Q20 | Potential Part Promotion | Nested subqueries + semi-join |
| Q21 | Suppliers Who Kept Orders Waiting | Multi-level EXISTS/NOT EXISTS |
| Q22 | Global Sales Opportunity | Subquery + NOT EXISTS + AVG |

## Usage

```bash
# Run all queries against Pangrova cluster
./scripts/run-tpc-benchmark.sh --benchmark tpch --scale-factor 100

# Run specific query
pangrova-cli --file queries/tpch/q01.sql
```

## Data Generation

```bash
./scripts/generate-tpc-data.sh --benchmark tpch --scale-factor 100
```
