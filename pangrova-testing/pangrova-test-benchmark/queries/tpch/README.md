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

# Qualified P99 regression input (at least 1000 completed samples per query)
./scripts/run-tpc-benchmark.sh --benchmark tpch --scale-factor 100 \
  --iterations 1000 --warmup 1 --schema sf100 --output results/tpch-sf100.json

# Authenticated cluster: populate the variable through your secret mechanism.
# Only its name is passed in argv; the CLI reads the token from its environment.
./scripts/run-tpc-benchmark.sh --benchmark tpch --scale-factor 100 \
  --token-env PANGROVA_BENCHMARK_TOKEN --cli-path ../../bin/pangrova-cli.sh

# Run specific query
pangrova-cli --file queries/tpch/q01.sql
```

The runner uses the batch CLI's `--execution-receipt` contract and requires
`FINISHED`, fully consumed results, a query ID and the SHA256 of the exact original
UTF-8 SQL file. A process exiting successfully without that receipt fails the run.
Missing query files fail preflight; failed warmup is recorded as failed and never
followed by measured iterations for that query. SQL is sent intact to the server's
parser. The runner does not split statements or rewrite query files.

`--token-env` requires a set, nonblank environment variable and the current CLI
with environment authentication support. The token is never placed in command
arguments, JSON reports or runner logs. CLI failure reports preserve the exit
code and execution phase; they do not copy arbitrary client/server stderr, which
may contain credentials or local paths. `--timeout` is the overall subprocess
deadline including JVM startup (default 600 seconds). `--query-timeout` is the
CLI query deadline after submission (default 300 seconds); the process deadline
must be larger so the CLI has time to cancel and close after its query deadline.
Hard process termination does not prove remote query or worker resource drain.

Latency uses the monotonic performance counter and includes CLI process startup,
submission, result transfer and decoding. It measures a serial client workload,
not isolated engine operator time. Schema defaults to `sf<scale-factor>`.

Percentiles use explicit Hyndman-Fan **R7** linear interpolation. Output retains
raw samples, distinct measured/warmup query IDs, `sampleCount`, `minimumSamples`
and `percentileQualified`. The default three-iteration run reports P50, mean,
minimum and maximum; it does **not** emit P99. P99 requires at least 1000 completed
measured samples per query. This is a repository evidence policy, not a claim that
1000 requests alone establish a narrow tail confidence interval.

The regression comparator requires all 22 queries, raw samples, matching SQL
digests, scale, timing scope, R7 method and warmup/process/query deadline settings. It recomputes
P50/P99 from samples. Calibration uses the same record structure, with
`baselineP50Ms`/`baselineP99Ms` in place of the result percentile fields and a
`regressionThreshold` (default 0.15). Maximum-only historical summaries, zero
baselines and insufficient samples require real recalibration. Do not convert
old maxima into R7 P99 values or replace baseline numbers with test fixtures.

These adapted query latency runs are not certified TPC-H QphH scores. See the
[performance audit](../../../../docs/internal/ai-native-research/performance-current-audit.md)
for official sources, checks and remaining evidence gaps.

## Data Generation

```bash
./scripts/generate-tpc-data.sh --benchmark tpch --scale-factor 100
```
