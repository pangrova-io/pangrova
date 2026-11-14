# TPC-DS Plan Stability Coverage — Remaining Queries

Currently covered: **Q1-Q31** (31 of 99).

## Remaining work — adapt Q32-Q99 from the DuckDB reference set

The DuckDB-bundled TPC-DS query set (`deps-download/duckdb/extension/tpcds/dsdgen/queries/NN.sql`) contains all 99 queries in the canonical TPC-DS dialect. Each must be adapted before it can be added here:

1. **Implicit joins -> ANSI JOIN.** `FROM a, b WHERE a.x = b.y` becomes `FROM a JOIN b ON a.x = b.y`.
2. **`LIMIT n;` -> `FETCH FIRST n ROWS ONLY`.** Calcite parses both, but `FETCH FIRST` is what the rest of the suite uses.
3. **Header comment.** Add a `-- TPC-DS QNN: <short description>` header so log output and golden-file diffs are self-describing.
4. **Trailing semicolon.** Remove — the test harness does not expect one.

The test class (`TpcdsPlanStabilityTest`) picks up new queries automatically via directory scan, so dropping a new `qNN.sql` file in here is the only code change required. The first run generates the golden plan under `golden-plans/tpc-ds/qNN.plan`; review it and commit.

## Status table

| Range | Status |
|-------|--------|
| Q1-Q30 | Covered (first batch, Task 11.2) |
| Q31 | Covered (extension example, Task 29.1) |
| Q32-Q99 | TODO (Task 29.1 follow-up) |

## Why not auto-import?

The DuckDB query files use implicit-join syntax which Calcite *does* parse, but the resulting plans are sensitive to join-reorder rule application order. Hand-adapting to ANSI JOIN syntax produces stable plans that match the style of the existing Q1-Q30 set and makes plan diffs reviewable.
