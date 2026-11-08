# TPC-DS engineering workload

The canonical `workload.json` contains **99 templates and 103 explicit steps**.
Q14, Q23, Q24 and Q39 each have two results. Execute the step list with
`run-tpc-suite.sh`; concatenated legacy qNN.sql files are not single-statement CLI
inputs and must not be truncated to their first statement.

SQL is generated from the pinned community **2.10.0** kit using qualification
parameters and the Netezza template dialect, with documented ANSI day intervals,
qualified ORDER BY references and reserved-alias adaptations. Source/template/SQL
hashes and outstanding semantic review are explicit. The target official
specification is **4.0.0**; source-kit age and adaptations remain certification gaps.

SF100 is an engineering scale. Formal TPC-DS 4.0 scales start at SF1000 (1TB).
A successful SQL run or 99 filenames is not full correctness or a QphDS score.
Reference truth requires Spark and Trino to agree on every typed result, including
legitimate empty sets, on identical shared Iceberg snapshots.

See [the methodology](../../../../docs/testing/tpc-benchmark-methodology.md) and
[the provenance notice](../NOTICE.md).
