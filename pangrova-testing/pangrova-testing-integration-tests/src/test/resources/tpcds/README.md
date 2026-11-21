# TPC-DS test ownership

`TpcDsCorrectnessTest` retains eight representative SQL-pattern tests on four
simplified synthetic tables. These fixtures are neither an official SF nor the
99-template benchmark.

The uncalled ten-query resources, placeholder expected results, generator that
rounded DECIMAL through floating point, and schema/nonempty fallback helper have
been removed. The complete 99-template/103-step engineering workload is owned by
`pangrova-testing-benchmark/queries/tpcds/workload.json`.

Full-result qualification now requires independently captured Spark and Trino
results on the identical dataset and Iceberg snapshots; missing references fail.
An agreed empty result is valid. See
[the benchmark methodology](../../../../../../docs/testing/tpc-benchmark-methodology.md).
