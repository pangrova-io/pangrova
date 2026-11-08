# TPC-H engineering workload

`workload.json` binds all 22 templates to exact SQL files, qualification parameters,
source hashes, projection metadata and recorded dialect adaptations. It targets
SF100. Use `prepare_tpc_workload.py` for another SF; do not relabel an existing
workload or a synthetic fixture.

The source is checksum-pinned community TPC-H **2.14.0**, while the current official
specification target is **3.0.1**. These engineering results are not certified QphH.
Q15 uses a CTE; row-count directives become LIMIT and interval precision is
normalized. Every query still requires full reference validation.

Use `scripts/run-tpc-suite.sh` for persistent-client full-suite measurements,
shared snapshot verification, agreed Spark/Trino reference results and three
independent windows. See [the runbook](../../../../docs/testing/tpc-benchmark-methodology.md) for the
acceptance matrix and current evidence limits.

The older `scripts/run-tpc-benchmark.sh` remains a separate serial CLI-process
end-to-end experiment. It requires FINISHED, completely consumed results, a unique
query ID and the exact original UTF-8 SQL SHA256. It sends SQL intact to the server;
no statement splitting or runtime rewriting occurs. Its latency includes JVM
startup, transport and decode. Token values stay in the environment: pass only
`--token-env NAME`.

Three samples provide descriptive statistics only, never P99. The unchanged strict
TPC-H tail gate requires all 22 queries, raw samples, R7 interpolation, at least
1000 completed measured samples per query and matching warmup/query/process
deadlines. The sample floor is repository evidence policy, not an official TPC
rule or a tail-confidence guarantee. Zero/aggregate-only historical baselines
remain uncalibrated; never convert maxima or fixtures into calibration.
