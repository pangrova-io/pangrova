---
title: "Repository and Script Guide"
description: "Directory ownership, script entry points, and maintenance decisions"
---

# Repository and Script Guide

Use this guide to locate an owner before adding a file or a new command. The
[script inventory](script-inventory.md) records every tracked Shell/Python file
examined during the repository review, its consumers, and retention decision.
The root README is an introduction; development inventories belong here.

## Repository structure

| Path | Responsibility and retention |
|---|---|
| `.github/` | CI/release/website workflows, issue/PR templates, CODEOWNERS and dependency updates |
| `.mvn/` | Maven root-discovery anchor; keep `.gitkeep` because plugin paths depend on it |
| `.pangrova/rules/` | Canonical engineering policy; keep `AGENTS.md` as the entry point |
| `.pangrova/scripts/` | Repository gates and CI preparation; tests are under `tests/` |
| `bin/` | Supported process/client/operator commands |
| `config/` | Checkstyle and SpotBugs configuration; not runtime deployment configuration |
| `etc/` | Runtime and catalog templates consumed by launch scripts |
| `deploy/docker/` | Production and native-builder images, Compose and source verification |
| `deploy/helm/pangrova/` | Kubernetes chart, values, templates and namespace-isolation verification |
| `docs/` | User, developer, design and operational documentation |
| `website/` | Docusaurus rendering, navigation, blog and shared static images |
| `pangrova-common` / `pangrova-config` | Shared primitives and typed configuration |
| `pangrova-types` | Connector-independent logical types, signatures, declarations and scalar/row values |
| `pangrova-batch` | Block/page values and Arrow decode/projection; readers borrow allocator-owned memory |
| `pangrova-function` | Function metadata and scalar/aggregate/table extension contracts |
| `pangrova-execution` | Task stage/purpose/deadline values and canonical execution binding |
| `pangrova-parser` / `pangrova-planner` | Separate single-artifact domains containing their public contracts and ANTLR/Calcite implementations |
| `pangrova-protocol` | Canonical wire contracts and build-time function-contract generators |
| `pangrova-generation` | Durable generation jobs: core contracts/codecs/routing; PostgreSQL job/stage/exchange storage lives in `pangrova-persistence/pangrova-persistence-generation-postgresql` |
| `pangrova-schema-migrations` | Resource-only canonical PostgreSQL migrations; runtime bootstrap and owner test fixtures consume the same version sequence |
| `pangrova-query` | Query runtime services and profiling/audit orchestration; JDBC/in-memory durable state lives in `pangrova-persistence/pangrova-persistence-query-*` |
| `pangrova-coordination` | Coordination API/Core plus the optional ZooKeeper provider; PostgreSQL and Memory persistence providers live under `pangrova-persistence/pangrova-persistence-coordination-*` |
| `pangrova-server` | Coordinator and standalone process assembly, bootstrap and HTTP entry points |
| `pangrova-worker` | Worker process, task lifecycle, transport and JNI native bridge |
| `pangrova-worker-native` | C++20 execution, JNI, Velox adapters and Lance FFI |
| `pangrova-native-bundle` | Platform-specific JNI library packaging |
| `pangrova-connector` | Iceberg, Paimon and Lance connector modules |
| `pangrova-metadata` | Metadata API/Core, catalog resolution, projection and synchronization |
| `pangrova-table` | Single artifact owning table control, observation, maintenance, statistics, and governance binding |
| `pangrova-file` | Single artifact owning Volume/Stage contracts, authorization, URL issuance, and directory refresh; Worker endpoint assembly lives in the Worker domain |
| `pangrova-semantic` | Semantic View identity, authorized creation, immutable definition validation, version listing and fenced publication. The PostgreSQL provider owns the active-version CAS and uses the Server-supplied data source |
| `pangrova-ai` | Shared AI descriptors, governed control plane, and optional Worker-side AI runtime |
| `pangrova-filesystem` | SDK-free Filesystem API and independent LOCAL/S3/OSS providers; backend capability limits are documented in the File architecture |
| `pangrova-materialization` | Dynamic-table/materialized-view domain kernel; relational state stores live in `pangrova-persistence/pangrova-persistence-materialization-postgresql` |
| `pangrova-streaming` | CDC and streaming infrastructure: API, Core, format/writer providers and PostgreSQL persistence |
| `pangrova-shuffle` | Exchange contracts and deployable shuffle service |
| `pangrova-operations` | DDL/DML operation infrastructure |
| `pangrova-scheduler` | Cluster admission, quota/resource placement, scheduling strategies and fair/load-balanced execution order |
| `pangrova-security` / `pangrova-governance` | Authentication, authorization, classification, lineage and quality governance |
| `pangrova-indexing` | API, codec, lifecycle, and built-in strategy artifacts; PostgreSQL persistence is owned by the corresponding persistence modules |
| `pangrova-observability` | Observability contracts/no-op implementations, OpenTelemetry providers and adapters; Query audit ports live in `pangrova-persistence/pangrova-persistence-query-api` and stores in `pangrova-persistence/pangrova-persistence-query-*` |
| `pangrova-transport` / `pangrova-client` | Client result transport, REST, JDBC and CLI |
| `pangrova-standalone` | Single-JVM Coordinator/Worker assembly |
| `pangrova-testing` | Shared test infrastructure, E2E, benchmark, chaos and reference generator |
| Root community files | README, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, CHANGELOG, LICENSE and NOTICE |

Module presence is not an end-to-end capability guarantee.
See the [architecture overview](../architecture/overview.md) for how the modules
fit into the execution path.

## Documentation ownership

The [documentation guide](documentation.md) owns the directory conventions.
`getting-started`, `sql-reference`, `data-types`, `indexing`, `configuration` and
`deployment` serve users. `architecture`, `development`, `testing`, `contributing`
and `operations` serve contributors and operators.

`docs/` contains only published material. Historical audits, execution ledgers, and
review evidence are not retained as repository documents; use git history and CI
records when tracing a past change. Do not recreate the retired non-published documentation directory.

Keep lockfiles, patches, protocol descriptors, golden results and test certificates:
they are reproducibility inputs or test fixtures. A generated-looking file can be
an intentionally checked-in oracle. Do not blanket-delete `.bin`, `.pem`, `.lock`
or `.gitkeep` files.

## Command entry points

| Task | Entry point |
|---|---|
| Manage local processes | `bin/pangrova.sh` delegates to role-specific commands |
| Build the pinned Iceberg SDK | `.pangrova/scripts/build-iceberg-sdk-candidate.sh` |
| Build native dependencies, Lance, engine and JNI package | `pangrova-worker-native/bin/build-native.sh` |
| Check native tools without installing packages | `pangrova-worker-native/bin/setup-native-build-env.sh --check-tools` |
| Test native runtime | `python3 pangrova-worker-native/bin/run-native-tests.py` |
| Validate instruction layout | `.pangrova/setup.sh --check` |
| Validate website | `pnpm run typecheck` and `pnpm run build` in `website/` |
| Generate TPC data | `python3 pangrova-testing/pangrova-testing-benchmark/scripts/generate_tpc_data.py --help` |
| Manage an explicit benchmark deployment | `python3 pangrova-testing/pangrova-testing-benchmark/scripts/tpc_cluster.py start --help` (or `stop`) |
| Run complete benchmark suites | `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-suite.sh` |
| Run the distinct CLI latency experiment | `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-benchmark.sh` |
| Reference SQL generator | `pangrova-testing/pangrova-testing-fuzzer/scripts/run-operator-fuzzer.sh`; DuckDB only |

The benchmark [methodology](../testing/tpc-benchmark-methodology.md) defines evidence
requirements. Keep JMH, complete-suite experiments and the 22-query tail experiment
separate: they measure different things and enforce different contracts.

## Regression reports

Produce a TPC-H regression report from an explicit result and its matching reviewed
baseline (from the benchmark module):

```bash
mkdir -p reports
HISTORY_DIR=results REPORT_OUTPUT=reports/tpch-regression.txt \
  scripts/tpch-regression-check.sh results/selected-run.json baseline/reviewed-baseline.json
```

The comparator rejects incomplete or uncalibrated evidence and returns its exit
status. Scheduling and notification are operator decisions.

## Similar scripts kept separate


- `pangrova-coordinator.sh`, `pangrova-worker.sh` and `pangrova-standalone.sh` share
  launcher mechanics but differ in main class, configuration and JVM defaults.
  A future shared launcher should preserve their public names and test argument
  forwarding, environment precedence, classpath resolution, signals and PID
  ownership, followed by the affected startup/integration chain.
- `pangrova-cli.sh` has client-specific flags and classpath needs; it should not
  acquire server-role defaults through a blanket launcher merge.
- Container liveness, interactive health, cluster status and recovery measurement
  have different consumers and output/exit contracts. Keep the distinct commands.
- Native release and ASan setup must select separate dependency cohorts. Shared
  package helpers can be extracted only with admission tests; do not merge them
  into one environment with silent fallback.
- AWS source preparation and AWS installation serve Docker and CMake respectively.
  Their shared upstream pin needs coordinated maintenance, not deletion of one path.
- Small Python modules under `function-contracts/` and benchmark tooling are import
  boundaries with tests, not redundant command-line scripts. Keep them cohesive.
- Tests discovered by `unittest` or registered with CTest do not need literal
  callers for every filename. Manual cold-cache and SpotBugs filter diagnostics
  are retained and identified in the inventory.

## Local files and publication

Git ignores local environment files, secrets directories, logs, build products,
IDE state and agent settings. Docker context exclusions also cover website
dependencies and generated caches. `.env.example` templates remain visible.
Use `git status --short` and `git ls-files -ci --exclude-standard` before publishing;
ignore rules do not remove content already committed to history.

README and the website navigation share `website/static/img/logo-transparent.png`.
The architecture PNG is rendered from `website/static/img/architecture-overview.svg`.
