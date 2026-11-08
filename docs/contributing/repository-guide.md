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
| `pangrova-parser` / `pangrova-planner` | Parser API/Core and Planner API/Core; SDK-free contracts with implementation-owned ANTLR/Calcite |
| `pangrova-protocol` | Canonical wire contracts and build-time function-contract generators |
| `pangrova-query-api` | Execution identities, immutable journal/lineage facts and query audit/recovery ports; no Query runtime implementation dependency |
| `pangrova-generation` | Durable generation jobs: core contracts/codecs/routing, independent transaction SPI and PostgreSQL job/stage/exchange implementation |
| `pangrova-schema-migrations` | Resource-only canonical PostgreSQL migrations; runtime bootstrap and owner test fixtures consume the same version sequence |
| `pangrova-query` | Query runtime services, profiling/audit orchestration and JDBC/in-memory durable state implementations |
| `pangrova-coordination` | Independent coordination API, Coordinator service, JDBC, InMemory and optional ZooKeeper providers; providers depend on the API, not the process implementation |
| `pangrova-worker` | Worker process, task lifecycle, transport and JNI native bridge |
| `pangrova-worker-native` | C++20 execution, JNI, Velox adapters and Lance FFI |
| `pangrova-native-bundle` | Platform-specific JNI library packaging |
| `pangrova-connector` | Iceberg, Paimon and Lance connector modules |
| `pangrova-metadata-core` | Durable metadata, catalog and coordination persistence |
| `pangrova-table` | Table API/Core family: ownership, observation, maintenance, statistics and governance binding |
| `pangrova-file` | File API/Core/Worker Adapter family: Volume/Stage contracts, authorization, URL issuance, directory refresh and delegated Worker HTTP operations |
| `pangrova-semantic` | Semantic View identity, immutable versions, definition validation, publication fencing and JDBC storage |
| `pangrova-ai` | Shared AI descriptors, governed control plane, and optional Worker-side AI runtime |
| `pangrova-filesystem` | SDK-free Filesystem API and independent LOCAL/S3/OSS providers; backend capability limits are documented in the File architecture |
| `pangrova-materialization` | Dynamic-table/materialized-view domain kernel and JDBC state store |
| `pangrova-streaming` | CDC and streaming infrastructure: API, Core, format/writer providers and PostgreSQL persistence |
| `pangrova-shuffle` | Exchange contracts and deployable shuffle service |
| `pangrova-operations` | DDL/DML operation infrastructure |
| `pangrova-scheduler` | Cluster admission, quota/resource placement, scheduling strategies and fair/load-balanced execution order |
| `pangrova-security` / `pangrova-governance` | Authentication, authorization, classification, lineage and quality governance |
| `pangrova-indexing` | API/Codec/Core family; unified Indexing API and SPI replace the retired central SPI; strategy, JDBC and planner implementation separation remains in progress |
| `pangrova-observability` | Observability contracts/no-op implementations, OpenTelemetry providers and adapters; Query audit persistence belongs to `pangrova-query` |
| `pangrova-transport` / `pangrova-client` | Client result transport, REST, JDBC and CLI |
| `pangrova-standalone` | Single-JVM Coordinator/Worker assembly |
| `pangrova-testing` | Shared test infrastructure, E2E, benchmark, chaos and reference generator |
| Root community files | README, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, CHANGELOG, LICENSE and NOTICE |

Module presence is not an end-to-end capability guarantee. See
[capability status](../getting-started/capability-status.md) and
[architecture](../architecture/overview.md).

## Documentation ownership

The [documentation guide](documentation.md) owns the directory conventions.
`getting-started`, `sql-reference`, `data-types`, `indexing`, `configuration` and
`deployment` serve users. `architecture`, `development`, `testing`, `contributing`
and `operations` serve contributors and operators.

`docs/internal/` retains design decisions, audits and historical evidence excluded
from the website. It remains public in a public repository. Do not delete it merely
because its documents are absent from the sidebar: production comments, tests and
public documents reference some of that evidence. Review confidential content and
stale claims separately from deleting unused assets.

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

## Consolidation decisions

Removed the unused `generate-tpc-data.sh`, `start-benchmark-cluster.sh` and
`stop-benchmark-cluster.sh` forwarding aliases. Use their Python implementations
above. Repository callers already used those implementations; no benchmark logic
was removed.

Removed `generate-monthly-report.sh`. It duplicated comparison setup and selected a
result by filename sorting. Request the existing comparison/report entry point
with an explicit result and matching baseline instead (from the benchmark module):

```bash
mkdir -p reports
HISTORY_DIR=results REPORT_OUTPUT=reports/tpch-regression.txt \
  scripts/tpch-regression-check.sh results/selected-run.json baseline/reviewed-baseline.json
```

The comparator still rejects incomplete or uncalibrated evidence and returns its
original exit status. Scheduling is an operator decision; this command does not
create a scheduler or send a notification unless explicitly configured.

The following similar scripts remain separate for concrete reasons:

- `pangrova-coordinator.sh`, `pangrova-worker.sh` and `pangrova-standalone.sh` share
  launcher mechanics but differ in main class, configuration and JVM defaults.
  A future shared launcher should preserve their public names and test argument
  forwarding, environment precedence, classpath resolution, signals and PID
  ownership, followed by the affected startup/integration chain. This review did
  not change startup behavior.
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

README and the website share `website/static/img/logo.png`. The architecture in the
README is maintained as Mermaid source; obsolete target-architecture PNG/HTML
artwork and the duplicate logo were removed. The redundant resource-directory
placeholder was removed; the functional Maven root marker and Pages `.nojekyll`
marker remain.
