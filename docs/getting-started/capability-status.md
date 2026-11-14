---
title: "Current Capability Status"
description: "Implemented execution paths, default wiring gaps, and qualification limits"
---

# Current Capability Status

Use this page before choosing SQL features or evaluating a deployment. It summarizes
the current source and default bootstrap wiring. **An implemented path is not a
production qualification result.** Tests, examples, and interfaces are separate evidence;
the exact connector, query shape, deployment, and failure scenario still need validation.

## Reading the Documentation

| Document | What it establishes |
|---|---|
| This page and statement-specific limitations | Current wiring and known gaps |
| [Project positioning](project-positioning.md) | Intended problems, advantages, and proposed customer profiles |
| [SQL reference](../sql-reference/index.md) | Syntax, dispatch boundaries, and runtime prerequisites |
| [Architecture](../architecture/index.md) | Implementation details and explicitly labelled design constraints or targets |
| [Production checklist](../deployment/production-checklist.md) | Work required before deployment acceptance |

## Execution and Lakehouse Access

| Capability | Current status | Boundary |
|---|---|---|
| Relational queries | Java planning and Native Worker execution are wired | Supported operators, types, formats, and distributed behavior must be checked for the workload |
| Relational DML, including `INSERT VALUES` | Routed through planner → Worker → Native, with Worker-side connector SDK sink callbacks | Workers return write fragments; Java owns the final fenced connector catalog commit. Distributed-write qualification remains open |
| Catalog/schema/table DDL | Java metadata dispatch is wired | Connector support and authenticated session context apply; a DDL handler is not proof of all table-format features |
| Volume and Stage DDL | Definitions, RBAC, audit, and the file access-ticket flow are wired against the metadata authority | `SHOW`/`DESCRIBE` plus `LIST @stage`/`REMOVE @stage` for SQL. File transfer (single-request, ranged, multipart), recursive listing, version-checked delete, presigned URLs, and role-authorized stable Stage URLs work through the [file access API](catalogs/volumes-stages-files.md). Manual `ALTER STAGE ... REFRESH` publishes a durable directory index after a complete Worker listing. Native Directory projections, automatic index updates, `PUT`/`GET`, SQL URL functions, and `COPY INTO`/`read_files` ingestion are not implemented |
| Iceberg and Paimon | Connector implementations exist | Table-format capability does not imply equivalent Pangrova SQL support. Paimon merge/file policies have explicit limits |
| Cross-catalog queries | Shared planning and connector paths exist | Validate the actual join and pushdown shape; no qualified performance advantage is claimed |
| Standalone `COMPACT`, `OPTIMIZE`, `VACUUM` | Registered; the default bootstrap injects `ControlledTableMaintenance` into `MaintenanceExecutor` | A successful statement does not prove files were rewritten or removed. `OPTIMIZE` maps to compaction; `VACUUM` maps only to orphan cleanup, not snapshot expiration |
| Catalog maintenance `CALL` | Distinct connector procedure dispatch is wired | Use the connector-specific procedure and arguments. This is not the same path as the standalone statements |
| `COPY INTO` | Subquery form executes through the relational `INSERT ... SELECT` write path | URI sources and file-loading options fail closed in the parser. Server-side file ingestion is not implemented |
| Clustering DDL / `RECLUSTER` | Parser and executor/task code exist; no default production registration found | Syntax examples describe partial work, not a usable maintenance lifecycle |
| Server-side `SET` | Registered handler logs the property and returns an empty result | It does not mutate server session state. Supply supported context in query requests or client connection settings |
| Catalog YAML auto-registration | Documented format, automatic startup loading remains planned | Use SQL `CREATE CATALOG` for the documented registration path |

Source evidence: [statement bootstrap](https://github.com/pangrova/pangrova/blob/main/pangrova-server/pangrova-server-coordinator/src/main/java/org/pangrova/server/coordinator/bootstrap/QueryExecutionBootstrap.java),
[plan dispatch](https://github.com/pangrova/pangrova/blob/main/pangrova-coordination/pangrova-coordination-control/src/main/java/org/pangrova/coordinator/query/execution/dispatch/PlanExecutionFactory.java),
[maintenance executor](https://github.com/pangrova/pangrova/blob/main/pangrova-coordination/pangrova-coordination-control/src/main/java/org/pangrova/coordinator/query/statement/MaintenanceExecutor.java),
[DDL executor](https://github.com/pangrova/pangrova/blob/main/pangrova-coordination/pangrova-coordination-control/src/main/java/org/pangrova/coordinator/query/statement/DdlExecutor.java),
and [Worker connector write bridge](https://github.com/pangrova/pangrova/blob/main/pangrova-worker/src/main/java/org/pangrova/worker/write/NativeWriteExecutor.java).
See [DML](../sql-reference/dml.md), [maintenance](../sql-reference/maintenance.md),
[session context](../sql-reference/session.md), and [catalog management](catalogs/catalog-management.md).

## AI and Retrieval

| Capability | Current status | Boundary |
|---|---|---|
| Vector distance SQL | Native functions and relational ordering path exist | Distance ordering alone does not prove ANN acceleration |
| ANN retrieval | Rewriter and Native index/artifact paths exist | Requires an eligible distance-order/positive-limit query and a matching index binding. An unbound ordinary query can remain exact SQL; a bound ANN task must obey its explicit fallback policy |
| Full-text / hybrid retrieval / reranking | Components and bounded planning paths exist | Required artifacts, capabilities, model bindings, and supported query shapes apply. Filters are not proof of ANN prefiltering; arbitrary OR/NOT, aggregation, and multi-vector fusion are not a blanket support contract |
| Text-to-vector query shorthand | Rule code exists but is not wired into the default planner | Use explicit model invocation where supported; do not assume text literals automatically become query vectors |
| `AI_EMBED`, `AI_COMPLETE`, and other model functions | Native expressions invoke a governed Worker-local provider callback | Models, provider endpoints, credentials, modalities, and exact capabilities must be configured; a SQL function does not bundle a model service |
| `FILE_FROM_URL` | HTTPS importer and governed blob storage are implemented | Requires an allowed HTTPS host and importer/storage limits. It does not accept arbitrary S3, HDFS, HTTP, or other source schemes |
| `FILE` values | Governed claim-check references | The stored URI is an opaque `pangrova-blob://` reference, not a public storage credential or an unrestricted URL |
| Feature Store / training datasets / embodied workflows | Partial components or product direction | Complete serving, training-export, and operational lifecycles are not established |

Source evidence: [vector rewriter](https://github.com/pangrova/pangrova/blob/main/pangrova-indexing/pangrova-indexing-lifecycle/src/main/java/org/pangrova/indexing/planner/vector/VectorSearchRewriter.java),
[retrieval intent collection](https://github.com/pangrova/pangrova/blob/main/pangrova-coordination/pangrova-coordination-control/src/main/java/org/pangrova/coordinator/query/execution/retrieval/RetrievalIntentCollector.java),
and [HTTPS importer](https://github.com/pangrova/pangrova/blob/main/pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/blob/HttpsBlobImporter.java).
Read the [vector reference](../sql-reference/functions/vector-search.md),
[AI functions](../sql-reference/functions/ai-functions.md), and [FILE type](../data-types/ai-native/file.md)
for examples and prerequisites.

## Streaming and Refresh

| Capability | Current status | Boundary |
|---|---|---|
| Stream DDL | Registered when the streaming bootstrap supplies its executor | Registration does not qualify ingestion, recovery, or table-format correctness |
| CDC data writes | Current bootstrap creates a Coordinator-local `ConnectorPageSinkCdcWriter` | This is a known exception to the Native data-plane target. Catalog publication and source offsets are not one atomic transaction; no exactly-once/effectively-once production guarantee is established |
| Continuous-query refresh | Default scheduler callback logs a refresh tick | It does not execute the refresh query |
| Dynamic-table lifecycle | Create/alter/drop/show/describe handlers register when durable metadata publication is configured; the planner rewrites published targets | Continuous-query materialization returns unsupported, and automatic refresh scheduling or exactly-once refresh is not established |
| Complete automatic materialized-view lifecycle | Definitions can be persisted and recovered for query rewriting | No materialized-view DDL/refresh execution path is registered; do not infer an automatic create/refresh/recovery service from rewrite classes or refresh syntax |

Source evidence: [streaming bootstrap](https://github.com/pangrova/pangrova/blob/main/pangrova-server/pangrova-server-coordinator/src/main/java/org/pangrova/server/coordinator/bootstrap/StreamingBootstrap.java),
[planner wiring](https://github.com/pangrova/pangrova/blob/main/pangrova-server/pangrova-server-coordinator/src/main/java/org/pangrova/server/coordinator/bootstrap/PlannerWiring.java),
and the statement bootstrap linked above. See [streaming SQL](../sql-reference/streaming.md)
and the [current CDC boundary](../architecture/streaming.md).

## Deployment and Evidence Limits

- Multi-Coordinator lease/fencing components exist. The default lease backend is JDBC;
  ZooKeeper is optional. Availability also depends on the durable database, catalog,
  object store, network, and deployment configuration. Multiple Coordinators alone
  do not remove every single point of failure.
- Native execution uses Velox representations; [Native shuffle serialization](https://github.com/pangrova/pangrova/blob/main/pangrova-worker-native/src/engine/shuffle/ShuffleBatchSerializer.cpp) produces standard
  Arrow stream batches for Flight. Worker write staging can call Java connector
  page sinks through JNI, including conversion for row-only sinks. Native planning
  does not make every physical I/O operation a pure C++ path. JNI, IPC, conversion,
  compression, and network transfers must be measured
  individually before claiming zero-copy behavior.
- SQL `GRANT`/`REVOKE` now parses and dispatches through authenticated,
  PostgreSQL-backed privilege administration, including grant options,
  RESTRICT/CASCADE and durable receipts. Component tests cover real SQL dispatch
  and database authority. `SHOW PRIVILEGE MUTATION` confirms the original
  submitter's receipt. An authenticated HTTP/Native SQL test proves that SQL-issued
  grants enable actual reads and SQL revokes deny subsequent reads; OAuth2/LDAP
  use separate authentication integration tests. Overall release qualification
  still depends on the full repository gates. See
  [the supported syntax and limits](../architecture/security.md#sql-privilege-administration).
- Security, resource governance, and observability have implementations and wiring.
  They require configuration and workload/failure validation; their presence does not
  establish production readiness.
- Overall production qualification remains open. Recorded component successes must
  be read alongside failed, skipped, or unexecuted gates. This documentation audit did
  not rerun the Java/Native runtime qualification chain.
- No qualified comparative baseline establishes speedup multiples, cost savings,
  PB-scale capacity, unlimited scaling, or semantic-cache hit-rate improvements.
  Customer profiles describe intended users; they are not verified customer deployments.

Current status and open items are tracked in the [Roadmap](roadmap.md) and the
focused tests named in the supported-capability rows. These records are dated
evidence, not promises that the current checkout or another platform has passed
the same checks.
