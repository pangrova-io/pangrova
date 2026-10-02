---
title: "AI Native Distributed Architecture Research Source"
audience: "Pangrova maintainers and architecture reviewers"
date: "2026-09-04"
status: "primary-source research baseline"
---

# AI Native Distributed Architecture Research Source

## Scope and direct answer

This report evaluates the control-plane/data-plane boundary and large-scale
distributed execution model needed by Pangrova AI Native. It covers Snowflake,
Apache Spark, Apache Gluten, Substrait and Velox evidence that affects query
scheduling, Native execution, shuffle, resource isolation, retrieval, index
refresh and rollout.

The direct answer is that Pangrova must keep its JVM Coordinator as a bounded,
high-availability control plane and execute every row- or candidate-dependent
operation in the Native Worker data plane. It must retain one distributed
scheduler, one canonical v2 protocol/descriptor set and one columnar exchange
path. Exact Top-K, ANN, BM25, fusion, rerank and candidate fill are Native
relations inside the scheduled DAG; none belongs in Coordinator heap.

The detailed decisions, capacity model and qualification gates are maintained
in [AI Native Snowflake and Gluten Distributed Architecture Review](../ai-native-snowflake-gluten-distributed-architecture-review.md).

## Evidence reconciliation

| Consequential claim | Primary evidence | Confidence | Pangrova decision | Remaining evidence gap |
|---|---|---:|---|---|
| Snowflake separates persistent storage, independent MPP compute and coordinating cloud services. | [Snowflake key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts) | High | Coordinator owns auth/catalog/plan/dispatch; Native Workers own data processing; immutable artifacts live in shared storage. | Snowflake does not publish every internal execution detail; do not infer undocumented algorithms. |
| Snowflake scales a single query primarily by warehouse size and concurrency by independent/multi-cluster warehouses. | [Warehouse considerations](https://docs.snowflake.com/en/user-guide/warehouses-considerations), [Multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster) | High | Separate foreground/search/background pools and distinguish query parallelism from concurrency scale-out. | Pangrova thresholds require its own load evidence. |
| Cortex Search is a durable schema object with independently operable indexing/serving layers, target lag, incremental refresh, compaction and a pinned searchable index. | [Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview), [CREATE CORTEX SEARCH SERVICE](https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search), [ALTER CORTEX SEARCH SERVICE](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search) | High | Model Search Service as catalog metadata plus immutable, atomically published generations; separate refresh compute from serving. | Internal shard/index layout is proprietary and is not copied. |
| Production hybrid retrieval combines lexical/vector candidates and optional reranking, exposes component scores and treats reranking as an explicit latency/quality choice. | [Customizing Cortex Search scoring](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-customize-scoring), [Query a Cortex Search Service](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/query-cortex-search-service) | High | Compile source retrieval, fusion and rerank as separate typed Native stages with explicit budgets and score direction. | Pangrova must establish its own NDCG/MRR and latency baselines. |
| Spark keeps Driver scheduling separate from Executor computation and models work as jobs, stages and tasks. | [Spark cluster mode overview](https://spark.apache.org/docs/latest/cluster-overview.html) | High | Reuse Pangrova StageDAG/attempt/fencing control flow; do not add a second C++ cluster scheduler. | None for the boundary decision. |
| Spark AQE changes physical topology using runtime statistics at shuffle barriers. | [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) | High | Permit epoch-fenced partition coalescing/splitting and reducer fan-in changes, never semantic/snapshot/model changes. | Exact tuning policy must be benchmarked in Pangrova. |
| Executor removal is safe only when shuffle state is preserved by an external service, tracking, decommissioning or reliable plugin. | [Spark dynamic allocation and decommissioning](https://spark.apache.org/docs/latest/job-scheduling) | High | Scale-in must drain or transfer shuffle obligations; fetch loss recomputes the producing stage rather than returning partial data. | Current Pangrova remote shuffle durability still needs implementation and chaos proof. |
| Gluten reuses Spark control flow, converts physical regions to Substrait, executes in Native backends and uses Arrow-compatible columnar batches. | [Apache Gluten architecture](https://github.com/apache/gluten), [Apache Gluten site source](https://github.com/apache/gluten-site/blob/main/index.md) | High | Build maximal typed Native regions and keep retrieval columnar through exchange/result. | Gluten compatibility fallback is deliberately not adopted for AI/retrieval correctness. |
| Native stages need explicit off-heap isolation, spill and stage-sensitive resource profiles. | [Gluten configuration](https://gluten.apache.org/docs/v1.5.0/Configuration.html), [Gluten stage resource adjustment](https://gluten.apache.org/docs/v1.5.0/get-started/VeloxStageResourceAdj.html), [Gluten Velox backend](https://gluten.apache.org/docs/v1.5.0/get-started/Velox.html) | High | One immutable stage grant covers JVM, Native, shuffle, object-store, candidate, provider-token and media budgets. | Stage profile coefficients require production-shaped measurements. |

## Contradictions and bounded conclusions

- Gluten permits explicit row/column transitions and fallback for unsupported
  general SQL operators. That mechanism improves compatibility but is unsafe as
  an implicit AI/retrieval recovery path because it changes placement, memory,
  scoring and failure semantics. Pangrova therefore fails admission or uses a
  typed Native exact fallback recorded in the result envelope.
- Snowflake Cloud Services coordinates query execution, but the official
  architecture assigns query processing to MPP compute. This is not evidence
  for candidate fusion or final Top-K in Pangrova Coordinator JVM.
- Snowflake and Gluten performance claims do not transfer to Pangrova. The
  architecture is accepted only with Pangrova-specific scale, chaos, soak,
  correctness-oracle and rollback evidence.

## Search log and stopping rationale

Discovery covered current official Snowflake architecture, warehouses, Cortex
Search create/alter/query/scoring documentation; current Spark cluster,
scheduling, AQE, shuffle and decommissioning documentation; and current Apache
Gluten architecture, configuration, Velox, spill and stage-resource documents.
Follow-up resolved the consequential questions about who owns data processing,
how concurrency differs from single-query scale, how Native offload retains JVM
control flow, and why shuffle durability is required for elastic scale-in.

Research stops here because every architecture decision needed for the current
implementation has direct primary evidence and another broad search is unlikely
to change it. Undocumented vendor internals, vendor API imitation and performance
equivalence claims remain intentionally excluded.

## Source ledger

All sources were accessed on 2026-09-04. Publishers are Snowflake Inc., Apache
Software Foundation (Spark and Gluten), or their official project repositories.
The descriptive links in the evidence table are the canonical URLs and record
the title, publisher context and access path needed to reproduce this review.
