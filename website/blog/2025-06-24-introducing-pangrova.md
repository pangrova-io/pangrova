---
title: Introducing Pangrova — One SQL Engine for Lakehouse Analytics, Retrieval, and AI
authors: [pangrova-team]
tags: [announcement, architecture]
description: Pangrova is an open-source distributed SQL engine for open lakehouses. It unifies relational analytics, retrieval, and AI SQL over Iceberg and Paimon with Calcite, Arrow, and Velox—and binds governance and data versions into the execution contract.
---

# Introducing Pangrova

The lakehouse has largely won the storage argument. Apache Iceberg and Apache
Paimon put table data on open object storage, with open metadata and an ecosystem
of engines. But the **execution layer above that storage is still fragmented**:
one system for relational SQL, another for vector search, another for full-text
search, a pipeline for embeddings, and yet another layer for permissions, masking,
and audit. Every boundary becomes a copy, a synchronization job, a version skew,
or a governance gap.

Pangrova is an **open-source distributed SQL query engine for open lakehouses**
built around a different thesis: relational analytics, retrieval, AI model
invocation, and governance should run through **one execution framework over the
data that already lives in the lakehouse**. It combines **Apache Calcite** for
planning, **C++20 / Velox** for vectorized execution, and **Apache Arrow /
Arrow Flight** for columnar interchange and result transport.

This post is a deep introduction: what Pangrova is, how a query actually moves
through it, the problems it is designed to solve, the advantages that follow from
its architecture, and where the project is going. It also states the evidence
boundaries explicitly—an implemented path is not a production qualification.

<!-- truncate -->

## 1. What Pangrova Is

Pangrova is a distributed SQL engine with a strict separation between **control
plane** and **data plane**:

```text
SQL / JDBC / REST / CLI
          |
          v
  Coordinator JVM
  parse -> bind -> authorize -> plan -> admit -> schedule -> fence -> commit
          |
          v
  canonical v2 TaskRequest + Substrait plan + execution bindings
          |
          v
  Worker JVM
  lifecycle / admission / JNI / secrets / governed provider callbacks
          |
          v
  C++ Native (Velox)
  scan -> filter/project -> join/aggregate -> retrieve -> shuffle -> fill -> output
          |
          v
  Arrow Flight exchange and result stream
```

The Coordinator parses and plans SQL, resolves catalogs and snapshots, authorizes
access, admits resources, builds the stage DAG, tracks attempts, validates terminal
reports, and owns the final fenced catalog commit for writes. It **never scans
table data, materializes result rows, merges retrieval candidates, or scores
them**. Those operations belong to the Native data plane.

A Worker JVM is a node runtime, not a second query engine. It owns process
lifecycle, task admission, JNI, credential resolution, policy enforcement, and
governed provider networking. Relational computation runs in C++/Velox. This is
what makes the architecture more than "Java plans, C++ scans": the same boundary
governs joins, aggregations, shuffle, retrieval, reranking, and write staging.

### 1.1 One execution path, including writes

Reads and writes share the same pipeline. Relational DML—including
`INSERT VALUES`—follows planner → Worker → Native. Workers produce commit
fragments and invoke Worker-side connector SDK page sinks through JNI where the
connector requires them; the Coordinator owns the final fenced connector catalog
commit. DDL remains a Java metadata operation.

This is **coordination/execution separation**, not "C++ reads and Java writes every
row." It preserves the table format's transactional semantics while keeping
relational computation in the vectorized engine.

### 1.2 One result path

The Coordinator does not proxy result rows through its heap. All output stages
(Gather, Merge, final aggregation, final Top-K) run on an **Output Worker**—an
ordinary Worker with an output plan. The Coordinator redirects the client to that
Worker's Arrow Flight endpoint. Data flows Compute Worker → Output Worker → Client,
with end-to-end backpressure: a slow client pauses the Output Worker, which pauses
the Compute Workers.

### 1.3 One protocol

Execution is described by a canonical v2 `TaskRequest` carrying the Substrait
plan, splits, capability bindings, snapshot/index/model identities, and resource
grants. Task control uses HTTP; data exchange and results use Arrow Flight. The
Java/Native boundary uses canonical Protobuf plus Arrow interoperability where
applicable. Generated protocol sources are the single source of truth; the build
validates descriptor identity rather than allowing mirrored schemas.

### 1.4 AI-native is an execution capability, not a sidecar

Pangrova extends the same execution model to retrieval and model invocation:

- `VECTOR` and `FILE` are first-class typed values.
- Retrieval is expressed as typed **source, merge, rerank, and fill** operators
  scheduled by the control plane and executed by Native Workers.
- Index generations publish as **immutable artifacts** with capability and format
  identities; missing, stale, or mismatched artifacts fail closed instead of
  silently returning an empty Top-K.
- Coverage is explicit: `COMPLETE`, policy-permitted `PARTIAL`, or
  `FALLBACK_EXACT`—not a single optimistic flag.
- `AI_EMBED`, `AI_COMPLETE`, and related functions invoke a **governed Worker-local
  provider callback** with exact model version, capability signature, credential
  reference, deadline, and resource budget. Pangrova does not bundle a model
  service, and it does not pretend an external endpoint is in-database compute.

### 1.5 What Pangrova is not

Pangrova is not a managed service, not an OLTP database, and not a claim that every
SQL combination is production-ready. It is an engine and a set of explicit
contracts. The [capability status](/docs/getting-started/capability-status) is the
authoritative map of what is wired, what is partial, and what is still target
design.

## 2. Suitable Scenarios

Pangrova is designed for teams that already operate an open lakehouse and need
more than isolated SQL or isolated vector search. The strongest starting point is
a **real workload with business filters, permissions, and a baseline from the
existing system**.

### 2.1 Business-constrained retrieval over lakehouse data

A product catalog, document corpus, or knowledge base already lives in Iceberg or
Paimon. A query must filter by tenant, region, permission, price, freshness, or
table relationships **before** semantic retrieval, then join retrieved rows back
to business data and rank them. Pangrova keeps filters, retrieval, joins, and
fusion in one execution framework instead of coordinating a vector database and an
application-side merge.

### 2.2 Enterprise knowledge search with governance

Departmental permissions, row filters, column masks, document versions, and index
generations must apply to retrieval and to the joins that follow. Pangrova binds
tenant, authorization, snapshot, model, and index-generation identities into task
admission and execution, so derived results remain traceable to the data that
produced them.

### 2.3 Analytics and AI processing on the same data

Classification, summarization, extraction, and embedding generation often run as
external scripts before results are written back. Pangrova exposes model invocation
through SQL, with bounded provider callbacks and explicit resource accounting, so
data engineers can use familiar SQL workflows without giving up governance or
version binding.

### 2.4 Paimon operational analytics

Teams using Paimon primary-key merge semantics and changing data can validate read
correctness, freshness, and refresh latency within supported paths.

### 2.5 Platform integrators

Teams building a deployment they operate can use the modular API/SPI surface,
connector plugin isolation, and Apache 2.0 license as a foundation for a customized
engine. Extension boundaries are explicit; capability validation is part of the
contract.

### 2.6 Weaker fits today

Pangrova is not currently the best fit for small reporting workloads, online
transaction processing, isolated vector search with no relational or governance
requirements, or teams that require an immediately available fully managed
service. Overall production qualification is still open, and complete Feature
Store, training-dataset, and end-to-end materialized-view lifecycles remain work
in progress.

## 3. Problems Solved and the Advantages They Create

The advantages below are **architectural advantages**—properties of the design
that change what teams can build and operate. They are not performance claims.
Where an advantage depends on qualification or deployment, the boundary is stated.

### 3.1 The problem: open storage, fragmented execution

A typical lakehouse stack today looks like this:

| Layer | Typical system | Consequence |
|---|---|---|
| Relational analytics | A SQL engine | Copies or federates data; separate governance |
| Vector search | A vector database | Another copy, another index lifecycle, another permission model |
| Full-text / hybrid | A search engine | Another copy and another ranking pipeline |
| AI processing | Scripts and pipelines | Outputs written back out of band; version drift |
| Governance | Application code plus each system | Duplicated RBAC, masking, quotas, and audit |

Pangrova attacks the fragmentation directly.

### 3.2 Advantage 1 — One engine and one governance model for analytics, retrieval, and AI

**Mechanism.** Relational SQL, vector distance, ANN retrieval, full-text and hybrid
retrieval, fusion, reranking, and model invocation are all expressed in the same
planned execution graph. Retrieval is a set of typed relations and operators, not
an RPC interceptor. The Coordinator schedules stages; Native Workers execute them;
the Output Worker streams the final result.

**Why it matters.** A business-filtered search no longer requires exporting
candidates to an application, joining them in custom code, and re-implementing
permissions around the merge. The filter, retrieval, join, and ranking share one
plan, one resource budget, one cancellation path, and one audit trail.

**Boundary.** Filtered ANN, hybrid retrieval, and reranking have qualification
limits per connector and query shape. The engine does not claim every combination
is equally supported today.

### 3.3 Advantage 2 — Open lakehouse reuse without giving up native execution

**Mechanism.** Iceberg and Paimon connectors provide metadata, scan planning, and
catalog publication. A plugin Connector SPI with classloader isolation separates
connector code from the engine. Java owns metadata and final catalog commit; C++
owns vectorized relational execution.

**Why it matters.** Teams keep their open table formats, catalogs, and object
storage while gaining a native execution path and a single SQL surface for
analytics and retrieval. They are not forced to migrate data into a proprietary
warehouse to get vectorized execution or AI primitives.

**Boundary.** Connector support is not uniform across table features, file formats,
merge policies, and write modes. Validate the exact connector and workload.

### 3.4 Advantage 3 — Correctness-first execution contracts

**Mechanism.** Execution bindings carry table snapshots, index generations, model
versions, artifact identities, tenant identity, authorization context, and
resource grants into task admission and execution. Index builds are durable jobs:
source snapshots are pinned, work units and fences advance across retries, and a
generation is published atomically only after every report and checksum validates.
Failed or superseded attempts cannot publish.

**Why it matters.** The hardest bugs in a data+AI platform are not "the query was
slow." They are "the query used the wrong snapshot," "the index was from a
different model," or "a retry overwrote a newer generation." Pangrova makes those
conditions explicit failures instead of plausible-looking results.

**Boundary.** Publication, rebuild, cancellation, and recovery paths require
validation per feature. The contract is the design advantage; the qualification is
still in progress.

### 3.5 Advantage 4 — Native execution with end-to-end resource discipline

**Mechanism.** Velox executes relational operators in C++. Workers exchange data
over Arrow Flight. The plan uses separate compute and output stages, and
backpressure propagates from the client through the Output Worker to compute
stages. A Java-side memory arbitrator and Native Velox memory pools govern
allocation; spill and resource queues provide bounded behavior under pressure.
Shuffle and result paths are accounted separately—there is no blanket zero-copy
claim.

**Why it matters.** Native vectorized execution is not just a benchmark story. It
changes the operational shape: row materialization stays out of the Coordinator,
the result path has a dedicated owner, and memory pressure has a place to spill
instead of becoming an OOM. The design is built for bounded, cancellable,
distributed work.

**Boundary.** Speedup multiples, PB-scale capacity, and production stability
require reproducible evidence on the deployed version. Arrow buffer sharing,
IPC, format conversion, compression, and network transport have real costs and are
measured separately.

### 3.6 Advantage 5 — Governance is in the execution path

**Mechanism.** Pangrova has authentication, RBAC plus attribute-based policy,
row-level security filter injection, column masking, tenant resource pools,
quota admission, and audit components. Metadata resolution and authorization are
part of planning; tenant and resource identity are carried into scheduling.

**Why it matters.** When analytics and retrieval live in separate systems,
governance is duplicated and drifts. In Pangrova, the same policy framework applies
to the relational query and to the retrieval path, and the execution report carries
the identity needed for audit.

**Boundary.** Deployments must verify the wired authentication, policy, and
configuration surface. Component presence is not production qualification.

### 3.7 Advantage 6 — Operational depth: multi-active coordination, durable state, fenced recovery

**Mechanism.** Coordinators are multi-active for query reception; DDL is routed to a
leader. Lease and coordination state are backed by the metadata database (with an
optional ZooKeeper backend). Workers register, heartbeat, and are fenced by
incarnation. Query state, journals, and commit receipts are durable. Shuffle
contracts and a deployable remote shuffle service support attempt-scoped recovery;
terminal reports are validated against the dispatched request.

**Why it matters.** Distributed SQL fails in the seams: a dead Worker, an ambiguous
commit, a stale attempt, a lost shuffle partition. Pangrova treats those as
first-class states—fenced attempts, explicit unknown outcomes, durable receipts—
rather than assuming a retry is always safe.

**Boundary.** Availability still depends on the database, catalog, object store,
network, and deployment. Multi-active Coordinators do not remove every single
point of failure, and recovery behavior must be tested for the deployed topology.

### 3.8 Advantage 7 — Modular architecture that is meant to be extended

**Mechanism.** Each domain has an API/SPI contract artifact, a Core implementation,
and replaceable providers. Persistence is split by domain port and PostgreSQL or
Memory backend. A module manifest and dependency-policy tests enforce boundaries;
the central SPI and generic adapter modules are retired. Connector plugins are
isolated by classloader policy.

**Why it matters.** Teams can add a connector, a provider, or a storage backend
without forking the control plane or weakening transaction ownership. The
architecture is designed to be assembled and replaced at explicit seams.

**Boundary.** Some provider and plugin-runtime work remains in progress; the module
boundaries are enforced, but not every extension point has a production provider.

### 3.9 Advantage 8 — Fail-closed behavior and an honest capability model

**Mechanism.** Missing snapshots, unavailable index generations, unknown provider
outcomes, unsupported native encodings, and incomplete retrieval coverage fail
explicitly. There is no silent fallback to a different revision, no empty
candidate list on a missing shard, and no "static default means supported" claim.
The documented source-of-truth order is code and tests before configuration,
rules, and architecture prose.

**Why it matters.** In a data platform, a silent fallback is often worse than an
error. A wrong result that looks successful poisons downstream decisions and is
much harder to detect than a failed query. The same discipline applies to the
project's documentation and roadmap: known gaps are stated, not hidden.

**Boundary.** Fail-closed behavior is a design rule; each path still needs a real
failure test. The capability status page records the current evidence.

### 3.10 Where the advantage must be proven

Architecture creates potential, not a completed product. The project does not claim
a qualified speedup multiple, cost reduction, PB-scale capacity, or cache hit-rate
gain. A serious evaluation should fix hardware, data snapshots, SQL, cache state,
resource budgets, and result-consumption scope, then measure correctness,
permissions, retrieval quality, latency, resource cost, and recovery on one real
workload.

## 4. Current Maturity and Future Direction

### 4.1 Where the project is today

Pangrova has working foundations across connectors, SQL clients, distributed
execution, indexing, security, observability, streaming, and AI SQL components,
backed by focused unit, integration, and E2E tests. Overall production qualification
remains open: component tests do not establish a complete release gate, independent
multi-Worker recovery, PB-scale capacity, or a qualified performance baseline.

Use the [current capability status](/docs/getting-started/capability-status) for
implemented paths and known gaps, and the
[production checklist](/docs/deployment/production-checklist) before a deployment
decision.

### 4.2 Roadmap

The roadmap is a set of development goals, not release commitments:

| Direction | Scope |
|---|---|
| Materialized views and dynamic tables | Qualify refresh, publication, recovery, and query rewriting across the shared lifecycle |
| Cross-catalog execution | Broaden validation of multi-catalog queries and connector pushdown |
| Retrieval quality | Measure recall and precision for supported search paths |
| RAG workflows | Explore integrated retrieve, augment, and generate execution |
| Python and training-data access | Develop Arrow-based client and dataset delivery integrations |
| GPU acceleration | Evaluate acceleration of suitable inference and compute operators |
| Serverless deployment | Explore suspension, resumption, and deployment lifecycle support |

Known open items include:

- **AI-native execution:** public-entry AI SQL and retrieval with real providers,
  the complete Native build, sanitizer coverage, and release gates. `DROP EMBEDDING
  MODEL` remains fail-closed until durable dependency reservations and a resumable
  CASCADE job exist. Training Dataset has catalog metadata but no SQL entry, durable
  multi-source export, Native plan wiring, or streaming reader. Evaluation currently
  has only control-plane metadata.
- **System tables:** full Iceberg `$files`/`$entries`, the Paimon `$files`
  type/filesystem matrix, and non-local `$branches`. Quoted table names containing
  a literal `$` still need a typed relation identity through parser, Calcite, and
  connector boundaries.
- **Table and file plane:** Native DIRECTORY execution, billion-entry directory
  publication, 100 GiB transfer, PB-scale data, and Linux production qualification.
- **Modularization:** P0–P12 boundaries are enforced by the module manifest and the
  domain-modularization ADR; remaining work is tracked as issues.

### 4.3 The direction in one sentence

Pangrova is working toward a lakehouse where the **shared data**—not a copied
analytical silo—is the place where relational analytics, retrieval, model
invocation, and governance meet; where every derived artifact is bound to the data
and model version that produced it; and where the contracts stay explicit enough
that teams can extend and operate the engine themselves.

## Getting Involved

- Start with the [quickstart](/docs/getting-started/quickstart).
- Read the [architecture overview](/docs/architecture/overview) for the execution
  model and module boundaries.
- Check [current capability status](/docs/getting-started/capability-status) before
  relying on a feature in production.
- Review the [roadmap](/docs/getting-started/roadmap) and propose concrete work
  through [GitHub issues](https://github.com/pangrova-io/pangrova/issues).

Pangrova is developed in the open. If your team has a real lakehouse workload that
combines relational analytics, retrieval, and governance, we would like to validate
it with you—starting with correctness, permissions, retrieval quality, latency,
resource cost, and recovery on one concrete scenario.
