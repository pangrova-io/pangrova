---
title: "Project Positioning, Value, and Target Customers"
description: "Pangrova's core problems, advantages, target customers, competitive positioning, and capability boundaries"
---

# Project Positioning, Value, and Target Customers

Pangrova is a distributed SQL query engine for open lakehouses. It brings business analytics,
data processing, and AI retrieval into a shared query and governance framework, helping
enterprises reuse their Apache Iceberg / Apache Paimon data assets.

The strongest use case to validate is retrieval over existing lakehouse data constrained by
business conditions and permissions, followed by relational analysis. The intended value is
less data movement, application-side result integration, and duplicated governance. Actual
benefits must be measured on customer workloads.

This guide distinguishes existing engineering foundations, product benefits that require
validation, and future directions. Target customers are suggested based on technical fit;
they are not deployed-customer claims or market conclusions validated through customer
interviews. Specific SQL support depends on the [SQL reference](../sql-reference/index.md),
connector constraints, and validation of the applicable execution path.

The customer and benefit analysis below describes product fit and hypotheses.

## Core Problems

### Long Paths from Lakehouse Data to Business Queries

Enterprises may already store data in an open lakehouse, yet interactive analytics can still
require copying data, maintaining synchronization jobs, or adding another analytical database.
Pangrova accesses existing lake tables through Iceberg / Paimon connectors and uses distributed
Native execution for query computation. The goal is to reduce complete business-dataset
migration and reuse existing storage investments.

Direct lake-table access may still require indexes, caches, embeddings, and other derived data.
Evaluate benefits through actual data-copy volumes, synchronization job counts, query latency,
and maintenance costs.

### Relational Analytics and AI Retrieval across Separate Systems

When business tables, full-text indexes, and vector databases are maintained separately,
applications must coordinate business filters, retrieval candidates, table joins, and final
ranking. Pangrova provides relational SQL, vector distances, and full-text and reranking
components to combine these operations within one query framework.

Filtered ANN, hybrid retrieval, and reranking have support and qualification boundaries.
Syntax examples do not establish support for arbitrary combinations. See the
[vector search reference](../sql-reference/functions/vector-search.md).

### Separate Scripts and Pipelines for AI Data Processing

Classification, summarization, extraction, and embedding generation often run in external
programs before their outputs are written back to the data platform. Pangrova exposes SQL
functions such as `AI_EMBED` and `AI_COMPLETE` so data engineers can invoke models through
familiar SQL workflows.

Model serving, credentials, network access, and runtime resources still require configuration.
Model calls run on the Worker side; the Coordinator handles planning and admission. See the
[AI function reference](../sql-reference/functions/ai-functions.md).

### Misaligned Data, Model, and Index Versions

After lake tables change, queries may use stale indexes or embeddings produced by different
models. Pangrova's execution contracts include table snapshots, model versions, and index
generation bindings to make reads and derived results traceable. Complete publication, rebuild,
cancellation, and recovery require validation of their respective paths. See the
[AI architecture](../architecture/ai-native.md).

### Duplicated Governance across Shared Data Platforms

Separate analytics and retrieval systems can duplicate authorization, masking, tenant limits,
and auditing. Pangrova has authorization, row-filtering, column-masking, tenant, and resource
admission components that provide a foundation for shared governance.

Deployments must verify the wired authentication and configuration surface, including the
identities and permissions used for foreground queries and background maintenance. See
[security configuration](../configuration/security.md) and
[multi-tenancy architecture](../architecture/multi-tenancy.md).

## Advantages and Evidence Boundaries

### Reuse of Open Lakehouse Assets

Iceberg / Paimon have connector implementations and registration wiring for existing catalogs
and lake tables. Customers can evaluate the engine incrementally within supported paths,
reducing the need to migrate to proprietary storage. Confirm support separately for table
formats, physical file formats, merge policies, and write modes. See the
[Iceberg](connectors/iceberg.md) and [Paimon](connectors/paimon.md) guides.

### Native Execution and Distributed Scaling Foundations

Java owns planning, authorization, admission, scheduling, and final catalog commits.
C++20 / Velox owns relational computation, while Workers exchange and output data through
Arrow Flight. This provides a foundation for execution efficiency and scaling, but the stack
alone does not establish speedup multiples.

Measure Arrow buffer sharing, format conversion, IPC, compression, and network transport
costs separately. No claim of physically zero-copy execution across the complete path is made.
See [Native execution](../architecture/native-execution.md).

### Integration of Analytics and AI Retrieval

The project includes relational query, indexing, model-invocation, and governance implementations.
Its strongest potential differentiator is how those capabilities work together over a customer's
open lakehouse. Demonstrate this through result correctness, retrieval quality, latency, and
reduced integration work. Complete hybrid retrieval, generated embeddings, and multimodal
workflows require qualification by feature and execution path.

### Versioning and Governance in Execution Contracts

Snapshot, model, index, tenant, and resource bindings enter task admission, making execution-time
validation and result traceability part of the architecture. This can benefit shared data
platforms. Complete maintenance authorization, recovery, and resource isolation at scale still
require measurements and validation.

### SQL Access and Customization

JDBC, REST, and CLI implementations allow teams to reuse familiar data-engineering workflows.
The modular SPI and Apache 2.0 license provide foundations for deployments customers operate
and customize. Validate compatibility with individual BI tools; operations still involve Java,
Native dependencies, catalog services, object storage, and model services. See
[JDBC](clients/jdbc.md) and [deployment planning](../deployment/planning.md).

## Target Customers

Priorities reflect existing data assets, problem fit, and the ability to validate a deployment.
An industry label alone does not establish customer fit.

| Priority | Customer Profile | Typical Need | Sponsor or Buyer | Suggested Engagement |
|---|---|---|---|---|
| First | Enterprise data platform teams with Iceberg / Paimon | Interactive lakehouse analytics, cross-table queries, analytics/retrieval integration | Data platform leads, architects, data engineering leads | A proof of concept with one real workload |
| First | Enterprise AI / knowledge platform teams with a lakehouse | Retrieval constrained by business conditions, departmental permissions, and data versions | AI platform leads, search leads, data architects | Joint validation of analytics and retrieval |
| Second | Operational analytics teams using Paimon | Queries over changing data, primary-key merge semantics, and freshness | Real-time warehouse leads, data infrastructure leads | Read-correctness and refresh-latency validation |
| Second | Industry software vendors and data platform integrators | An extensible SQL engine embedded in deployments they operate | CTOs, product architects, platform engineering leads | Custom integration and joint development |
| Medium term | Financial services, telecommunications, and large manufacturing enterprises | Deployment control, auditing, access governance, and operational stability | Data, infrastructure, and security teams | Expanded pilots after production qualification |
| Long term | Robotics and embodied AI data platforms | Multimodal selection, versioned datasets, and training delivery | Data platform and training infrastructure leads | Architecture collaboration; domain workflows remain to be built |

Initial partners should ideally have:

1. An existing open lakehouse, stable catalogs, and object storage.
2. A concrete query-performance or analytics/retrieval integration problem.
3. Sanitized data, real SQL, and measurements from their existing system.
4. A data infrastructure team willing to validate the deployment jointly.

Customers focused on small reporting workloads, simple online transactions, isolated vector
retrieval, or immediate access to a mature managed service are currently a weaker fit.

## Example Scenarios and Validation Priorities

These are candidate applications, not delivered customer cases. Support for every query
combination is not implied.

- **Product search and business analytics:** constrain products by region, price, inventory,
  and availability; perform semantic retrieval; join order or return data. Validate filtering,
  retrieval quality, join results, and data freshness.
- **Enterprise knowledge and business queries:** retrieve documents within departmental
  permissions and valid versions, then join tickets, customers, or projects. Validate
  permissions, version bindings, retrieval quality, and end-to-end latency.
- **Feedback and content processing:** classify, summarize, or extract information from
  reviews, tickets, or documents, then aggregate the results. Validate output quality,
  model-call failure semantics, resource use, and result publication.

## Competitive Positioning

Open lakehouse access, Native execution, and AI SQL are available in other projects.
Pangrova should establish advantages through integration benefits on specific workloads.
Technology choices alone do not establish unique capabilities or performance leadership.

| Comparison | Capabilities Confirmed by Official Sources | Difference Pangrova Must Validate |
|---|---|---|
| Trino | Iceberg queries and INSERT / UPDATE / DELETE / MERGE | Benefits for target queries, retrieval integration, and resource efficiency |
| StarRocks | Iceberg / Paimon external catalogs; vector indexing has separate deployment and maturity constraints | Queries, derived-index maintenance, and governance over open lake tables |
| Spark + Gluten / Presto C++ | Native execution using Velox | Interactive experience, deployment complexity, and workload cost |
| Snowflake Cortex Search | Vector and keyword hybrid retrieval for RAG and enterprise search | Deployment control, open lakehouse access, and customized governance |
| Milvus | Multi-vector hybrid retrieval and BM25-related capabilities | Reduced integration work from combining relational queries, joins, and retrieval |

Sources: [Trino Iceberg](https://trino.io/docs/current/connector/iceberg.html),
[StarRocks catalogs](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/),
[StarRocks vector indexes](https://docs.starrocks.io/docs/table_design/indexes/vector_index/),
[Velox ecosystem](https://velox-lib.io/),
[Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview),
and [Milvus hybrid retrieval](https://milvus.io/docs/multi-vector-search.md).
Recheck product versions and limitations during selection.

## Current Maturity and Future Directions

The project has connector, SQL client, distributed execution, and AI engineering foundations,
with focused tests. Overall production qualification remains open. Passing component tests
does not establish complete reactor, independent multi-Worker, recovery, or release-gate success.
Pilots should follow the [production checklist](../deployment/production-checklist.md) and
verify each required feature.

Sufficient reproducible performance evidence is still needed to support speedup multiples,
cost-reduction percentages, PB-scale capacity, or cache hit-rate gains. Comparisons should
fix hardware, data snapshots, SQL, cache state, resource budgets, and result-consumption scope.

Complete Feature Store and training-dataset services, full dynamic-table/materialized-view
refresh and publication lifecycles, and embodied AI ingestion, time alignment, episode semantics,
and training delivery remain implementation or qualification work.

Start by validating correctness, permissions, retrieval quality, latency, resource cost, and
recovery for one real customer scenario. Expand delivery scope based on the results. Customer
performance and reliability commitments should come from evidence for the deployed version.

## Project Descriptions and Discovery

Use a consistent introduction across the repository and website: position the engine for open
lakehouses, then explain the execution stack and intended uses. Describe Iceberg and Paimon
as the current built-in connectors, with the Connector SPI as the extension point for additional
table formats and data sources. The main positioning should accommodate that expansion.

### GitHub About Description

> Open-source distributed SQL query engine for open lakehouses, built with Calcite, Arrow, and Velox. Extensible connectors, native vectorized analytics, and SQL primitives for vector search and AI.

### Extended Introduction

Pangrova is an open-source distributed SQL query engine for open lakehouse
architectures. It uses Apache Calcite for SQL planning and optimization, C++20 /
Velox for vectorized execution, and Apache Arrow and Arrow Flight for columnar
interoperability and transport. Apache Iceberg and Apache Paimon connectors are
built in, and the pluggable Connector SPI provides the extension point for more
table formats and data sources. The project targets data platform teams that
already operate a lakehouse, as well as developers who want to combine business
data with AI retrieval. It provides SQL capabilities and engineering foundations
for relational analytics, data processing, vector search, full-text search, and
model invocation. The SQL reference and connector guides describe supported query
combinations and connector limits.

### Suggested Repository Topics

Core topics:

```text
sql query-engine distributed-sql lakehouse olap analytics
apache-iceberg apache-paimon apache-calcite apache-arrow velox
```

Additional topics for the documented retrieval components:

```text
vector-search full-text-search ai-sql
```

These are suggested repository settings; editing this document does not update GitHub About
or Topics. [GitHub topics](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/classifying-your-repository-with-topics)
help users find related repositories. The website uses descriptive page titles and summaries
so readers and search engines can identify its subject;
[Google may use page descriptions in search snippets](https://developers.google.com/search/docs/appearance/snippet).
These changes improve the clarity of discovery surfaces; traffic and ranking effects still
need to be measured after publication.
