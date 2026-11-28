---
title: "Roadmap"
description: "Priority roadmap for Python, training datasets, and the multimodal data lifecycle"
---

# Roadmap

This roadmap describes planned work. The [SQL reference](../sql-reference/index.md)
and [connector guides](connectors/index.md) describe current behavior.

## Highest priorities

**Python and Training Dataset are the first delivery.** Start both while the
Lance SDK and media-ingestion work proceeds. Establish the first publish and read
path on an existing snapshot-capable table, then run that same path on the
managed multimodal pilot before closing the first milestone.

1. **Installable Python SDK.** Deliver authenticated, parameterized SQL, Arrow
   record-batch streaming, catalog discovery, dataset and job inspection, and
   cancellation. Add a PyTorch iterable dataset with projection, bounded
   prefetch, deterministic sharding, and checkpoint/resume against a published
   dataset version. Publish an installation guide, runnable notebook, and
   client/server compatibility matrix.
2. **Training Dataset lifecycle.** Expose SQL to define, publish, inspect,
   and retire a dataset. Reuse durable Native export and generation
   publication. Start with a single retained source snapshot, versioned schema,
   lineage, checksummed manifest, and authorized Flight stream. The next
   milestone adds multiple source snapshots, group-aware splits, global
   shuffle, elastic resume, and training caches.
3. **Managed multimodal pilot.** Connect discovery, ingestion, provenance,
   media previews, and filtered hybrid retrieval to that same Python and
   training workflow. Use one corpus with text, images, documents, and labels.

## Multimodal alignment

The reference scope is the open Lance format and the public multimodal
workflows of LanceDB Embedded 0.41.0 and Enterprise 1.14.0. Record each
reference behavior, target Pangrova entry point, milestone, and acceptance
evidence. Use the following workstreams to organize that inventory.
[LanceDB Multimodal Lakehouse](https://www.lancedb.com/blog/multimodal-lakehouse) ·
[LanceDB versioned documentation](https://docs.lancedb.com/llms.txt)

| Workstream | Pangrova starting point | Work to close |
|---|---|---|
| File and media lifecycle | Stage and Volume file access and governed `FILE` references | SQL directory projection and ingestion, managed copy, Blob v2, authorized previews and ranges, media retention, and reclamation. [File storage](../architecture/file-storage.md) |
| Tables and interoperability | Managed Lance append/overwrite and snapshot reads | External registration and import, stable row IDs, row mutations, schema evolution, version administration, and maintenance. [Lance connector](connectors/lance.md) |
| Exploration and curation | Distributed SQL and an existing governance foundation | Per-modality inspection, profiling, random and selected-row sampling, chunking, near-duplicate selection, and reproducible slices. [Data governance](../architecture/data-governance.md) |
| Search and serving | Native vector, full-text, and hybrid execution with documented query-shape limits | Lance index binding, ANN over model-produced query vectors, multivectors, filtering and ranking, coverage and freshness, and real-time visibility. [Vector search](../sql-reference/functions/vector-search.md) |
| Feature engineering and evaluation | Async embeddings, single-source Feature View export, and evaluation metadata | Versioned transforms, incremental and scheduled computation, multi-source materialization, and Native evaluation jobs. [Feature Views](../sql-reference/feature-views.md) · [AI architecture](../architecture/ai-native.md) |
| Training and clients | Training Dataset metadata and an existing Arrow Flight data plane | Installable Python SDK, publish/read lifecycle, PyTorch delivery, reproducible splits, elastic loading, and broader client and Embedded workflows. [AI architecture](../architecture/ai-native.md) |
| Operations | Durable job, governance, and deployment infrastructure | Maintenance automation, cache tiers and prewarming, workload isolation, backup and restore, distributed serving, and deployment qualification. [Production checklist](../deployment/production-checklist.md) |

### Data storage and publication order

Arrows indicate persistence dependencies. Independent derivatives run in
parallel from the same committed input. For example, full-text indexing can
start once the text is published, and video transcripts and frame embeddings
can use the same committed video independently. Index publication validates
the source version and the coverage of its own inputs.

| Data | Save and publish in this order | Storage and lookup |
|---|---|---|
| Structured records and metadata | Source record and business key → type, quality, and duplicate validation → versioned row → statistics and scalar indexes | Typed columns preserve values by snapshot. Business keys drive updates and stable row IDs identify rows. |
| Text and conversations | Original text or messages → normalized text → passages or messages with source ranges → publish passages for full-text indexing and generate embeddings for vector indexing | Text columns hold the source. Child rows record the parent and character range or message ordinal. |
| Images and scans | Original bytes → dimensions, format, and orientation → thumbnails and regions → OCR, captions, and image or region embeddings → indexes | Blobs hold originals and reusable previews. Typed columns hold text, labels, and coordinates. |
| PDF and other documents | Original file → document and page metadata → page text, tables, figures, and layout → publish chunks for full-text indexing and generate embeddings for vector indexing | Blobs hold originals. Child rows reference pages and regions so a match can locate the source. |
| Audio | Original bytes → codec, sample rate, channels, and duration → time ranges → transcripts, speaker labels where available, and embeddings → indexes | Blobs hold source audio. Temporal child rows identify the channel and interval. Workers read a byte range when the container supports it and otherwise decode within resource limits. |
| Video | Original bytes → container, tracks, and time base → addressable clips, keyframes, and audio references → transcripts, labels, and features → segment indexes | Blobs hold original video. Timeline metadata locates tracks, frames, and clips. Persist reusable previews, proxies, or training clips as derived data. |
| Time series, sensor logs, and robotics data | Original log → episode and clock reference → sensor streams, frames, or windows → aligned samples and labels → features, search, or training outputs | An episode relates typed time series to media and point clouds. Retain clock alignment and missing-data state. Keep each episode within one split. Use a larger group when examples share a source or must stay together. |
| Tensors and point clouds | Source object or array → dtype, shape, coordinates, and checksum → normalized arrays, blocks, or tiles → features and spatial or vector indexes | Use Arrow-compatible typed arrays for regular values and Blobs with typed descriptors for large or variable-length objects. |
| Labels and annotations | Pinned target version and range → annotation, author, and taxonomy version → review or correction → published annotation version → affected feature and index refresh | Versioned annotation tables can reference an entire row, page, region, or time range. Training manifests pin the annotation version. |
| Imported and generated vectors | Imported vectors → validate origin, model, dimension, and metric → commit with source rows. Generated vectors → pin source and model → compute and validate → publish generation → index | Use typed vectors or multivectors. Bind each index to its model and vector space. Cross-modal queries use an explicitly shared space. |
| Derived and curated datasets | Freeze source snapshots → persist normalized selection and transform versions → write row membership or materialized output → verify schema, lineage, and coverage → publish | Reuse retained inputs for reference-based collections. Persist outputs for transformed bytes or independent delivery. |
| Training and evaluation datasets | Freeze source snapshots, labels, and features → persist sample and group identities, splits, and seed → write immutable row lists or materialized shards → verify counts, checksums, and coverage → publish → attach checkpoints or metrics | Use a versioned manifest. Materialize policy-approved projections when needed. Retain manifests and inputs together for the promised replay window. |

Derived rows carry the parent table incarnation, stable row ID, input snapshot,
and transform or model version. Page numbers, coordinates, time intervals, or
text ranges locate the original content. Lance Blob v2 uses its format-defined
placement for small and large payloads. `FILE` continues to represent governed
file references. [Lance Blob v2 specification](https://lance.org/format/table/blob/) ·
[LanceDB curation and training workflows](https://docs.lancedb.com/_llms/embedded/0-41-0/documentation.txt)

### Shared lifecycle

1. **Discover and freeze inputs.** Reuse Stage, Volume, and storage integrations.
   Connect `DIRECTORY`, `read_files`, `COPY INTO`, external tables, and Arrow
   ingestion to immutable source manifests. Bind the source object version and
   checksum, principal, target table incarnation, and import identity. Persist
   per-object results so retries can resume and rejected objects can be reviewed
   or reprocessed.
2. **Verify bytes before publishing references.** Native Workers validate and
   stage payloads, including the managed copy for `FILE MANAGED`, or verify the
   selected external reference. Build fragments and their manifest before
   publishing the base snapshot through the existing commit fence. A commit
   with an unknown outcome retains staged objects for reconciliation. A fixed
   version can identify external bytes while those bytes remain available.
   Reproducible publication requires `RETAINED` assurance for the promised
   replay window. Copy into managed storage when the source cannot provide it.
   [File storage and retention](../architecture/file-storage.md#current-status-and-gaps)
3. **Publish derivatives from fixed inputs.** Bind each transform, feature,
   index, export, and evaluation job to immutable inputs. Validate schema,
   row identity, lineage, and coverage before publishing. Changes produce a
   new generation. Carry forward existing output only when the source
   identity and transform inputs are proven unchanged. Expose snapshot
   coverage, freshness, and ANN recall as separate observations. A query can
   request an exact scan of uncovered rows at the same snapshot where supported.
4. **Authorize and filter before reading media.** The Coordinator binds
   policies and versions, schedules jobs, and publishes metadata. Native
   Workers ingest, scan, retrieve, process media, and stream Arrow Flight
   batches using the existing Substrait and protocol v2 execution path. Apply
   row filters and column masks to previews, search, provider egress, and
   training reads. Renew authorization against the same immutable data
   version. Require object-wide authorization for direct reads and bound
   delegated access by its expiry.
5. **Version, retain, and reclaim.** Updates, replacements, deletes, and restores
   publish new snapshots. Keep stable row identity through compaction and
   resolve its physical location at the pinned snapshot. Protect references
   from tags, branches, clones, indexes, datasets, in-flight jobs, holds, and
   backups. Generate a reclaim preview and stop reclamation if reachability
   cannot be verified. Each table has one owner for commits and cleanup.
   Register external Lance snapshots for read-only access, copy by default
   during import, and require explicit ownership transfer for in-place adoption.
   Verify that external SDKs can read compatible Pangrova publications.
   [Lance versioning](https://lance.org/format/table/versioning/) ·
   [Lance branches and shallow clones](https://www.lancedb.com/blog/branching-and-shallow-clone)

### Python and training contracts

- **Definition and version.** The catalog definition stores selection and
  publishing policy. Each immutable published version identifies the exact
  authorized source snapshots, schema and projections, labels and features,
  split rules, sample identities, manifest checksums, and retention.
  Expose inspectable job state for pending, failed, and published generations.
- **Selection and order.** Phase one covers a single pinned source and a
  deterministic order and assignment across fixed ranks. Phase two freezes
  an explicit per-source snapshot map and assigns groups before computing
  each epoch's global permutation. The order is independent of worker and
  rank topology. Publish the split and ordering algorithm versions so a
  later client can reproduce the result.
- **Resume and payload access.** Checkpoint the manifest identity, split,
  epoch, and acknowledged position in the deterministic order. At an elastic
  checkpoint, commit the contiguous prefix consumed by all ranks together
  with the matching model checkpoint. On resume, repartition the remaining
  positions, including a partial final batch.
  Validate authorization on reopen. Project and prefetch only the authorized
  columns and media selected by the reader. Key cached data by immutable
  identity and enforce authorization at delivery.
  [LanceDB distributed training](https://docs.lancedb.com/oss/v0-41-0/training/distributed-training)

### Delivery phases

#### 0. Contracts and compatibility

- Start Python client and training manifest work in parallel with format
  compatibility. Validate authenticated Arrow streaming and the existing
  Native export path on a retained snapshot-capable table.
- Confirm the open-format, Embedded, and Enterprise capability inventory.
  Re-run representative file access, managed Lance, retrieval, and Feature
  View cases. Use independently generated fixtures for legacy and Blob v2
  files, nested types, stable row IDs, and feature flags. Obtain required
  approval for the core Lance SDK upgrade and validate Rust, C++, JNI, and
  packaging.
- **Gate.** Publish the compatibility matrix, representative datasets, and
  benchmark environment. Verify old-table reads and rejection of unsupported
  feature flags before enabling new writes.

#### 1. Python, Training Dataset, and the managed pilot

- Ship the Python SDK, SQL Training Dataset lifecycle, protected Native
  publication, and PyTorch delivery on a single retained snapshot. Validate
  this slice before it depends on managed Blob ingestion.
- Connect governed Stage discovery and import to a Lance table with text,
  images, documents, and annotations. Deliver provenance, previews, indexed
  text and shared-space image/text embeddings, filtered hybrid search, and
  late media fetch. Publish a training dataset selected at that same snapshot.
- **Gate.** Run Stage → published snapshot → hybrid search → published training
  manifest → Python/PyTorch batches on an independent Coordinator and at least
  two Workers. Replay an import, resume a partially consumed dataset, and
  repeat old-version reads after append and permission changes. Verify zero
  payload bytes read for metadata-only queries.

#### 2. Distributed training and complete media management

- Deliver multi-source publication, deterministic group-aware splitting and
  global shuffle, elastic resume, column projection, prefetch, and caching.
  Qualify Iceberg, Paimon, and Lance source combinations individually against
  snapshot identity and retention.
- Expand to audio, video, tensors, point clouds, time series, and episodes.
  Add row-level `UPDATE`, `DELETE`, and `MERGE`, column evolution, system tables,
  views, change queries, tags, branches, shallow and deep clones, restore,
  compaction, and cleanup. Add Stage SQL upload, download, and scoped URLs, plus profiling,
  sampling, deduplication, slicing, and joins.
- **Gate.** Resume after a topology change with no omissions or duplicates
  relative to the recorded checkpoint. Exercise each modality's import,
  source lookup, version replay, and retirement. Verify live and historical
  readers through concurrent mutation, compaction, and cleanup.

#### 3. Retrieval, feature engineering, and evaluation

- Bind ANN to model-produced query vectors. Deliver multivectors, analyzers
  and phrase search, scalar indexes, prefilters and postfilters, pagination,
  fusion, and model reranking. Qualify external native indexes for read-only
  use and add snapshot-bound incremental index updates for managed tables.
- Reuse durable jobs for computed columns, keyed backfill, multi-source
  materialized views, change-based and scheduled refresh, and checkpointed
  Python UDFs and table functions on CPU, GPU, or remote compute. Add
  protected model dependency tracking and Native evaluation with fenced metric
  publication. Publish usage, lineage, and failure state for these jobs.
- **Gate.** Compare exact results to an oracle, measure ANN quality separately
  from coverage, and compare incremental output to full recomputation at the
  same versions. Reproduce evaluation inputs and link metrics back to source,
  label, feature, and model versions.

#### 4. Real-time serving, clients, and production operations

- Add recoverable low-latency writes and incremental indexing. Query, search,
  and base-table merge share the same committed visibility contract.
- Deliver tiered caches and prewarming, automated maintenance, isolated
  resource pools, service replicas, autoscaling, and diagnostics. Broaden
  Python and TypeScript coverage, qualify SQL/JDBC and Flight SQL, and
  deliver a local Embedded workflow. Qualify Kubernetes, workload identity,
  storage integration, audit, backup and restore, and cross-region or cloud
  deployments.
- **Gate.** Verify committed visibility under load and failure. Complete the
  capability inventory, the full lifecycle exercise, and same-revision Linux
  production and full-regression checks.

### End-to-end acceptance

- Run a mixed-modality workload through discovery, import, review, editing,
  branching, features, search, training, evaluation, restore, and retirement
  via public entry points. Trace any sample to the exact retained source and
  reproduce it from its manifest.
- Inject repeated requests, object replacement, revoked access, corrupt
  manifests, stale attempts, Coordinator or Worker loss, and partial index
  coverage. Run historical reads, compaction, and cleanup together. Verify
  each retained reference and the reclaim decision for expired managed bytes.
- Establish benchmarks in phases 0 and 1 and lock release targets before
  qualifying at scale. Measure p95/p99 latency, Recall@K, NDCG@K, ingest-to-search
  freshness, bytes read, backfill and training throughput, recovery, and
  storage cost. Test reader-first rollouts, per-table activation, and rollback
  to verified snapshots.
- Run affected module, protocol, Native, JNI, sanitizer, and independent
  process checks as applicable. Update SQL, connector, SDK, and operations
  documentation alongside each public workflow.

## Other open work

| Direction | Remaining scope |
|---|---|
| Continuous queries | Extend CDC and affected-key refresh with incremental execution and watermark-gated scheduling. |
| Embedding model deletion | Enable `DROP EMBEDDING MODEL` after durable dependency reservations and resumable CASCADE reconciliation are in place. |
| Paimon system tables | Extend `$files` type and filesystem coverage and support non-local `$branches`. Carry table names containing a quoted literal `$` through parser, planner, and connector identities. |
| Session assignment | Define server-side `SET` lifetime and supported properties across clients. |
| Startup catalog registration | Load catalog templates through the durable authority with explicit error and replay semantics. |
| Partition-scoped reclustering | Bind `RECLUSTER WHERE` to eligible partitions and verify that unaffected rows are preserved. |
| Iceberg CDC mutation coverage | Extend writes to partitioned tables and multiple Kafka partitions with explicit conflict ordering. |
| Advanced SQL retrieval | Expand predicate and aggregation shapes beyond the initial search workflows. |
| Feature serving and embodied workflows | Build online feature serving and domain workflows on published features and episode datasets. |
| RAG workflows | Orchestrate retrieval, context assembly, and generation. |
| GPU engine acceleration | Qualify GPU-backed engine operators for suitable workloads. |
| Serverless deployment | Connect idle scaling to provisioning and demand-driven resumption. |

## Qualification goals

Billion-entry directory publication, 100 GiB transfers, PB-scale workloads, and
Linux production acceptance require workload-specific evidence. Comparative
performance, cost, scaling, and semantic-cache claims require measured baselines.
Measure JNI, conversion, IPC, compression, and network costs before making
zero-copy claims. Record measured scale and support limits in each release.
Follow the [production checklist](../deployment/production-checklist.md) for
release acceptance.

Propose priorities or concrete work through GitHub issues.
