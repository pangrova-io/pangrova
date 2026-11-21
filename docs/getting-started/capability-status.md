---
title: "Open Capability Status"
description: "Implementation and release evidence still required for existing capabilities"
---

# Open Capability Status

This page tracks existing execution paths that still need implementation or
production evidence. The [SQL reference](../sql-reference/index.md) and
[connector guides](connectors/index.md) describe supported behavior. The
[Roadmap](roadmap.md) lists proposed capabilities and extensions.

## Implementation and acceptance

| Capability | Remaining work | Current verification |
|---|---|---|
| Clustering DDL and `RECLUSTER` | Dispatch rewrite work to Workers and qualify recovery after Worker loss. The current bounded rewrite runs in the Coordinator process | On macOS, `IndependentClusteringE2ETest` passed both Iceberg clustering modes and recovered the exact published snapshot after the Coordinator restarted with an unfinished journal entry. The PostgreSQL CAS test also fenced an unpublished attempt at its original metadata revision. [Maintenance SQL](../sql-reference/maintenance.md#clustering) |
| Worker CDC and streaming-only writes | Qualify the remaining Worker-loss and catalog-commit windows for Iceberg and Paimon | On macOS, the Iceberg CDC test verified a Worker UPDATE, a MAINTENANCE DELETE, and two restarts with injected failures at journal completion and source-offset acknowledgement. Both recovered the original publication, and unrelated rows were preserved. The Paimon process suite also covers staged Worker loss and a MAINTENANCE restart with a PENDING journal. [CDC architecture](../architecture/streaming.md) |
| Distributed relational writes | Qualify the remaining process-loss windows and ambiguous publication across supported table formats | Ten independent speculation cases passed. `IndependentRelationalWriteRecoveryE2ETest` also confirmed one Iceberg publication after the original Worker exited with its staging report still withheld. [Distributed writes](../architecture/distributed-writes.md) |
| `AI_COMPLETE` | Qualify the Linux production image | On macOS, `IndependentAiProcessesE2ETest` passed with real model calls across two Worker processes and a restart. All 13 `OllamaAiSqlE2ETest` cases passed with the production Flight idle budget. [AI functions](../sql-reference/functions/ai-functions.md) |

## Production evidence

The release gates include a same-revision Linux production image, full regression,
isolated Native owner suites, and deployment-specific acceptance. The [repository command guide](../contributing/repository-guide.md#command-entry-points)
and [production checklist](../deployment/production-checklist.md) list the validation paths.
