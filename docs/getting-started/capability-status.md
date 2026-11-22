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
| `AI_COMPLETE` | Qualify the Linux production image | On macOS, `IndependentAiProcessesE2ETest` passed with real model calls across two Worker processes and a restart. All 13 `OllamaAiSqlE2ETest` cases passed with the production Flight idle budget. [AI functions](../sql-reference/functions/ai-functions.md) |

## Production evidence

The release gates include a same-revision Linux production image, full regression,
isolated Native owner suites, and deployment-specific acceptance. The [repository command guide](../contributing/repository-guide.md#command-entry-points)
and [production checklist](../deployment/production-checklist.md) list the validation paths.
