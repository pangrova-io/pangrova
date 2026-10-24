---
title: "Roadmap"
description: "Pangrova development priorities and proposed directions"
---

# Roadmap

These directions are development goals, not release commitments or claims of
end-to-end support. Check the [current capability status](capability-status.md)
for implemented paths and qualification limits.

| Direction | Scope |
|---|---|
| Materialized views and dynamic tables | Qualify refresh, publication, recovery and query rewriting across the shared lifecycle |
| Cross-catalog execution | Broaden validation of multi-catalog queries and connector pushdown |
| Retrieval quality | Measure recall and precision for supported search paths |
| RAG workflows | Explore integrated retrieve, augment and generate execution |
| Python and training-data access | Develop Arrow-based client and dataset delivery integrations |
| GPU acceleration | Evaluate acceleration of suitable inference and compute operators |
| Serverless deployment | Explore suspension, resumption and deployment lifecycle support |

See the [AI architecture](../architecture/ai-native.md) for target designs.
Propose priorities or concrete work through
[GitHub issues](https://github.com/pangrova/pangrova/issues).
