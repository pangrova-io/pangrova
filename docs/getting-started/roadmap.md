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

## In progress and open items

The following are known incomplete areas; they are not end-to-end support claims.

- **AI-native execution:** public-entry AI SQL and retrieval with a real provider,
  the complete Native build, ASan, and release gates. `DROP EMBEDDING MODEL` remains
  fail-closed until durable dependency reservations and a resumable CASCADE job
  exist. Training Dataset has catalog metadata but no SQL entry, durable multi-source
  export, Native plan wiring, or streaming reader. Evaluation has only control-plane
  metadata; its data plane, fenced metric reports, and RBAC subject resolution are
  open.
- **System tables:** full Iceberg `$files`/`$entries`, the Paimon `$files`
  type/filesystem matrix, and non-local `$branches` remain open. Quoted table names
  containing a literal `$` still require a typed relation identity through the
  parser, Calcite, and connector boundaries.
- **Table and file plane:** Native DIRECTORY execution, billion-entry directory
  publication, 100 GiB transfer, PB-scale data, and Linux production qualification.
- **Modularization:** The module manifest records current package ownership and
  dependency rules. Role-specific distributions, provider isolation, Shuffle
  publication and release qualification have separate exit criteria in the
  [domain-modularization ADR](../architecture/domain-modularization-adr.md).

See the [AI architecture](../architecture/ai-native.md) for target designs.
Propose priorities or concrete work through
[GitHub issues](https://github.com/pangrova/pangrova/issues).
