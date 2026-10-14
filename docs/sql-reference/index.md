---
title: "SQL Reference"
description: "How to interpret Pangrova SQL support and runtime requirements"
---

# SQL Reference

This reference separates syntax recognition from executable support. A parser rule or
Calcite operator proves only that SQL can be parsed or validated; executable support also
requires a production dispatch path, a connector or native implementation, and a test at
the relevant boundary.

## Support levels

| Level | Meaning |
|---|---|
| End-to-end | Production entry point, wiring, implementation, and integration/E2E coverage exist |
| Runtime-dependent | The execution path exists but requires a connector, model provider, index backend, or external service |
| Planning-only | Parser/planner support exists, but the repository does not establish an end-to-end execution contract |
| Planned | Not part of the published SQL reference; keep proposals in roadmap material |

## Reference map

| Area | Scope |
|---|---|
| [Queries](queries.md) | Query expressions, joins, grouping, ordering, and set operations |
| [DML](dml.md) | INSERT, UPDATE, DELETE, and MERGE routing |
| [DDL](ddl.md) | Catalog, schema, table, view, and index metadata operations |
| [Maintenance](maintenance.md) | Connector-dependent maintenance statements and procedures |
| [Streaming](streaming.md) | Streaming and CDC syntax with runtime prerequisites |
| [Versioning](versioning.md) | Snapshot, branch, tag, and incremental-read behavior |
| [Session](session.md) | Session properties |
| [Functions](functions/index.md) | Scalar, aggregate, window, table, spatial, vector, and AI functions |

Connector-specific behavior takes precedence over generic examples. When adding a statement,
update its dispatch implementation and tests before documenting it as end-to-end supported.

Numeric literals accept decimal and scientific notation, such as `4.`, `.25e2`,
and `3.3283234E-4`. The parser preserves their decimal value exactly before
the planner coerces it to the expression or target column type.
