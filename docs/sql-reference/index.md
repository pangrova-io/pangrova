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
| Implemented path | Production entry point and implementation are wired; runtime qualification is separate |
| Runtime-dependent | A wired path requires a connector, configured model, compatible index artifact, or external service |
| Partial / not wired | Components exist, but default dispatch, provider injection, or a required lifecycle is incomplete |
| Syntax only | Parser/AST recognition does not reach a working default execution path |
| Target | Intended behavior, explicitly labelled; no executable support claim |

A test file is evidence of intended coverage; only a recorded executed result establishes
that the tested path passed for its platform, dependencies, and revision.

## Reference map

| Area | Scope |
|---|---|
| [Queries](queries.md) | Query expressions, joins, grouping, ordering, and set operations |
| [DML](dml.md) | INSERT, UPDATE, DELETE, and MERGE routing |
| [DDL](ddl.md) | Catalog, schema, table, view, index, volume, and stage metadata operations |
| [Maintenance](maintenance.md) | Standalone maintenance, connector procedures, and clustering gaps |
| [Streaming](streaming.md) | Stream wiring, CDC limitations, and dynamic-table registration limits |
| [Versioning](versioning.md) | Snapshot, branch, tag, and incremental-read behavior |
| [Session](session.md) | Request context and current non-persisting SET behavior |
| [Functions](functions/index.md) | Scalar, aggregate, window, table, spatial, vector, and AI functions |

Connector-specific behavior takes precedence over generic examples. When adding a statement,
verify dispatch, implementation, and relevant executed tests before documenting it as supported.

Numeric literals accept decimal and scientific notation, such as `4.`, `.25e2`,
and `3.3283234E-4`. The parser preserves their decimal value exactly before
the planner coerces it to the expression or target column type.
