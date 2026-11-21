---
title: "Connectors"
description: "Choose between Iceberg, Paimon, and Lance, and understand the shared Connector SPI"
---

# Connectors

Connectors translate SQL planning and execution into table-format metadata, splits, page
sinks, and catalog commits. The same Connector SPI also provides classloader-isolated
plugins for additional formats.

## Built-in connectors

| Connector | Registration | Best for | Important limits |
|---|---|---|---|
| [Iceberg](iceberg.md) | `CREATE CATALOG ... USING iceberg` | ACID tables, snapshot travel, hidden partitioning, metadata tables | Format-version and metadata-table support vary; validate the exact table feature |
| [Paimon](paimon.md) | `CREATE CATALOG ... USING paimon` | Streaming-oriented lake tables with merge engines and Paimon procedures | Merge/file policies and procedure coverage have explicit limits |
| [Lance](lance.md) | `CREATE CATALOG ... USING lance` | Managed Lance datasets with Native dataset control | Requires Native Lance FFI; no row-level UPDATE/DELETE/MERGE, column DDL, views, or maintenance |

## Shared behavior

- A catalog names a deployment-scoped configuration; tables carry tenant and deployment
  identity into planning and execution.
- Reads are bound to a resolved table version. The connector returns metadata, splits, and
  a read binding that the Worker validates before scanning.
- Writes return commit fragments to the Coordinator, which owns the final fenced catalog
  commit. The connector implementation owns format publication and abort semantics.
- Capability flags are fail-closed: an operation is rejected when the connector does not
  declare it, rather than attempted with an implicit fallback.

## Connector SPI and plugins

- [Connector design](../../architecture/connectors.md) describes the factory contract,
  classloader isolation, Substrait mapping, and federation.
- External plugins load from `connector.plugin-directory`; installation requires restart,
  and duplicate names or incompatible versions fail startup.
- [Connector configuration](../../configuration/connectors.md) lists catalog properties,
  storage backends, and examples.

## Choosing a connector

Use Iceberg or Paimon when the table already exists in that format and its feature set
matches the workload. Use Lance for managed datasets that require the Native dataset
control path. A connector being installed does not imply that every SQL operation,
predicate, or write mode is supported; check the connector page and
[current capability status](../capability-status.md).
