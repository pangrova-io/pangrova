---
title: "Lance"
description: "Managed Lance Dataset connector: warehouse configuration, reads, writes, snapshots, and limits"
---

# Lance

The Lance connector exposes managed [Lance](https://lance.org/) datasets as
Pangrova tables. Lance files and manifests live in a connector-owned warehouse, while table
identity, tenant scope, revisions, and publication evidence live in Pangrova's shared metadata
authority. The connector never opens its own JDBC connection.

> **Status.** The adapter supports catalog/schema/table creation, snapshot-pinned reads,
> `INSERT`, `INSERT OVERWRITE`, distributed writes with abort, and time travel to a published
> version. Row-level `UPDATE`/`DELETE`/`MERGE`, column DDL, views, system tables, and table
> maintenance are not supported by this adapter.

## Prerequisites

- A Native runtime with Lance FFI support. Build it with
  `./pangrova-worker-native/bin/build-native.sh`; a Java-only deployment cannot read or
  write Lance datasets.
- The shared metadata authority required by the deployment. The connector validates catalog
  and tenant identity against it and rejects connector-level `jdbc.*` properties.
- A writable warehouse location reachable by every Worker that executes the query.

## Register a catalog

```sql
CREATE CATALOG lake_lance
USING lance
WITH (
  'warehouse' = 's3://analytics-lance/warehouse'
);
```

Local deployments can use an absolute path or a canonical `file:///` URI:

```sql
CREATE CATALOG local_lance
USING lance
WITH (
  'warehouse' = 'file:///var/lib/pangrova/lance-warehouse'
);
```

A per-catalog YAML reference may be kept in `${PANGROVA_CONF_DIR}/catalog/<name>.yml`, but
automatic startup registration is not active. The filename does not create a runtime
catalog.

```yaml
connector: lance
properties:
  warehouse: "s3://analytics-lance/warehouse"
```

### Warehouse validation

| Rule | Behavior |
|---|---|
| Required | The warehouse must be present and non-blank |
| Schemes | `s3://` with a bucket, `file:///` with an absolute path, or an absolute normalized local path |
| Rejected forms | Relative or non-normalized paths, `file:/`, `%`-escaped values, query strings, fragments, and user-info |
| Connector JDBC | `jdbc.url`, `jdbc.user`, and `jdbc.password` are rejected; the shared metadata authority is mandatory |
| Length | At most 8192 characters |

A trailing slash is removed before table paths are allocated. The catalog name, table name,
tenant, table UUID, and dataset URI form the durable identity; a dropped-and-recreated table
is a different incarnation even when it reuses the same name.

## Reading data

```sql
SELECT region, COUNT(*) AS orders
FROM lake_lance.sales.orders
GROUP BY region;
```

- Every scan is pinned to one catalog version. Splits and metadata for the query resolve the
  same hidden publication evidence, including the dataset manifest digest.
- The Native control plane validates the physical manifest and schema fingerprint before a
  historical snapshot is used.
- Row count comes from the durable table state. Column statistics are not published by this
  adapter, so the optimizer must not infer them.
- Reads fail closed when the table incarnation was dropped or replaced after the snapshot
  was selected.

### Time travel

```sql
SELECT *
FROM lake_lance.sales.orders
FOR SYSTEM_VERSION AS OF 42;
```

The requested version must be positive and must resolve to a published manifest with the
same table UUID, dataset URI, and schema fingerprint. Version `0` denotes a created but
unwritten table and is a valid selected snapshot.

## Writing data

```sql
CREATE TABLE lake_lance.sales.orders (
  order_id BIGINT,
  region   VARCHAR,
  amount   DECIMAL(18, 2)
);

INSERT INTO lake_lance.sales.orders VALUES (1, 'eu', 42.50);

INSERT OVERWRITE lake_lance.sales.orders
SELECT * FROM lake_iceberg.sales.orders;
```

| Operation | Support | Notes |
|---|---|---|
| `CREATE TABLE` | Yes | Requires exact ordered typed columns; the schema fingerprint is recorded |
| `INSERT` | Yes | Appends a new dataset version through the Native write path |
| `INSERT OVERWRITE` | Yes | Replaces table contents with a new version |
| Distributed write commit | Yes | Reserved through the shared authority, finalized through the Native publication barrier, and idempotent per attempt |
| Abort | Yes | An uncommitted attempt can be aborted with its exact durable journal context |
| `UPDATE` / `DELETE` / `MERGE` | No | The adapter declares no row-level delete capability |
| Column DDL | No | `ALTER TABLE` column operations are not implemented by this adapter |
| Views | No | View creation and replacement are unsupported |
| Maintenance | No | `COMPACT`, `OPTIMIZE`, `VACUUM`, and connector procedures are not declared |
| System tables | No | `$snapshots`, `$files`, and similar suffixes are not served |

A write commits only after the Native publication barrier returns the accepted manifest.
An `UNKNOWN` preparation outcome keeps reserved objects for replay rather than reporting a
retryable failure. The adapter fences writes with the format identity and base version, so a
write prepared against an older table incarnation cannot publish after a drop/recreate.

## Limitations and operational notes

- Lance dataset control is tenant-scoped. A session whose tenant differs from the table's
  tenant is rejected before metadata or splits are served.
- The warehouse must be visible at the same URI from the Coordinator and every Worker. The
  connector does not rewrite `file:///` paths per host.
- Backups must cover both the warehouse objects and the shared metadata rows. Copying only
  the dataset directory does not preserve Pangrova table identity or publication evidence.
- The connector publishes no column statistics and no table-maintenance capabilities; query
  plans must not assume either.

**See also:** [Connector design](../../architecture/connectors.md),
[Connector configuration](../../configuration/connectors.md),
[Distributed writes](../../architecture/distributed-writes.md)
