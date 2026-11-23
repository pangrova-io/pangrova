---
title: "Iceberg"
description: "Apache Iceberg connector configuration and feature support"
---

# Iceberg

Configure the Iceberg connector and review supported table features.

### Ambiguous commits

A catalog commit timeout or an interruption after submission has an UNKNOWN
outcome unless a durable commit marker proves publication. An unchanged
snapshot after the verification grace period does not prove failure: the remote
commit may still finish. UNKNOWN commits retain their staged files and must be
reconciled before retrying; they are not reported as retryable transient errors.


## Iceberg Connector

Apache Iceberg provides ACID transactions, schema evolution, and time travel on data lakes.

### Catalog Configuration

The YAML below is a reference format; automatic startup registration is planned.
Use the SQL registration path below to create the runtime catalog.

```yaml
# etc/catalog/my-iceberg.yml
connector: iceberg

properties:
  type: hadoop          # hadoop, hive, rest, jdbc
  warehouse: "s3://my-bucket/iceberg-warehouse"
  # uri: "thrift://hive-metastore:9083"  # Required when type=hive
```

### SQL Registration

```sql
CREATE CATALOG my_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 's3://my-bucket/iceberg-warehouse'
);
```

### Catalog Types

| Type | Description | Required Properties |
|------|-------------|-------------------|
| `hadoop` | File-system based catalog (default) | `warehouse` |
| `hive` | Hive Metastore backed catalog | `warehouse`, `uri` |
| `rest` | REST catalog (e.g., Polaris, Unity) | `uri` |
| `jdbc` | JDBC backed catalog | `uri`, `jdbc.user`, `jdbc.password` |

For a PostgreSQL-backed JDBC catalog, a query that names a table using its exact catalog spelling can resolve it without enumerating the namespace. The planner still binds the selected snapshot and authorizes the source. Case-insensitive aliases use the catalog spelling.

The selected read schema is encoded once for the connector handle and its
fingerprint. Historical reads use the selected snapshot's schema even when the
current table has a different schema.

### Iceberg Feature Boundaries

| Feature | Status | Notes |
|---------|--------|-------|
| Schema evolution | Supported | ADD/DROP/RENAME COLUMN |
| Iceberg v3 `unknown` | Supported for optional top-level columns | Reads return NULL, writes omit the physical Parquet column, and ALTER COLUMN TYPE can introduce a concrete type. Non-NULL writes are rejected. |
| Partition evolution | Supported | SET PARTITION SPEC |
| Time travel | Supported | FOR SYSTEM_TIME AS OF / FOR SYSTEM_VERSION AS OF |
| MERGE INTO | Supported | Row-level deletes via position delete files |
| INSERT OVERWRITE | Supported | Dynamic and static overwrite |
| COMPACT / OPTIMIZE | Connector procedures and standalone statement | `CALL catalog.compact(...)` performs compaction; the standalone SQL statement uses the controlled maintenance backend |
| Clustering | Native sort declaration or interleaved keys | [Bounded Iceberg v2 rewrite](../../sql-reference/maintenance.md#clustering) with Worker staging and fenced Coordinator publication |
| CDC | Append and bounded row mutations | [CDC SQL](../../sql-reference/streaming.md) lists the table, source partition, and row-image requirements |
| VACUUM | Connector procedures only by default | Standalone `VACUUM` invokes orphan cleanup with a 24-hour cutoff, not snapshot expiration; use `CALL ...remove_orphan_files` / `expire_snapshots` |
| Hidden partitioning | Supported | year/month/day/hour/bucket/truncate transforms |
| Metadata tables | Partially supported | `$snapshots`, `$history`, `$refs`, `$manifests`, `$partitions`, and supported projections of `$files`/`$entries`. |

Metadata Avro containers support `null`, `deflate`, and `zstandard` compression.
The Native reader rejects unsupported codecs and malformed or oversized blocks.


### Snapshot metadata

Incremental append eligibility follows the snapshot parent chain over `(base, target]`.
Snapshot IDs need not increase: a retained snapshot with ID `900` can be the parent
of snapshot `800`. Both endpoints must exist, and the base must be an ancestor of
the target. Equal existing endpoints represent an empty interval. Every intervening
commit must be an append without delete manifests; an overwrite is not append-only
even if it creates no delete manifest. These checks follow the
[Iceberg snapshot specification](https://iceberg.apache.org/spec/#snapshots).

`SELECT * FROM catalog.schema.table$snapshots` and
`SHOW SNAPSHOTS FROM catalog.schema.table` expose retained Iceberg snapshots.
Expired snapshots are absent; an empty table returns no rows.

| Column | Type | Meaning |
|--------|------|---------|
| `committed_at` | `TIMESTAMP WITH TIME ZONE` | Commit instant, with microsecond precision |
| `snapshot_id` | `BIGINT` | Snapshot identifier |
| `parent_id` | `BIGINT` | Parent snapshot, or NULL |
| `operation` | `VARCHAR` | Commit operation |
| `manifest_list` | `VARCHAR` | Manifest-list location, or NULL for legacy embedded manifests |
| `summary` | `MAP<VARCHAR, VARCHAR>` | Snapshot summary, excluding the separate `operation` field |

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM my_iceberg.analytics.orders$snapshots
ORDER BY committed_at DESC, snapshot_id DESC;
```

A missing or empty JSON summary produces NULL. A summary containing only an
operation produces an empty map. Snapshot timestamps are read as exact instants,
without floating-point conversion.


### Partition statistics

`SELECT * FROM catalog.schema.table$partitions` and
`SHOW PARTITIONS FROM catalog.schema.table` expose the current snapshot's live
file statistics for Iceberg v2/v3 tables. Data, position-delete and equality-delete
files contribute to their respective counters. Partitions containing only delete
files are included; deleted manifest entries are excluded. Empty input has no rows.

| Column | Type |
|--------|------|
| `partition` | Unified partition STRUCT |
| `spec_id` | INTEGER |
| `record_count` | BIGINT |
| `file_count` | INTEGER |
| `total_data_file_size_in_bytes` | BIGINT |
| `position_delete_record_count` | BIGINT |
| `position_delete_file_count` | INTEGER |
| `equality_delete_record_count` | BIGINT |
| `equality_delete_file_count` | INTEGER |
| `last_updated_at` | TIMESTAMP WITH TIME ZONE |
| `last_updated_snapshot_id` | BIGINT |

Unpartitioned tables omit `partition` and `spec_id`. Spec evolution preserves the
SDK's distinction between absent dimensions and explicit NULL values. Signed
floating zeros remain distinct partition keys, while all NaN payloads form one
key. Statistics are file metadata counts, rather than the result of applying
row-level deletes to data records.

`last_updated_at`, `last_updated_snapshot_id` and `spec_id` come from one record
at the maximum retained entry snapshot commit time. Tied timestamps may choose
any matching record. If all contributing entry snapshots have expired, both
last-update fields are NULL and `spec_id` is zero, matching the Iceberg SDK.
The same base-table SELECT privilege and row/column policy protections apply to
both metadata SQL forms.

Equality-delete reads remain unsupported until cross-engine qualification passes.
Split planning rejects them with the table, snapshot and delete-file identity,
for both row projections and aggregates. Native split admission also rejects
unqualified equality deletes and unknown delete types; they cannot silently skip
filtering or enter the experimental equality reader through a direct split.

Iceberg v3 deletion-vector reads select the Puffin blob by its referenced data
file and preserve 64-bit row positions. Readers validate the container, payload
length, IEEE CRC-32 and cardinality before applying deletes. Ambiguous or missing
blob identity and corrupt payloads fail the query; compressed Puffin footers and
compressed DV blobs are currently rejected explicitly.
