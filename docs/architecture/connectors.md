---
title: "Connector Design"
description: "Connector SPI, plugin isolation, Substrait mapping, and Iceberg/Paimon/Lance boundaries"
sidebar_label: "Connector Design"
---

# Pangrova — Connector Design

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The Trino-style connector SPI, the Substrait plan abstraction, and federated queries across Iceberg and Paimon.

## 1. Trino-Style Plugin Architecture

### 1.1 Overall Architecture

![Connector Plugin Architecture](/img/diagrams/generated/connectors-01-7ad796f0cc.svg)

### 1.2 Core SPI Interfaces

All Connector contracts are owned by **`pangrova-connector-api`**. The artifact
contains caller values and implementation contracts under `org.pangrova.connector.api`. `Connector` and `ConnectorFactory` are in `.api.plugin`;
`ConnectorContext` is in `.api.plugin`. Provider JARs register
`META-INF/services/org.pangrova.connector.api.plugin.ConnectorFactory`.
The old `org.pangrova.spi.connector` classes are removed, without forwarding wrappers.

The API artifact depends only on Common, Types and Batch API. Engine types and scalar
values belong to `org.pangrova.types`; pages, blocks and `RowPageConverter` belong to
`org.pangrova.batch.api`. SDK types and complex codecs do not enter the contract closure.
`FixedSplitSource`, `PartitionDiscoveryCache`, `ConnectorExceptionTranslator` and
`ConnectorBinaryPageCodec` belong to Connector Runtime. Only fixed splits and the
binary codec currently have production callers; the cache/error helpers are not
proof of a wired capability. `IndexPredicateCodec` belongs to `indexing-codec` and
is not currently used by a production transport.

`ConnectorQueryAuthority` owns operation admission and drain evidence without
depending on metadata. `.api.metadata.ConnectorMetadataScope` adds retained metadata
and cleanup to the same state-bound query owner. Metadata consumers require that
capability from the exact session authority; they cannot derive it from a query ID
or a raw task-issuance authority. Cleanup retains failed resources for retry and
continues to require closed admission and physical operation drain.

External commit receipts and declarations belong to `.api.commit`. Query-owned
`WriteCommitJournal`, `WriteCommitOutbox`, `WriteCommitEntry`, `WriteCommitFingerprint`
and `JournalWriteException` belong to `persistence-query-api`. The same-connection
`ExecutionTransactionFence` is in `persistence-jdbc-api`, not the Connector API.
`SourceCatalogDeployment`, `SourceTableKey`, `SourceTableState` and `SourceSnapshot`
are standalone API values; the `ManagedSourceCatalog` extension consumes them.
This split preserves the original validation, authority checks and SQL transaction owners.

`FixedSplitSource` snapshots its input, returns at most the positive requested batch
size, and supports idempotent close. `ConnectorBinaryPageCodec` uses **CUSTOM_BINARY**,
not Arrow IPC. It supports null/string/int/long/double/boolean; unsupported values
fail explicitly instead of becoming strings. Decoding rejects wrong format, dimensions,
version, truncation, excessive counts/lengths, invalid UTF-8/booleans and trailing bytes.
Nonempty rows require at least one distinct column name. It borrows the input page; its caller closes that page. Columnar row conversion preserves
binary values as typed array entries because an untyped slice cannot distinguish bytes
from strings. General Arrow encoding remains in Batch Arrow.

Factory compatibility/role negotiation and generic Plugin Runtime extraction remain
separate work. The design diagrams below are conceptual; their version/conflict-management
boxes do not constitute evidence that those target capabilities are implemented.

![// ConnectorFactory - connector factory](/img/diagrams/generated/connectors-02-a5448245bc.svg)

For planning, a named table lookup calls the connector's `getTableHandle` and
`getTableMetadata` for that table only. Calcite table-name enumeration calls
`listTables` without loading each table's columns; the name cache is invalidated
after local DDL and expires for changes made by other coordinators. A failed
connector metadata call propagates as a resolution error rather than being
reported as a missing table. Iceberg table-handle lookup loads current catalog
state so an external drop or recreation is visible immediately.

---

## 2. ClassLoader Isolation Design

### 2.1 ClassLoader Hierarchy

![ClassLoader Hierarchy](/img/diagrams/generated/connectors-03-38480fcbc0.svg)

### 2.2 PluginClassLoader Implementation

The connector runtime owns class loading, connector factory discovery and plugin
lifecycle. `PluginClassLoader` delegates public contract packages and selected
signature types to the host. `SharedPackagePolicy` in
`org.pangrova.connector.runtime.enums` lists these boundaries. The same
`ConnectorFactoryRegistry` validates providers for the Coordinator and Worker.

The host owns engine `types`, batch, connector, function and streaming contracts.
Protocol sharing covers the AI task lifecycle port, generated v2 wire messages and
their Protobuf runtime. Query exposes its execution identity, trace context and the
governance column derivation enum. Those types retain host identity when a plugin
bundles its own copy. Shared Common classes are limited to API annotations, the
types used by contract signatures, and their causal exceptions. Common algorithms,
HTTP DTOs and persistence implementations retain plugin-first resolution.

`BROAD`, `NARROW` and `CUSTOM` select additional packages searched on the host
first. `NARROW` includes the data-plane and logging packages. `BROAD` also shares
Guava, Hadoop, Parquet, ORC and Jackson. Package matching honors Java package
boundaries. Private classes resolve from the plugin JARs; only JDK platform classes are available as fallback.

### 2.3 Connector plugin deployment and lifetime

Both server roles read the optional `connector.plugin-directory` setting from their
own deployment configuration. The default inventory contains the bundled factories on
the process classpath. Each immediate child of the configured root is a plugin
installation. Place its provider JAR and its private dependency JARs directly in
that directory. A plugin JAR declares its factory in
`META-INF/services/org.pangrova.connector.api.plugin.ConnectorFactory`.
Deploy only administrator-trusted JARs. External installations currently use the
NARROW shared-package policy and must ship their SDK dependencies.

At startup, each role builds its own `ConnectorPluginInventory`. The inventory
sorts the installation directories and JARs, allocates one classloader per
installation, and resolves the factory class from that installation. Factory
construction, validation and catalog creation run with the plugin's thread
context classloader. Connector instances can use their defining classloader for
lazy resource lookup. A factory declares its supported process roles. Conflicting
names for a role, incompatible factories, unreadable roots and installations
without a factory abort discovery and close the acquired loaders. To deploy a
connector update, restart the role with its new installation.

Coordinator closes the catalog registry after query drain, then releases the
inventory. Worker closes admission, drains connector leases and shuts down
connectors before releasing the loaders. A Worker close with an active lease
reports the owner still in use. Release that lease and retry shutdown. Standalone
constructs independent inventories through the two role entry points.

### 2.4 CatalogRegistry Implementation



> **Authoritative definition**: The full `CatalogRegistry` implementation (PostgreSQL persistence, ZooKeeper synchronization, health monitoring, dynamic catalog lifecycle management) is defined in [metadata.md §3.2](./metadata.md). This section describes only the Connector-side integration points.

Note: Catalog registration and lifecycle management are handled uniformly by `MetadataService`/`CatalogRegistry` (see [metadata.md §3.2](./metadata.md)). This section describes the target-state implementation of the Connector-side Catalog registration, table-reference resolution, and plugin loading.

Iceberg table-handle lookup reads the current catalog state for each resolution. A
process-wide time-based table cache cannot make `DROP TABLE` followed immediately by
`CREATE TABLE IF NOT EXISTS` correct when another catalog owner changes the table:
the cached handle could outlive the drop and suppress the create. A selected read
still pins its own immutable metadata and source identity through the query lifecycle.

```java
/**
 * Catalog manager — Connector-side view
 *
 * Note: the Catalog here is Pangrova's Catalog abstraction, not the Iceberg/Paimon Catalog.
 * - Pangrova Catalog: holds a Connector reference, used for SQL three-part table name routing
 * - Iceberg Catalog: org.apache.iceberg.catalog.Catalog, used for Iceberg table management
 *
 * The authoritative CatalogRegistry (07 §3.2) manages:
 *   - Catalog persistence (PostgreSQL) and cross-Coordinator sync (ZooKeeper Watch)
 *   - Catalog lifecycle (CREATING → ACTIVE → DEGRADED → SUSPENDED → DROPPING)
 *   - Health monitoring and dynamic load/unload
 *
 * This Connector-side view shows how ConnectorPluginInventory loads ConnectorFactory
 * instances and how table references are resolved through the Connector SPI.
 * At runtime, CatalogRegistry (07 §3.2) uses its ConnectorHandleFactory (§2.3)
 * for Connector-backed catalogs via loadConnectorCatalog().
 */
// See metadata.md §3.2 for the full CatalogRegistry implementation.
// The config-file-based loadCatalogs() below is the bootstrap path;
// runtime CREATE/DROP CATALOG goes through MetadataService (07 §3.1).
```

---

## 3. Substrait Protocol Abstraction Design

### 3.1 Design Goals

![Protocol Abstraction Goals](/img/diagrams/generated/connectors-05-e4151d28e6.svg)

### 3.2 Protocol Layer Architecture

![Protocol Layer Architecture](/img/diagrams/generated/connectors-06-2512adf421.svg)

### 3.3 Responsibility Boundary Between PangrovaReadExtension and DataSplit

Pangrova's read protocol is split into two layers:

| Layer | Proto Message | Definition Location | Responsibility | Consumer |
|---|---|---|---|---|
| Substrait extension layer | `PangrovaReadExtension` | This section (connectors.md) | Table-level read description within the Substrait Plan, used by the Planner for optimization | Calcite Planner → Substrait Converter |
| JNI transport layer | `DataSplit` | canonical v2 `task.proto` (single authority) | Snapshot-pinned generic execution input for one source range, executed directly by Native | Java SplitManager → canonical TaskRequest → Native task parser |

Conversion flow:
![PangrovaReadExtension (table-level, Planner phase)](/img/diagrams/generated/connectors-07-599361ec6d.svg)

PangrovaReadExtension is an intermediate Planner representation and is not
passed directly to Native. Native receives only canonical `DataSplit` values;
Iceberg/Paimon read fields live in the typed `FileSplitDetail` packed into
`connector_detail`.

```protobuf
/**
 * PangrovaReadExtension — table-level read extension within the Substrait Plan
 *
 * Purpose: in the Planner phase, describes "which version of which table to read"; contains no file-level details
 * Conversion: Java SplitManager expands it into repeated DataSplit
 */
message PangrovaReadExtension {
    string source_id = 1;                    // data source identifier
    string source_type = 2;                  // "iceberg", "paimon"

    // Version info (abstract)
    string version_id = 3;                   // Iceberg: snapshot_id, Paimon: commit_id
    int64 version_timestamp_ms = 4;          // version timestamp

    // Incremental read
    string from_version_id = 5;              // incremental read start (exclusive)
    string to_version_id = 6;               // incremental read end (inclusive, default = version_id)

    // Schema
    int32 schema_version = 7;
    bytes arrow_schema = 8;                  // Schema required by the query

    // Read config
    ReadMode read_mode = 9;
    map<string, string> properties = 10;     // format-specific properties (iceberg.*, paimon.*)
}

enum ReadMode {
    READ_MODE_SNAPSHOT = 0;                  // snapshot read (default)
    READ_MODE_TIME_TRAVEL = 1;              // Time Travel
    READ_MODE_INCREMENTAL = 2;              // incremental read
    READ_MODE_CHANGELOG = 3;                // Changelog read (Paimon)
}
```

### 3.4 Storage Adapter Mapping

The conversion from PangrovaReadExtension (Planner layer) → SplitInfo (execution layer) is performed by the Java SplitManager:

![PangrovaReadExtension → SplitInfo Conversion Mapping](/img/diagrams/generated/connectors-09-0523c1826f.svg)

### 3.5 Java SplitManager Conversion Interface

![/**](/img/diagrams/generated/connectors-10-09efff5393.svg)

---

## 4. Iceberg Connector Implementation

### 4.1 Module Structure

![pangrova-connector/pangrova-connector-iceberg/](/img/diagrams/generated/connectors-11-0c220e0261.svg)

### 4.2 Core Implementation

```java
public class IcebergConnectorFactory implements ConnectorFactory {
    @Override public int apiVersion() { return ConnectorFactoryContract.CURRENT_API_VERSION; }
    @Override public Set<ConnectorRole> roles() { return Set.of(ConnectorRole.COORDINATOR, ConnectorRole.WORKER); }
    @Override public void validateConfiguration(Map<String, String> config) { normalizeCatalogProperties(config); }

    @Override
    public String getName() { return "iceberg"; }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Configuration hadoopConf = new Configuration();
        config.forEach(hadoopConf::set);
        Catalog catalog = CatalogUtil.loadCatalog(config.get("catalog-impl"), catalogName, config, hadoopConf);
        return new IcebergConnector(catalog, config, context);
    }
}

public class IcebergSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableHandle tableHandle, DynamicFilter dynamicFilter, Constraint constraint) {

        IcebergTableHandle icebergTable = (IcebergTableHandle) tableHandle;
        Table table = catalog.loadTable(icebergTable.getTableIdentifier());

        TableScan scan = table.newScan();
        if (constraint.getSummary() != TupleDomain.all()) {
            Expression filter = convertToIcebergExpression(constraint.getSummary());
            scan = scan.filter(filter);
        }
        scan = scan.select(icebergTable.getProjectedColumns());

        if (icebergTable.getSnapshotId().isPresent()) {
            scan = scan.useSnapshot(icebergTable.getSnapshotId().get());
        }

        List<IcebergSplit> splits = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                splits.add(new IcebergSplit(
                    task.file().path().toString(), task.start(), task.length(),
                    task.file().format(), task.residual(), task.deletes()));
            }
        }
        return new FixedSplitSource(splits);
    }
}
```

---

### 4.3 Managed Lance connector

`pangrova-connector-lance` serves managed Lance datasets from a connector-owned warehouse
(local `file:///` or `s3://`). It records catalog identity, tenant scope, table UUID,
revision, schema fingerprint, and publication evidence in the shared metadata authority and
never opens a JDBC connection. Reads are snapshot-pinned and verified against the Native
manifest; writes use the Native publication barrier with abort and format/base-version
fencing. `UPDATE`, `DELETE`, `MERGE`, column DDL, views, system tables, and maintenance are
not declared. Configuration and the full capability matrix are in
[Lance](../getting-started/connectors/lance.md).

## 5. Extension Point Design

### 5.1 Extension Point Overview

| Extension Point | Location | Responsibility | Extension Method |
|--------|------|------|----------|
| Storage Adapter (Java) | pangrova-connector-{format}/ | Maps storage-specific metadata to PangrovaReadExtension | Implement the StorageAdapter interface |
| File Format Reader (C++) | pangrova-worker-native/src/engine/ | Reads specific file formats (Parquet/ORC/Avro) | Implement the DataSourceFactory interface |
| Delete Handler (C++) | pangrova-worker-native/src/engine/delete/ | Handles different types of delete files | Implement the DeleteHandler interface |
| TableMaintenance (Java) | pangrova-connector-{format}/ | Async compaction, orphan cleanup, snapshot expiration | Implement TableMaintenance interface, see [streaming.md §2.9](./streaming.md) (authoritative) |
| Properties Handler | properties map in protocol | Passes format-specific metadata | Convention-based key prefix |

**Write SPI boundary.**

`ConnectorPageSinkProvider` and `ConnectorPageSink` are the canonical connector write SPI.
The Worker `NativeWriteExecutor` appends columnar batches and obtains a staged metadata
fragment with `finishWithMetadata()`; the Coordinator publishes it with
`ConnectorMetadata.finishDistributedWrite()`. The CDC runtime writes through `CdcWriteSink`
and `ConnectorPageSinkCdcWriter`. `TableWriter` was retired: its single-shot batch hierarchy had
no production factory, consumer or close owner.

**Relationship between ClassLoader isolation and the Native layer**: Java ClassLoader isolation applies only to Java-layer Connector plugins (metadata, Split planning, transaction management). The Native-layer DataSourceFactory is registered by the `connector_type` string (such as `"iceberg"`, `"paimon"`); it is a global, stateless format reader and is not affected by Java ClassLoader isolation. When multiple Connectors need to read the Parquet format, the Native layer shares the same `ParquetFormatReader` instance and distinguishes the semantics of different lakehouse formats via `SplitInfo.connector_info` (oneof) (such as Iceberg Delete File handling vs Paimon LSM Merge). Therefore, Java-layer ClassLoader isolation and Native-layer DataSourceFactory registration are two independent dimensions that do not interfere with each other.

### 5.2 Properties Key Specification

#### 5.2.1 Naming Conventions

All Properties Keys must follow the `{namespace}.{category}.{name}` three-part naming scheme:

```
• iceberg.* : Iceberg-specific properties
• paimon.*  : Paimon-specific properties
• hudi.*    : reserved for the planned Hudi Connector
• pangrova.*  : Pangrova internal properties
```

#### 5.2.2 SplitInfo.extensions Key Specification

`SplitInfo.extensions` (defined in native-execution.md §5.3) is used to pass Connector-specific extension metadata. Keys must use a Connector prefix; bare keys without a prefix are forbidden.

| Key Format | Value Type | Description | Example |
|----------|--------|------|------|
| `{connector}.encryption.key_id` | bytes | encryption key ID | `iceberg.encryption.key_id` |
| `{connector}.compression.codec` | bytes (UTF-8) | compression codec | `paimon.compression.codec` |
| `{connector}.cache.hint` | bytes (UTF-8) | cache hint | `iceberg.cache.hint` = `"hot"` |
| `pangrova.io.coalesce_threshold` | bytes (int64 LE) | I/O coalesce threshold | overrides the default coalescing policy |
| `pangrova.io.prefetch_size` | bytes (int64 LE) | prefetch size | overrides the default prefetch policy |

Rules:
- A Key must be prefixed with a registered Connector name or `pangrova`
- The value type is uniformly `bytes`; the encoding is determined by the Key semantics (UTF-8 string / int64 LE / raw bytes)
- New Keys must be registered in this table; Connector implementations must not define unregistered Keys on their own
- When the Native layer encounters an unrecognized Key, it ignores it (without erroring), ensuring forward compatibility

#### 5.2.3 PangrovaReadExtension.properties Key Specification

`PangrovaReadExtension.properties` (defined in §3.3 of this document) is used to pass format-specific properties during the Planner phase.

| Key | Value | Consumer | Description |
|-----|-----|--------|------|
| `iceberg.merge_on_read` | `"true"` / `"false"` | IcebergSplitConverter | whether to enable MOR reads |
| `paimon.merge.engine` | `"deduplicate"` / `"partial_update"` / `"aggregation"` | PaimonSplitConverter | Merge Engine type |
| `paimon.changelog.producer` | `"none"` / `"input"` / `"lookup"` / `"full_compaction"` | PaimonSplitConverter | Changelog Producer type |
| `pangrova.read.sample_ratio` | `"0.01"` ~ `"1.0"` | SplitManager | sampling read ratio |

Rules:
- A Key must be prefixed with a Connector name or `pangrova`
- The value type is uniformly `string` (map<string, string>)
- New Keys must be registered in this table

### 5.4 Extension point status

Every extension family must end in one explicit state: **wired** (a production
`ServiceLoader` site or explicit construction on a reachable path), **test-only**,
**descriptor-only**, **zero-provider**, **zero-impl**, or **unwired**.
`.pangrova/modules.yaml` declares providers per artifact and
`ModuleDependencyPropertyTest` fails when descriptors and declared families diverge.
"Future extension" is not an acceptable resting state: a descriptor with no
production loader must be wired or deleted, and a registered provider that no
production caller reaches must not be described as supported.

---

## 6. Federated Query Design

### 6.1 Federation Query Architecture

![Federation Query Architecture](/img/diagrams/generated/connectors-14-174ddd2c7d.svg)

### 6.2 Connector Type Classification

![Connector Type Classification](/img/diagrams/generated/connectors-15-b18cd8e136.svg)

### 6.3 Cross-Source Join Optimization

![Cross-Source Join Optimization](/img/diagrams/generated/connectors-16-74332bf683.svg)

### 6.4 Federation Query Execution Flow

![Federation Query Execution Flow](/img/diagrams/generated/connectors-17-3e8b960cef.svg)

---



## 7. Observability Integration

This section defines the metrics and tracing integration points for the connector layer. For the full observability design, see [observability.md](./observability.md).

### 7.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.connector.splits.generated` | ConnectorSplitManager | Splits generated per query |
| `pangrova.connector.metadata.latency` | ConnectorMetadata | Metadata operation latency |
| `pangrova.connector.pagesource.bytes` | ConnectorPageSource | Bytes read through page source |
| `pangrova.connector.pagesink.bytes` | ConnectorPageSink | Bytes written through page sink |

### 7.2 Tracing Spans

Connector operations are children of the `query.optimize` and `task.execute` spans defined in [14 §8.2](./observability.md):

- `connector.getSplits` — Split generation (child of `query.schedule`)
- `connector.getMetadata` — Metadata resolution (child of `query.optimize`)
- `connector.readPage` — Page source read (child of `native.execute`)

### 7.3 Log Events

- Plugin load/unload events (INFO)
- Connector health check failures (WARN)
- ClassLoader isolation violations (ERROR)

See [14 §7](./observability.md) for structured logging format.

---

## REST Catalog API (v1)

Pangrova implements the open table format REST Catalog specification (v1), enabling compatibility with Polaris, Tabular, Spark, Trino, and any REST-catalog-compatible client.

### Endpoints

| Path | Method | Description |
|------|--------|-------------|
| `/v1/config` | GET | Catalog configuration |
| `/v1/oauth/tokens` | POST | OAuth2 token exchange |
| `/v1/namespaces` | GET/POST | List/Create namespaces |
| `/v1/namespaces/{ns}` | GET/HEAD/DELETE | Namespace CRUD |
| `/v1/namespaces/{ns}/tables` | GET/POST | List/Create tables |
| `/v1/namespaces/{ns}/tables/{t}` | GET/HEAD/DELETE | Table CRUD |
| `/v1/namespaces/{ns}/views` | GET | List views |

### Authentication

OAuth2 via `/v1/oauth/tokens`: `client_credentials` and `token_exchange` grants.

### Namespace Mapping

| REST Catalog | Pangrova |
|-------------|--------|
| Top-level namespace | Catalog |
| Second-level namespace | Schema |
| Table within namespace | Table within catalog.schema |

### Client Configuration

**Spark**: `spark.sql.catalog.pangrova.type=rest`, `uri=http://coordinator:8080`
**Trino**: `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://coordinator:8080`
**Polaris**: `catalog.type: rest`, `uri: http://coordinator:8080`

---

## 8. Connector query lifetime and admission

Connector reads are admitted through the authority issued by the Coordinator query
creation owner. The authority binds the query state machine to one
`TaskIdentityRegistry.Scope`; session copies preserve that object. A query identifier
or session property cannot create a read lifetime.

- The authority is indexed before compilation. A selected catalog lookup retains an
  operation permit, retains the original authenticated metadata instance before
  `BEGIN`, and keeps a failed `BEGIN` instance for cleanup. Catalog `BEGIN` is
  single-flight. Schema discovery without an issued query stays metadata-only and
  registers no splits or readers.
- Cleanup requires closed admission plus drain of the exact Scope's issued tasks and
  operations. The drain check claims cleanup and is not a read-only status query.
  Metadata cleanup traverses retained instances outside locks; successful instances
  are released once, failed instances stay retryable. Pending terminal transitions
  reject new operations but are not physical-drain evidence.
- Paimon retains selected SDK tables, splits, and readers in the same query Scope.
  A reader owns its operation permit until the SDK reader close succeeds. The first
  read binding is frozen per table; changing an explicit snapshot or schema selector
  fails instead of silently repinning. Reusing the exact historical bound handle,
  including its historical schema, remains valid; limit-only variation preserves the
  source. Multiple historical occurrences of one table in a single query remain
  unsupported.
- Connector shutdown seals admission before checking readers and incomplete SDK
  binding resolutions. A rejected close retains ownership and leaves the catalog open
  for retry after real I/O exits. Diagnostics expose only pending split and active
  reader counts, never mutable registries or SDK handles.
