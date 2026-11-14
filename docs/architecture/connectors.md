---
title: "Connector Design"
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

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Connector Plugin Architecture                       │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │                        Plugin Manager                                  │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │    │
│  │  │  Plugin Loader  │  │ ClassLoader Mgr │  │ Dependency Mgr  │       │    │
│  │  │ • SPI Discovery │  │ • Isolation     │  │ • Version Check │       │    │
│  │  │ • JAR Scanning  │  │ • Parent First  │  │ • Conflict Res  │       │    │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │    │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │                      Connector SPI Layer                               │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │    │
│  │  │ Connector   │  │ Connector   │  │ Connector   │  │ Connector   │  │    │
│  │  │ Factory     │  │ Metadata    │  │ SplitMgr    │  │ PageSource  │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │    │
│  │  ┌─────────────┐  ┌─────────────┐                                    │    │
│  │  │ Connector   │  │ Connector   │          │  │
│  │  │ PageSink    │  │ Transaction │  Index & Statistics capabilities   │    │
│  │  └─────────────┘  └─────────────┘  are provided via ConnectorMetadata│    │
│  │                                     (applyFilter, getTableStatistics)│    │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│          ┌─────────────────────────┼─────────────────────────┐               │
│          ▼                         ▼                         ▼               │
│  ┌───────────────┐        ┌───────────────┐        ┌───────────────┐         │
│  │   Iceberg     │        │    Paimon     │        │ Hudi(planned)│          │
│  │  Connector    │        │  Connector    │        │  Connector    │         │
│  │ ClassLoader-1 │        │ ClassLoader-2 │        │ ClassLoader-3 │         │
│  └───────────────┘        └───────────────┘        └───────────────┘         │
└──────────────────────────────────────────────────────────────────────────────┘
```

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

```java
// ConnectorFactory - connector factory
public interface ConnectorFactory {
    String getName();
    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);
}

// Connector - main connector interface
public interface Connector {
    ConnectorMetadata getMetadata(ConnectorSession session);
    ConnectorSplitManager getSplitManager();
    ConnectorPageSourceProvider getPageSourceProvider();
    default ConnectorPageSinkProvider getPageSinkProvider() {
        throw new UnsupportedOperationException("Write not supported");
    }
    default ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return ConnectorTransactionHandle.empty();
    }
    default void commit(ConnectorTransactionHandle transaction) {}
    default void rollback(ConnectorTransactionHandle transaction) {}
    default void shutdown() {}
}

// ConnectorMetadata - metadata interface
public interface ConnectorMetadata {
    // Schema operations
    List<String> listSchemaNames(ConnectorSession session);
    void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties);
    void dropSchema(ConnectorSession session, String schemaName);

    // Table operations
    List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table);

    // DDL operations
    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting);
    void dropTable(ConnectorSession session, ConnectorTableHandle table);
    void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName);

    // Statistics
    TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table);

    // Predicate pushdown
    default Optional<ConstraintApplicationResult<ConnectorTableHandle>>
            applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        return Optional.empty();
    }

    // Projection pushdown
    default Optional<ProjectionApplicationResult<ConnectorTableHandle>>
            applyProjection(ConnectorSession session, ConnectorTableHandle table,
                            List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
        return Optional.empty();
    }
}

// ── ConnectorMetadata Sub-Interface Proposal (ISP Compliance) ──
//
//
// The monolithic ConnectorMetadata (14+ methods, 5 responsibility areas) violates
// Interface Segregation Principle. Read-only connectors must implement DDL methods
// they don't support. Proposed decomposition for future migration:
//
//   ConnectorSchemaProvider:     listSchemaNames, createSchema, dropSchema
//   ConnectorTableProvider:      listTables, getTableHandle, getTableMetadata, getColumnHandles
//   ConnectorDdlHandler:         createTable, dropTable, renameTable          (optional)
//   ConnectorStatisticsProvider: getTableStatistics                           (optional)
//   ConnectorPushdownHandler:    applyFilter, applyProjection                 (optional)
//
// Connector interface would expose:
//   getSchemaProvider():     ConnectorSchemaProvider       (required)
//   getTableProvider():      ConnectorTableProvider        (required)
//   getDdlHandler():         Optional<ConnectorDdlHandler>
//   getStatisticsProvider(): Optional<ConnectorStatisticsProvider>
//   getPushdownHandler():    Optional<ConnectorPushdownHandler>
//
// Migration: ConnectorMetadata remains as a convenience base class implementing
// all sub-interfaces with default UnsupportedOperationException for optional methods.
// New connectors can implement only the required sub-interfaces.

// ConnectorSplitManager - split manager
public interface ConnectorSplitManager {
    ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint);
}

// ConnectorPageSourceProvider - data reading
public interface ConnectorPageSourceProvider {
    ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter);
}

// ConnectorPageSinkProvider - data writing
public interface ConnectorPageSinkProvider {
    ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle);

    ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle);
}
```

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

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ClassLoader Hierarchy                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                    ┌─────────────────────────┐                               │
│                    │   Bootstrap ClassLoader │                               │
│                    │   (JDK Core Classes)    │                               │
│                    └───────────┬─────────────┘                               │
│                                │                                             │
│                    ┌───────────▼─────────────┐                               │
│                    │   Pangrova Core Loader    │                               │
│                    │ • pangrova-connector-api   │                               │
│                    │ • pangrova-common         │                               │
│                    │ • pangrova-protocol       │                               │
│                    │ • Shared Dependencies   │                               │
│                    └───────────┬─────────────┘                               │
│                                │                                             │
│          ┌─────────────────────┼─────────────────────┐                       │
│          │                     │                     │                       │
│  ┌───────▼───────┐    ┌───────▼───────┐    ┌───────▼───────┐                 │
│  │   Iceberg     │    │    Paimon     │    │ Hudi(planned)│                  │
│  │ ClassLoader   │    │  ClassLoader  │    │  ClassLoader  │                 │
│  │ • iceberg-*   │    │ • paimon-*    │    │ • planned deps│                 │
│  │ • parquet-*   │    │ • flink-*     │    │ • spark-*     │                 │
│  │ (isolated)    │    │ (isolated)    │    │ (future)      │                 │
│  └───────────────┘    └───────────────┘    └───────────────┘                 │
└──────────────────────────────────────────────────────────────────────────────┘
```

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

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                         Protocol Abstraction Goals                                │
├───────────────────────────────────────────────────────────────────────────────────┤
│  Core goal: the Substrait Plan must be decoupled from the concrete storage format │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐      │
│  │                    Storage-Agnostic Design                               │     │
│  │  Currently supported:          Future extensions:                        │     │
│  │  • Iceberg                     • Hudi                                    │     │
│  │  • Paimon                      • Delta Lake                              │     │
│  │  • Parquet/ORC (raw)           • Custom Formats                          │     │
│  │                                                                          │     │
│  │  All storage formats share the same Substrait extension protocol         │     │
│  │  The Native layer is agnostic to which lakehouse format is used          │     │
│  └──────────────────────────────────────────────────────────────────────────┘     │
│                                                                                   │
│  Design principles:                                                               │
│  1. Abstraction over concretion: the protocol layer defines only "what", not "how"│
│  2. Reserved extension points: format-specific metadata via the properties map    │
│  3. Version compatibility: maintain backward compatibility as the protocol evolves│
│  4. Minimal-information principle: Native layer needs only minimal execution info │
└───────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Protocol Layer Architecture

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                         Protocol Layer Architecture                               │
├───────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────┐      │
│  │  Layer 1: Substrait Core (standard protocol)                             │     │
│  │  • Standard Rel types: ReadRel, FilterRel, ProjectRel, AggregateRel, etc.  │   │
│  │  • Standard Expression: ScalarFunction, FieldReference, Literal, etc.      │   │
│  │  • Standard Type: i32, i64, fp64, string, struct, list, map, etc.          │   │
│  │  This layer fully follows the Substrait standard, with no modifications    │   │
│  └──────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                           │
│                                       ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐      │
│  │  Layer 2: Pangrova Extension (common extensions)                            │    │
│  │  • PangrovaReadExtension: common data-read extension                        │    │
│  │  • PangrovaWriteExtension: common data-write extension                      │    │
│  │  • PangrovaShuffleExtension: distributed Shuffle extension                  │    │
│  │  This layer defines Pangrova-specific, storage-agnostic extensions          │    │
│  └──────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                           │
│                                       ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐      │
│  │  Layer 3: Storage Adapter (storage adaptation)                           │     │
│  │  • IcebergAdapter: Iceberg-specific metadata mapping                      │    │
│  │  • PaimonAdapter: Paimon-specific metadata mapping                        │    │
│  │  • HudiAdapter: planned Hudi-specific metadata mapping                    │    │
│  │  Implemented in Java Connector; maps format info to common extensions      │   │
│  └──────────────────────────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Responsibility Boundary Between PangrovaReadExtension and DataSplit

Pangrova's read protocol is split into two layers:

| Layer | Proto Message | Definition Location | Responsibility | Consumer |
|---|---|---|---|---|
| Substrait extension layer | `PangrovaReadExtension` | This section (connectors.md) | Table-level read description within the Substrait Plan, used by the Planner for optimization | Calcite Planner → Substrait Converter |
| JNI transport layer | `DataSplit` | canonical v2 `task.proto` (single authority) | Snapshot-pinned generic execution input for one source range, executed directly by Native | Java SplitManager → canonical TaskRequest → Native task parser |

Conversion flow:
```
PangrovaReadExtension (table-level, Planner phase)
    │
    │  Java SplitManager: expand into multiple file-level Splits
    │  - Resolve snapshot/commit → obtain file list
    │  - Partition pruning → filter out unneeded files
    │  - Schema Evolution → compute column_mappings
    │  - Delete File association → populate delete_files
    ▼
repeated DataSplit (snapshot-pinned execution inputs)
    │
    │  Identity/range stay generic; connector_detail packs FileSplitDetail
    │  Serialized directly into canonical TaskRequest.splits
    │  Passed to Native via nativeCreateTaskExecutor(byte[])
    ▼
C++ task parser validates DataSplit and its exact FileSplitDetail type URL
```

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

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                 PangrovaReadExtension → SplitInfo Conversion Mapping                │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  Iceberg:                                                                         │
│  ─────────────────────────────────────────────────────────────────────────────    │
│  PangrovaReadExtension                    SplitInfo                                 │
│  ──────────────────                     ─────────                                 │
│  version_id (snapshot_id)          →    iceberg_info.snapshot_id                  │
│  read_mode = SNAPSHOT              →    iceberg_info.read_mode = SNAPSHOT         │
│  read_mode = TIME_TRAVEL           →    Java resolves timestamp → snapshot_id     │
│                                         iceberg_info.read_mode = TIME_TRAVEL      │
│  read_mode = INCREMENTAL           →    iceberg_info.read_mode = INCREMENTAL      │
│  from_version_id / to_version_id   →    iceberg_info.from/to_snapshot_id          │
│  (Java: expand incremental files)  →    one SplitInfo per file                    │
│  Schema Evolution                  →    file_schema + column_mappings             │
│  Delete Files                      →    iceberg_info.delete_files                 │
│                                                                                   │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  Paimon:                                                                          │
│  ─────────────────────────────────────────────────────────────────────────────    │
│  PangrovaReadExtension                    SplitInfo                                 │
│  ──────────────────                     ─────────                                 │
│  version_id (commit_id)            →    paimon_info.snapshot_id                   │
│  read_mode = SNAPSHOT              →    paimon_info.read_mode = APPEND            │
│                                         or PRIMARY_KEY (depends on table type)    │
│  read_mode = CHANGELOG             →    paimon_info.read_mode = CHANGELOG         │
│  read_mode = INCREMENTAL           →    paimon_info.read_mode = INCREMENTAL       │
│  from_version_id / to_version_id   →    paimon_info.from/to_snapshot_id           │
│  properties["merge.engine"]        →    paimon_info.merge_engine                  │
│  properties["changelog.producer"]  →    paimon_info.changelog_producer            │
│  Bucket info                       →    paimon_info.bucket_id                     │
│  LSM Level files                   →    paimon_info.lsm_levels                    │
│  Schema Evolution                  →    file_schema + column_mappings             │
│                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
```

### 3.5 Java SplitManager Conversion Interface

```java
/**
 * SplitConverter — converts an PangrovaReadExtension into a list of SplitInfo
 * Each Connector implements its own SplitConverter
 */
public interface SplitConverter {
    String getSourceType();

    /**
     * Expand a table-level read description into a list of file-level SplitInfo
     *
     * Responsibilities:
     * 1. Resolve version → obtain file list
     * 2. Partition pruning → filter out unneeded files
     * 3. Schema Evolution → compute column_mappings
     * 4. Delete File association → populate connector_info
     */
    List<SplitInfo> convert(PangrovaReadExtension extension, ConnectorSession session);
}

/**
 * Iceberg SplitConverter
 */
public class IcebergSplitConverter implements SplitConverter {

    @Override
    public String getSourceType() { return "iceberg"; }

    @Override
    public List<SplitInfo> convert(PangrovaReadExtension extension, ConnectorSession session) {
        long snapshotId = Long.parseLong(extension.getVersionId());
        Table table = catalog.loadTable(tableIdentifier);

        TableScan scan = table.newScan().useSnapshot(snapshotId);

        // Incremental Read
        if (extension.getReadMode() == ReadMode.READ_MODE_INCREMENTAL) {
            long fromSnapshot = Long.parseLong(extension.getFromVersionId());
            long toSnapshot = Long.parseLong(extension.getToVersionId());
            scan = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(fromSnapshot)
                .toSnapshot(toSnapshot);
        }

        List<SplitInfo> splits = new ArrayList<>();
        for (FileScanTask task : scan.planFiles()) {
            SplitInfo.Builder builder = SplitInfo.newBuilder()
                .setSplitId(generateSplitId())
                .setConnectorId("iceberg")
                .setFilePath(task.file().path().toString())
                .setStartOffset(task.start())
                .setLength(task.length())
                .setFileFormat(toFileFormat(task.file().format()));

            // Schema Evolution: compute file_schema → query_schema mapping
            if (schemaChanged(task.file().schemaId(), extension.getSchemaVersion())) {
                builder.setFileSchema(toArrowSchemaBytes(task.file().schema()));
                builder.addAllColumnMappings(computeColumnMappings(
                    task.file().schema(), extension.getArrowSchema()));
            }

            // Iceberg-specific info
            IcebergSplitInfo.Builder icebergBuilder = IcebergSplitInfo.newBuilder()
                .setSnapshotId(snapshotId)
                .setReadMode(toIcebergReadMode(extension.getReadMode()))
                .setDataSequenceNumber(task.file().dataSequenceNumber())
                .setSpecId(task.file().specId());

            // Delete Files
            for (DeleteFile deleteFile : task.deletes()) {
                icebergBuilder.addDeleteFiles(toDeleteFileInfo(deleteFile));
            }

            builder.setIcebergInfo(icebergBuilder.build());
            splits.add(builder.build());
        }
        return splits;
    }
}
```

---

## 4. Iceberg Connector Implementation

### 4.1 Module Structure

```text
pangrova-connector/pangrova-connector-iceberg/
├── src/main/java/org/pangrova/connector/iceberg/
│   ├── IcebergConnectorFactory.java     # ServiceLoader factory
│   ├── IcebergConnector.java            # connector entry point
│   ├── IcebergMetadata.java             # metadata implementation
│   ├── IcebergSplitManager.java         # split discovery
│   ├── handle/                          # catalog/table handles
│   ├── read/                            # page source and expression conversion
│   ├── write/                           # page sink and write support
│   └── type/                            # type mapping
├── src/main/resources/META-INF/services/
│   └── org.pangrova.connector.api.plugin.ConnectorFactory
└── pom.xml
```

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

## 5. Extension Point Design

### 5.1 Extension Point Overview

| Extension Point | Location | Responsibility | Extension Method |
|--------|------|------|----------|
| Storage Adapter (Java) | pangrova-connector-{format}/ | Maps storage-specific metadata to PangrovaReadExtension | Implement the StorageAdapter interface |

| Table Writer (Java) | pangrova-connector-{format}/ | Unified write interface (CDC + SQL writes share) | Implement TableWriter interface, see [streaming.md §2.6](./streaming.md) (authoritative) |



**TableWriter ↔ ConnectorPageSinkProvider Delegation Model:**

`TableWriter` ([10 §2.6](./streaming.md)) is the upper-layer unified entry point for all write operations (both CDC and SQL paths). Internally, `TableWriter` delegates to `ConnectorPageSinkProvider` (defined above in §1.2) for format-specific data file persistence. This avoids duplicate write logic and unifies transaction management.

```
CDC Consumer / SQL Write Task
        │
        ▼
    TableWriter (unified entry — 10 §2.6)
        │
        ├── SELF_MANAGED (CDC): TableWriter manages transaction internally
        │     └── ConnectorPageSinkProvider.createPageSink()
        │           └── ConnectorPageSink.appendPage() + commit()
        │
        └── COORDINATOR_MANAGED (SQL): Coordinator manages transaction
              └── ConnectorPageSinkProvider.createPageSink()
                    └── ConnectorPageSink.appendPage() (no commit)
                    └── Coordinator calls Connector.commit() atomically
```

| Responsibility | ConnectorPageSinkProvider (§1.2) | TableWriter ([10 §2.6](./streaming.md)) |
|----------------|----------------------------------|---------------------------------------------|
| Layer | Connector SPI factory — creates format-specific PageSink instances | Upper-layer unified write interface — entry point for CDC and SQL |
| Acquisition | `Connector.getPageSinkProvider()` | Constructed by write task or CDC consumer |
| Transaction | External — managed by `Connector.beginTransaction()` / `commit()` / `rollback()` | Via `WriteContext.transactionMode` (COORDINATOR_MANAGED or SELF_MANAGED) |
| Input format | `ConnectorPageSink.appendPage(Page)` — Pangrova internal Page format | `write(List<WriteRecord>)` for CDC, `writeBatch(VectorSchemaRoot)` for SQL |
| CDC support | Not directly — no CDC-specific API | Explicit CDC support with UPSERT mode |
| Relationship | **Internal implementation detail** of TableWriter | **Delegates to** ConnectorPageSinkProvider internally |
| File Format Reader (C++) | pangrova-worker-native/src/engine/ | Reads specific file formats (Parquet/ORC/Avro) | Implement the DataSourceFactory interface |
| Delete Handler (C++) | pangrova-worker-native/src/engine/delete/ | Handles different types of delete files | Implement the DeleteHandler interface |
| TableMaintenanceSpi (Java) | pangrova-connector-{format}/ | Async compaction, orphan cleanup, snapshot expiration | Implement TableMaintenanceSpi interface, see [streaming.md §2.9](./streaming.md) (authoritative) |
| Properties Handler | properties map in protocol | Passes format-specific metadata | Convention-based key prefix |

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

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Federation Query Architecture                            │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Cross-source Join: Iceberg table JOIN Paimon table                            │
│  • Unified query entry: users need not care about physical data location         │
│  • Smart pushdown: push computation to the source when possible                  │
│  • Performance optimization: reduce cross-source data transfer                   │
│                                                                                  │
│  Architecture overview:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                           Coordinator                                    │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │      │
│  │  │                    Federation Planner                            │   │     │
│  │  │                                                                  │   │     │
│  │  │  SQL: SELECT * FROM iceberg.orders o                            │   │      │
│  │  │       JOIN paimon.customers c ON o.cust_id = c.id               │   │      │
│  │  │                                                                  │   │     │
│  │  │  1. Parse & Analyze → identify cross-Catalog references         │   │      │
│  │  │  2. Optimize → cross-source Join order optimization             │   │      │
│  │  │  3. Pushdown → push Filter/Project to each source when possible │   │      │
│  │  │  4. Plan → generate the federated execution plan                │   │      │
│  │  └─────────────────────────────────────────────────────────────────┘   │      │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  Execution plan example:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                                                                          │    │
│  │  Stage 0: Scan iceberg.orders (Native, Velox)                           │     │
│  │           Filter: date > '2024-01-01' (pushed down)                     │     │
│  │                    ↓ Shuffle (cust_id)                                  │     │
│  │                                                                          │    │
│  │  Stage 1: Scan paimon.customers (Native, Velox)                         │     │
│  │           Filter: status = 'active' (pushed down)                       │     │
│  │                    ↓ Shuffle (id)                                       │     │
│  │                                                                          │    │
│  │  Stage 2: HashJoin(orders, customers) on cust_id = id                   │     │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Connector Type Classification

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Connector Type Classification                            │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Type 1: Native Connector (high performance)                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Sources: Iceberg, Paimon (current); Hudi, Delta Lake (planned)       │     │
│  │  • Execution layer: C++ Velox                                           │     │
│  │  • Features: vectorized reads, SIMD optimization, zero-copy             │     │
│  │  • Use cases: large-scale scans, complex computation                    │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  Type 2: API Connector (external services)                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Sources: Elasticsearch, MongoDB, Redis, Kafka                        │     │
│  │  • Execution layer: Java                                                │     │
│  │  • Features: external API calls, streaming reads                        │     │
│  │  • Use cases: real-time data, search, cache queries                     │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 Cross-Source Join Optimization

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      Cross-Source Join Optimization                              │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Optimization strategy:                                                          │
│                                                                                  │
│  1. Join order optimization                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Estimate each table's size from statistics                            │    │
│  │  • Prefer the smaller table as the Build Side                            │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  2. Predicate pushdown                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Push Filter down to each source                                       │    │
│  │  • Convert Join conditions to Filter (Semi-Join optimization)            │    │
│  │  • Native sources: generate a Substrait Filter Expression                │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  3. Data locality                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Process same-Catalog tables in the same Stage when possible           │    │
│  │  • Reduce cross-source Shuffle                                           │    │
│  │  • Broadcast small tables to avoid Shuffle                               │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  4. Dynamic filtering (cross-source)                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  • Build a Bloom Filter from the small table                             │    │
│  │  • Push it down to the large-table scan to filter early                  │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 6.4 Federation Query Execution Flow

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      Federation Query Execution Flow                             │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  SQL: SELECT o.*, c.name                                                         │
│       FROM iceberg.orders o                                                      │
│       JOIN paimon.customers c ON o.customer_id = c.id                            │
│       WHERE o.date > '2024-01-01' AND c.status = 'active'                        │
│                                                                                  │
│  Execution flow:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Step 1: Planning                                                        │    │
│  │  • Identify sources: iceberg (Native), paimon (Native)                  │     │
│  │  • Statistics: orders (10M rows), customers (100K)                      │     │
│  │  • Join order: customers (Build Side) → orders                          │     │
│  │  • Predicate pushdown: date > '2024-01-01' → iceberg                    │     │
│  │              status = 'active' → paimon                                 │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Step 2: Stage 0 - Scan paimon.customers (Native Velox)                 │     │
│  │  • Substrait Plan with Filter: status = 'active'                        │     │
│  │  • Result: 50K rows → Shuffle by id                                     │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Step 3: Stage 1 - Scan iceberg.orders (Native Velox)                   │     │
│  │  • Substrait Plan with Filter: date > '2024-01-01'                      │     │
│  │  • Result: 5M rows → Shuffle by customer_id                             │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Step 4: Stage 2 - Join                                                  │    │
│  │  • HashJoin(orders, customers) on customer_id = id                      │     │
│  │  • Execution engine: Native Velox (vectorized)                          │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

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
