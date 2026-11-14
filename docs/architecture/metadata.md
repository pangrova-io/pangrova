---
title: "Metadata Management"
sidebar_label: "Metadata Management"
---

# Pangrova — Metadata Management

Federation factory providers implement `org.pangrova.metadata.api.catalog.FederatedCatalogFactory` from `pangrova-metadata-api` and register that exact service name. The Server installs `pangrova-metadata-catalog`, which validates factory IDs at startup and converts the selected catalog to a metadata handle. No production Federation backend ships in the repository. Examples using external federation providers require separately installed implementations.


> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** Unified metadata ownership, catalog federation, tiered metadata caching, and schema evolution.

The generic object-dependency component separates contracts and values in
`metadata.api.relation`, ordering and policy orchestration in
`metadata.core.relation`, the in-memory provider in
`metadata.provider.memory.relation`, and PostgreSQL persistence in
`metadata.store.jdbc.object`. Both providers use iterative postorder traversal:
for an acyclic graph, every dependent precedes every reachable parent, including
shortcut edges and shared descendants. Revisited nodes are skipped; cyclic input
terminates without duplication but cannot have a topological order. Cascade
failure leaves the failed and unprocessed objects' edges intact.

Graph identity is the natural `(catalog, schema, name)` key, shared by both
providers and traversal through the internal `ObjectDependencyKey`. `ObjectRef`
remains a four-field value; its record equality still includes kind. Kinds are
edge descriptions rather than current-catalog authority: rewriting an existing
edge refreshes that edge's descriptions without rewriting other edges. Lookups,
removal, cycle deduplication and self-edge rejection ignore kind. Traversal returns
the first discovered description for each natural object. Both providers snapshot
and validate all parents before mutation, so a null entry cannot leave a partial
addition or replacement in memory. PostgreSQL write rollback and connection
ownership remain unchanged.

JDBC additions and replacements of the same child's complete parent set take a
PostgreSQL transaction advisory lock before any edge deletion. The lock key uses
the resolved graph table OID and the three name segments, so isolated schemas and
different children do not share a global graph lock. This prevents concurrent
DELETE/INSERT from failing a duplicate recording or combining two replacements.
It does not turn transitive graph reads or connector DDL into one serializable
transaction; those remain separate integration boundaries.

Dependency writes return an immutable complete parent snapshot acquired under the
same memory lock or inside the JDBC transaction before commit. The service uses
that result for both single-edge and batch notifications, so it neither mistakes
an input delta for the complete graph nor performs a second read after commit.
A failed JDBC snapshot read rolls back the write and emits no success callback.
Notifications remain synchronous and best-effort, not a durable outbox: concurrent
operations can supersede the snapshot or deliver callbacks in a different order.
Each successfully deleted cascade dependent is cleaned up and notified immediately;
a later failed drop does not erase earlier success notifications. The parent's
aggregate cascade list is context, not a request to emit those children again.
WARN reports prospective orphaning but preserves all edges until the connector's
successful drop is confirmed through `onObjectDropped`.

The optional lineage adapter carries the operation and datasets in both START
and COMPLETE events. Its per-object callback does not replay aggregate cascade
context. This describes explicitly registered component consumers; it does not
register the graph or adapter as a new SQL lifecycle authority.

This component is not the current SQL dependency authority. DROP TABLE goes
through Table Control and the connector, DROP VIEW delegates to the connector,
and materialization DROP uses its own tenant-scoped repository and authorization.
The generic graph's V9 key lacks tenant and incarnation dimensions. Its service
and optional lineage adapter are not registered by production bootstrap; query
lineage uses its durable outbox. Future integration requires an explicit identity,
authority and atomicity design. Historical V9 schema and checksums remain intact.

Generation stores now consume `GenerationTransactionAuthority` from
`persistence.jdbc.api.generation`. This is explicitly a caller-owned JDBC transaction port,
not a data-source container or an authorization snapshot. The real
`MetadataGenerationTransactionAuthority` adapter retains the current tenant and
table-policy checks and delegates model validation through Security's
`persistence.jdbc.api.security.model.CurrentModelAuthority` on that same connection. The process
root constructs AI Control's `persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority`
with the real `JdbcCurrentDirectRoleReader`, then explicitly passes it to
`new GenerationStores(modelAuthority)` and the managed-source and generated-write
consumers. The instance factory supplies that adapter to build, stage, exchange,
publication and generated-submission stores; it has no static current-authority
singleton, default model provider or mutable global injection point.
AI Control's catalog-export adapter receives `GenerationJobSubmissionTransactions`
explicitly from `ai.control.assembly.AiInfrastructureStores`, whose caller supplies
the real job store with its injected authority. AI Control no longer imports Metadata runtime or its test-jar;
cross-domain authority tests belong to Testing Integration. Its build insertion and catalog association remain on
one connection; an authority failure rolls back the whole export transaction.
The existing authority gate, execution-owner fence and job/descriptor lock order
remain unchanged. `GenerationExecutionTransactionFence` exposes the same owner
checks to consumers without requiring the concrete JDBC implementation.
`GenerationTransactionInputs` supplies dispatch, dependency and page reads on that
same connection. The real JDBC reader implements the port directly and retains
auto-commit reads, exact tenant/job scope and corrupt-input rejection. Authority
and publication consumers receive it explicitly; SQL-reader construction remains
in assembly. The common publication output validator still owns SQL in Metadata
and is a separate migration step.

The model provider observes the exact incarnation, configuration version,
deployment version and signature, and rechecks direct invocation roles before
validating a deferred receipt. It never opens, commits, rolls back or closes the
caller connection. Empty `requireCurrent` snapshots perform no model check;
empty `observe` still requires the transaction gate and current direct roles.
Both stage-graph and definition snapshots are validated. Managed-source writes
retain gate → execution fence → intent lock → authority ordering, and query-only
Coordinators keep plain-table write receipt capture active. Model-role cleanup
and SQL triggers are separate ownership concerns; this provider migration does
not change them or the optional Indexing activation path.

Immutable dispatch/dependency/page/split reads are owned by
`generation.store.postgresql.query.PostgresGenerationQueries`. They retain the caller's
connection and do not begin, commit or close its transaction. The canonical historical SQL sequence is now owned by the resource-only
`pangrova-schema-migrations` artifact, consumed by Metadata as a runtime dependency.
Its `db/migration` location, filenames, versions and bytes are unchanged;
`META-INF/pangrova/schema-migrations.tsv` records their original hashes.
Metadata retains bootstrap execution and cross-domain composition. `PostgresCurrentGenerationAuthority`
still has a private constructor and is resolved through current authority plus the
real backend execution anchor. It rejects a port result for a different tenant or
job before constructing that context.

Execution-anchor locking is owned by Query's
`query.api.coordination.jdbc.ExecutionTransactionFence`, implemented by
`query.store.jdbc.anchor.JdbcExecutionTransactionFence`. It takes the caller's
connection and the issued execution/owner/incarnation/token tuple, returns the
actual database anchor, and never creates, commits, rolls back or closes a
transaction. It checks expiry using a separate database-clock statement after
the anchor row lock. Absent/replaced/expired ownership is a typed rejection;
SQL failures and corrupt rows are not stale-owner results.

Generation owns exact durable route reads/writes and publication-owner lookup in
`generation.store.postgresql.execution`. Its injected fence verifies the route, delegates
anchor authority to Query, and exposes `GenerationExecutionOwnerLostException` to
consumers without leaking a Metadata implementation type. Metadata composition
wires this real fence into build, stage, exchange and publication stores.
Managed-source publication and File's COPY-source read admission also receive the
Query transaction port explicitly; neither calls a static anchor-store helper.
The former File-owned duplicate anchor-fence interface has been removed.
Server Coordinator composes a `PostgresTableControlWriteAuthority` with the same
identity-bound row and mask projection used for foreground policy reads. Query and
Generation receive the `TableControlWriteAuthority` port. The PostgreSQL workflow
checks current direct roles, grants and policies under the publication lock on the
caller's connection. The caller owns commit, rollback and close. Generation checks
the tenant, then the table, then the required model snapshots on that connection.

Generic job/page/stage state, revocation and exchange writers now live in
`generation.store.postgresql.job` and `generation.store.postgresql.exchange`. Execution context
resolution lives in `generation.store.postgresql.execution` and receives Security's
`persistence.jdbc.api.security.policy.CurrentDirectRoleReader`; the real JDBC provider reads current
direct roles on the same connection after current authorization and execution
fencing. It never owns the caller's transaction or substitutes IdP groups.

Publication candidate selection uses `persistence.jdbc.api.generation.GenerationPublicationCandidates`,
implemented by Metadata's shared-database projection. The SQL filters live owners
before sorting/limiting candidates and reads persisted publication ownership without
acquiring an anchor lock after the job lock. This read projection is not an execution
ownership authority. Generic storage failures use `GenerationStorageException` while
retaining the existing persistence error code, original cause and rollback failures.

Domain publication, generated-value submission and cross-domain composition
remain in Metadata pending their owner migration. Current model authority is already
implemented by AI Control through the Security-owned transaction port. Historical SQL resources have their
separate schema-migrations owner; their location does not make Metadata the writer
for every domain table.
Stage/exchange consumers now depend on `persistence.generation.api` contracts and their
immutable records rather than nested JDBC implementation types. The JDBC providers
retain fresh checks and the same transaction-scoped preparation callbacks. Their
private current-authority context implements a read-only view; that view is not an
authorization credential and is never accepted as a JDBC authorization input.

Generation storage contracts and PostgreSQL implementations now belong to
`pangrova-persistence-generation-api` and
`pangrova-persistence-generation-postgresql`; the five same-connection participant
interfaces belong to `pangrova-persistence-jdbc-api`. Generation Core owns canonical
encoding, graph construction and publication routing, with no JDBC dependency.
The private `PostgresCurrentGenerationAuthority` is issued only by fresh provider
resolution; its `CurrentGenerationAuthority` view is not an authorization input.
This split does not complete domain publication and process-assembly migration.
Metadata still composes real providers, and its `GenerationStores.builds()` return
type currently exposes `PostgresGenerationJobStore`. Indexing/AI publication
writers and the shared publication-output SQL validator remain separate work.
Dynamic Table/Materialized View lifecycle ownership stays in `pangrova-materialization`.

## 1. Design Background and Goals

### 1.1 Problem Domain

Pangrova's current metadata management is scattered across multiple components:
- `ConnectorMetadata` ([connectors.md §1.2](./connectors.md)) handles Connector-level metadata operations
- `FederatedCatalog` (this doc [§3.8](#38-federatedcatalog-spi--external-catalog-federation-interface)) handles external catalog federation

These components have evolved independently and lack a unified metadata management layer, which leads to:
1. Catalog registration/discovery/lifecycle-management logic is scattered throughout the Coordinator startup flow
2. Metadata cache policy lacks a unified integration point with security policy ([security.md](./security.md)) and multi-tenancy isolation ([multi-tenancy.md](./multi-tenancy.md))
3. Federated Catalog synchronization, version management, and Schema Evolution have no unified abstraction
4. Dynamic Catalog registration/unloading (runtime `CREATE CATALOG` / `DROP CATALOG`) is not supported

### 1.2 Design Goals

| Goal | Description |
|------|------|
| Unified entry | All metadata operations go through the single `MetadataService` entry point, replacing scattered Managers |
| Catalog lifecycle | Supports Catalog registration, discovery, health checks, and dynamic load/unload |
| Multi-source federation | Uniformly adapts HMS, AWS Glue, Iceberg REST Catalog, and built-in Catalogs |
| Metadata cache | Tiered cache (local + Redis) with invalidation coordinated with security policy |
| Tenant isolation | Catalog visibility is bound to tenants; metadata operations are governed by RBAC |
| Schema Evolution | Unified schema change management with consistent compatibility checks across Connectors |
| Observability | End-to-end tracing of metadata operations; metrics such as cache hit rate and sync latency are exposed |

### 1.3 Design References

We follow Apache Gravitino's Catalog Federation approach, with the following simplifications:
- Gravitino is a standalone metadata management service (a separate process); Pangrova embeds metadata management inside the Coordinator process
- Gravitino supports cross-engine (Spark/Trino/Flink) metadata sharing; Pangrova serves only its own query engine
- Gravitino has a standalone REST API and UI; Pangrova is managed via SQL DDL and internal APIs
- Retain Gravitino's core abstraction: the four-level Metalake → Catalog → Schema → Table namespace

### 1.4 Relationship to the Existing Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Metadata Management Integration Map                      │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  This document (07)                    Referenced architecture docs              │
│  ─────────────                  ──────────────                                   │
│  MetadataService         ←→    01: Coordinator HA, Shared State Layer            │
│  CatalogRegistry         ←→    04: Connector Design (ConnectorSPI, PluginManager)│
│  MetadataCache           ←→    09: Cache Layer (Redis)                           │
│  CatalogFederation       ←→    this doc §3.8 FederatedCatalog SPI (authoritative) │
│  TenantCatalogBinding    ←→    13: Multi-Tenancy (Namespace Isolation)           │
│  MetadataAccessControl   ←→    12: Security (RBAC, RLS, Column Masking)          │
│  SchemaEvolution         ←→    11: Data Governance §2 (Schema Evolution)         │
│  MetadataObservability   ←→    14: Observability (OpenTelemetry)                 │
│  MetadataReplication     ←→    15: Operations §4 (Disaster Recovery)             │
│  QueryMetadataBinding    ←→    query execution (`MetadataSchemaBuilder`)           │
│  CatalogConfig           ←→    16: Configuration Reference                       │
│  StreamingMetadata       ←→    10: Streaming (CDC Table Metadata)                │
│  CacheSecurityBinding    ←→    05: Query Optimization §3 (Result Cache)          │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Architecture

### 2.1 Overall Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Metadata Management Architecture                         │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                        MetadataService (unified entry)                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │       │
│  │  │ Catalog     │  │ Schema      │  │ Table       │  │ Column      │   │       │
│  │  │ Operations  │  │ Operations  │  │ Operations  │  │ Operations  │   │       │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │       │
│  └─────────┼────────────────┼────────────────┼────────────────┼───────────┘      │
│            └────────────────┴────────────────┴────────────────┘                  │
│                                    │                                             │
│            ┌───────────────────────┼───────────────────────┐                     │
│            ▼                       ▼                       ▼                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐               │
│  │ AccessControl   │    │ MetadataCache   │    │ EventDispatcher │               │
│  │ (RBAC + Tenant) │    │ (Local + Redis) │    │ (Change Events) │               │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘               │
│           │                      │                      │                        │
│  ┌────────┴──────────────────────┴──────────────────────┴────────────────┐       │
│  │                        CatalogRegistry                                 │      │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                    │       │
│  │  │ Catalog     │  │ Lifecycle   │  │ Health      │                    │       │
│  │  │ Store       │  │ Manager     │  │ Monitor     │                    │       │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                    │       │
│  └─────────┼────────────────┼────────────────┼───────────────────────────┘       │
│            └────────────────┴────────────────┘                                   │
│                             │                                                    │
│         ┌───────────────────┼───────────────────────┐                            │
│         ▼                   ▼                       ▼                            │
│  ┌─────────────┐    ┌─────────────┐        ┌─────────────┐                       │
│  │  Internal   │    │  Connector  │        │  Federation │                       │
│  │  Catalog    │    │  Catalog    │        │  Catalog    │                       │
│  │  (system)   │    │  (iceberg,  │        │  (hms, glue,│                       │
│  │             │    │   paimon)   │        │   rest)     │                       │
│  └─────────────┘    └──────┬──────┘        └──────┬──────┘                       │
│                            │                      │                              │
│                     ┌──────▼──────┐        ┌──────▼──────┐                       │
│                     │ Connector   │        │ External    │                       │
│                     │ SPI         │        │ Metastore   │                       │
│                     │ (02-design) │        │ (HMS/Glue)  │                       │
│                     └─────────────┘        └─────────────┘                       │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Namespace Model

Following Gravitino's four-level namespace, Pangrova simplifies to three levels (dropping Metalake, since Pangrova is single-engine):

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Namespace Model                                          │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Three-part SQL reference: catalog.schema.table                                  │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Level 0: CatalogRegistry (globally unique)                              │    │
│  │  ├── iceberg_prod    (ConnectorCatalog, type=iceberg)                    │    │
│  │  ├── iceberg_staging (ConnectorCatalog, type=iceberg)                    │    │
│  │  ├── paimon_rt       (ConnectorCatalog, type=paimon)                     │    │
│  │  ├── hms_federation  (FederationCatalog, type=hms)                       │    │
│  │  ├── glue_analytics  (FederationCatalog, type=glue)                      │    │
│  │  └── system          (InternalCatalog, built-in)                         │    │
│  │                                                                          │    │
│  │  Level 1: Schema (within a Catalog)                                      │    │
│  │  iceberg_prod                                                            │    │
│  │  ├── warehouse                                                           │    │
│  │  ├── raw                                                                 │    │
│  │  └── staging                                                             │    │
│  │                                                                          │    │
│  │  Level 2: Table (within a Schema)                                        │    │
│  │  iceberg_prod.warehouse                                                  │    │
│  │  ├── fact_orders                                                         │    │
│  │  ├── dim_customers                                                       │    │
│  │  └── dim_products                                                        │    │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
│  Comparison with Gravitino:                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │  Gravitino          │  Pangrova              │  Note                      │     │
│  │  ─────────────────────────────────────────────────────────────────────  │     │
│  │  Metalake           │  (not needed)        │  Pangrova is single-engine   │     │
│  │  Catalog            │  Catalog             │  same                      │     │
│  │  Schema             │  Schema              │  same                      │     │
│  │  Table              │  Table               │  same                      │     │
│  │  Fileset/Topic/Model│  (not needed)        │  Pangrova handles tables only│     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Catalog Type System

```java
/**
 * Catalog type enum
 */
public enum CatalogType {
    INTERNAL,       // built-in Catalog (system, information_schema)
    CONNECTOR,      // Connector-backed Catalog (iceberg, paimon)
    FEDERATION      // external Metastore federation (hms, glue, rest)
}

/**
 * Catalog descriptor — the unified Catalog metadata model
 *
 * Key distinction (see the domain-modularization ADR):
 * - Pangrova Catalog: three-part SQL table-name routing (catalog.schema.table)
 * - Iceberg Catalog: Iceberg table metadata management (org.apache.iceberg.catalog.Catalog)
 * The Pangrova Catalog is the routing layer; the Iceberg Catalog is the storage layer.
 */
public class CatalogDescriptor {
    private final String name;                    // Catalog name (SQL reference name)
    private final CatalogType type;               // Catalog type
    private final String provider;                // provider (iceberg, paimon, hms, glue)
    private final CatalogState state;             // lifecycle state
    private final Map<String, String> properties; // configuration properties
    private final String comment;                 // description
    private final AuditInfo auditInfo;            // audit info (creator, timestamp)

    // tenant binding (see multi-tenancy.md)
    private final Set<String> allowedTenants;     // list of visible tenants; empty = globally visible
}

/**
 * Catalog lifecycle state
 */
public enum CatalogState {
    CREATING,       // creating (loading Connector, validating connection)
    ACTIVE,         // running normally
    DEGRADED,       // degraded (external Metastore unreachable; using cache)
    SUSPENDED,      // suspended (manually paused by an administrator)
    DROPPING        // dropping (cleaning up resources)
}
```

---

## 3. Core Component Design

### 3.0 SessionContext → ConnectorSession Conversion



`SessionContext` is the engine-level session object carrying identity, tenant, and query
properties. `ConnectorSession` (defined in [connectors.md §1.2](./connectors.md))
is the Connector SPI session object passed to all `ConnectorMetadata` methods. The
`toConnectorSession()` method bridges these two layers.

```java
/**
 * ConnectorSession — SPI-level session passed to all Connector methods.
 * Authoritative field definition (see also connectors.md §1.2).
 */
public interface ConnectorSession {
    String getIdentity();              // authenticated user principal
    String getCatalogName();           // catalog this session is scoped to
    Optional<String> getSchema();      // current default schema (if set)
    String getTimeZone();              // session timezone (IANA format)
    Locale getLocale();                // session locale
    Map<String, String> getProperties(); // connector-specific session properties
}

/**
 * SessionContext.toConnectorSession() — field mapping:
 *
 *   SessionContext field        → ConnectorSession field
 *   ─────────────────────────────────────────────────────
 *   identity.getUser()         → getIdentity()
 *   currentCatalog             → getCatalogName()
 *   currentSchema              → getSchema()
 *   sessionProperties["timezone"] → getTimeZone()
 *   sessionProperties["locale"]   → getLocale()
 *   connectorProperties(catalog)  → getProperties()
 *
 * Fields NOT propagated (filtered for SPI isolation):
 *   - tenantId        (tenant context is enforced at MetadataService level, not leaked to connectors)
 *   - securityToken   (connectors use their own auth; engine token is not forwarded)
 *   - queryId         (not relevant to connector operations)
 *   - transactionId   (managed by ConnectorTransactionHandle, not session)
 */
public ConnectorSession toConnectorSession() {
    return new ConnectorSessionImpl(
        this.identity.getUser(),
        this.currentCatalog,
        Optional.ofNullable(this.currentSchema),
        this.getProperty("timezone", ZoneId.systemDefault().getId()),
        this.getProperty("locale", Locale.getDefault()),
        this.getConnectorProperties(this.currentCatalog));
}
```

### 3.1 MetadataService — Unified Entry Point



```java
/**
 * Metadata service — the unified entry point for all metadata operations
 *
 * Responsibilities:
 * 1. Permission checks (delegated to AccessControl; see security.md)
 * 2. Cache lookups (delegated to MetadataCache)
 * 3. Routing to the correct Catalog implementation
 * 4. Publishing metadata change events
 */
public class MetadataService {

    private final CatalogRegistry catalogRegistry;
    private final MetadataCache metadataCache;
    private final AccessControl accessControl;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionChecker schemaChecker;

    // ── Catalog Operations ──

    public void createCatalog(SessionContext session, CatalogDescriptor descriptor) {
        accessControl.checkCanCreateCatalog(session.getIdentity());
        catalogRegistry.register(descriptor);
        eventDispatcher.dispatch(new CatalogCreatedEvent(descriptor));
    }

    public void dropCatalog(SessionContext session, String catalogName, boolean cascade) {
        accessControl.checkCanDropCatalog(session.getIdentity(), catalogName);
        if (!cascade && !listSchemas(session, catalogName).isEmpty()) {
            throw new CatalogNotEmptyException(catalogName);
        }
        catalogRegistry.unregister(catalogName);
        metadataCache.invalidateCatalog(catalogName);
        eventDispatcher.dispatch(new CatalogDroppedEvent(catalogName));
    }

    public List<String> listCatalogs(SessionContext session) {
        Set<String> allCatalogs = catalogRegistry.listCatalogs();
        // tenant filtering (see multi-tenancy.md)
        return allCatalogs.stream()
            .filter(c -> accessControl.canAccessCatalog(session.getIdentity(), c))
            .filter(c -> tenantFilter(session, c))
            .toList();
    }

    // ── Schema Operations ──

    public List<String> listSchemas(SessionContext session, String catalogName) {
        accessControl.checkCanAccessCatalog(session.getIdentity(), catalogName);
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        return handle.getMetadata(session).listSchemaNames(session.toConnectorSession());
    }

    // ── Table Operations ──

    public ResolvedTable resolveTable(SessionContext session, TableRef tableRef) {
        accessControl.checkCanAccessCatalog(session.getIdentity(), tableRef.getCatalog());

        // 1. look up cache
        //
        Optional<TableMetadata> cached = metadataCache.getTable(
            tableRef.getCatalog(), tableRef.getSchema(), tableRef.getTable());
        if (cached.isPresent()) {
            // Wrap cached TableMetadata into ResolvedTable (security policies re-evaluated below)
        }

        // 2. load from Catalog
        CatalogHandle handle = catalogRegistry.getCatalog(tableRef.getCatalog());
        ConnectorMetadata metadata = handle.getMetadata(session);
        SchemaTableName schemaTable = new SchemaTableName(
            tableRef.getSchema(), tableRef.getTable());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(
            session.toConnectorSession(), schemaTable);
        if (tableHandle == null) {
            throw new TableNotFoundException(schemaTable);
        }

        // 3. permission check (table-level)
        accessControl.checkCanSelectFromTable(
            session.getIdentity(),
            new CatalogSchemaTableName(tableRef.getCatalog(), schemaTable));

        // 3.1 Security policy extraction (RLS + Column Masking)  [DEF-161]
        // Retrieve row-level security filters and column masks for this table.
        // The returned policies are attached to ResolvedTable and later injected
        // into the Calcite RelNode tree during the Security Rewrite phase
        // (see query-execution.md §2.1, security.md §3.2/§4.1).
        Optional<Expression> rowFilter = accessControl.getRowFilter(
            session.getIdentity(),
            new CatalogSchemaTableName(tableRef.getCatalog(), schemaTable));
        Map<String, Expression> columnMasks = accessControl.getColumnMasks(
            session.getIdentity(),
            new CatalogSchemaTableName(tableRef.getCatalog(), schemaTable));

        // 4. build ResolvedTable and cache it
        ResolvedTable resolved = new ResolvedTable(
            handle, tableHandle, metadata,
            metadata.getTableMetadata(session.toConnectorSession(), tableHandle),
            rowFilter,      // [DEF-161] RLS filter expression
            columnMasks);   // [DEF-161] column mask expressions
        metadataCache.putTable(tableRef.getCatalog(), tableRef.getSchema(),
            tableRef.getTable(), resolved.getTableMetadata());

        return resolved;
    }

    // ── Schema Evolution ──

    public void alterTable(SessionContext session, TableRef tableRef, TableChange change) {
        accessControl.checkCanAlterTable(session.getIdentity(), tableRef);

        CatalogHandle handle = catalogRegistry.getCatalog(tableRef.getCatalog());
        ConnectorMetadata metadata = handle.getMetadata(session);

        // Schema compatibility check (authoritative rules: §3.7 SchemaEvolutionValidator)
        if (change instanceof SchemaChange schemaChange) {
            schemaChecker.checkCompatibility(metadata, session, tableRef, schemaChange);
        }

        metadata.alterTable(session.toConnectorSession(), tableRef, change);
        //
        metadataCache.invalidateTable(tableRef.getCatalog(), tableRef.getSchema(), tableRef.getTable());
        eventDispatcher.dispatch(new TableAlteredEvent(tableRef, change));
    }
}
```

### 3.2 CatalogRegistry — Catalog Registration and Lifecycle

A drop compares the catalog name, incarnation and revision of the local handle in
the PostgreSQL delete. Only a matching row produces a catalog invalidation in the
same transaction. Registration compensates a handle creation failure only after
the database acknowledged the save, using that same compare-and-delete operation.
An unconfirmed save leaves durable state for reconciliation. Reconciliation
replaces a local handle when the stored incarnation or revision changes.




```java
/**
 * Catalog registry — manages registration, discovery, and lifecycle of all Catalogs
 *
 * Adds lifecycle management, health checks, and dynamic load/unload capabilities.
 *
 * Persistence: Catalog descriptors are persisted to PostgreSQL (see distributed.md §3.4)
 * Synchronization: Catalog changes are synchronized across Coordinators via ZooKeeper Watch
 */
public class CatalogRegistry {

    private final ConcurrentMap<String, CatalogHandle> catalogs = new ConcurrentHashMap<>();
    private final CatalogStore catalogStore;           // PostgreSQL persistence
    private final PluginManager pluginManager;         // Connector loading (connectors.md §2.3)
    private final CuratorFramework curator;            // ZK synchronization
    private final CatalogHealthMonitor healthMonitor;

    /**
     * Load all Catalogs at startup
     * Invocation point: PangrovaCoordinator.start() (distributed.md §1.2)
     *
     * ── Initialization ordering contract (relationship to Coordinator HA Leader Election) ──
     *
     * Invocation order of PangrovaCoordinator.start():
     *   1. haService.start()              — connect to ZK; register as a Leader candidate
     *   2. haService.registerAsCandidate() — participate in Leader Election
     *   3. metadataService.initialize()   — invokes this method (CatalogRegistry.initialize())
     *   4. workerManager.startDiscovery() — begin discovering Workers
     *   5. httpServer.start()             — begin accepting client requests
     *
     * Key constraints:
     * - CatalogRegistry.initialize() runs before Leader Election completes
     * - at this point the current Coordinator may or may not be the Leader
     * - therefore initialize() does not depend on Leader status; every Coordinator loads the full set of Catalogs
     * - the ZK Watch (watchCatalogChanges) remains valid after Leader Election,
     *   because the Watch uses the ZK session (not Leader-exclusive)
     *
     * Interaction with Leader Election:
     * - DDL operations (CREATE/DROP CATALOG) require Leader coordination
     *   (see distributed.md §3.5.1)
     * - but Catalog loading (initialize) is a read-only operation and does not require the Leader
     * - if ZK is unavailable during initialize(), startup fails (fail-fast)
     *
     * Sequence diagram:
     *   ZK Connect ──► Leader Election (async) ──► ...eventually elected...
     *        │
     *        └──► CatalogRegistry.initialize()  ← does not wait for the Election result
     *               ├── PostgreSQL: load descriptors  ← read-only, no Leader needed
     *               ├── loadCatalog() × N        ← local load, no Leader needed
     *               ├── watchCatalogChanges()    ← ZK Watch, not Leader-exclusive
     *               └── healthMonitor.start()
     */
    public void initialize() {
        // 0. Precondition: ZK session must be established (by haService.start())
        Preconditions.checkState(curator.getState() == CuratorFrameworkState.STARTED,
            "ZooKeeper session not established. CatalogRegistry.initialize() " +
            "must be called after haService.start()");

        // 1. load persisted Catalog descriptors from PostgreSQL
        List<CatalogDescriptor> descriptors = catalogStore.listAll();
        for (CatalogDescriptor desc : descriptors) {
            loadCatalog(desc);
        }

        // 2. register a ZK Watch to observe Catalog changes from other Coordinators
        watchCatalogChanges();

        // 3. start health checks
        healthMonitor.start();
    }

    /**
     * Register a new Catalog (dynamic runtime registration)
     */
    public void register(CatalogDescriptor descriptor) {
        // 1. persist to PostgreSQL
        catalogStore.save(descriptor);

        // 2. load the Catalog
        loadCatalog(descriptor);

        // 3. notify other Coordinators (ZK event)
        notifyCatalogChange(descriptor.getName(), ChangeType.CREATED);
    }

    /**
     * Unload a Catalog (dynamic runtime unload)
     */
    public void unregister(String catalogName) {
        CatalogHandle handle = catalogs.remove(catalogName);
        if (handle != null) {
            handle.shutdown();
        }
        catalogStore.delete(catalogName);
        notifyCatalogChange(catalogName, ChangeType.DROPPED);
    }

    /**
     * Load a single Catalog — dispatch by type
     */
    private void loadCatalog(CatalogDescriptor descriptor) {
        CatalogHandle handle = switch (descriptor.getType()) {
            case INTERNAL -> loadInternalCatalog(descriptor);
            case CONNECTOR -> loadConnectorCatalog(descriptor);
            case FEDERATION -> loadFederationCatalog(descriptor);
        };
        handle.setState(CatalogState.ACTIVE);
        catalogs.put(descriptor.getName(), handle);
    }

    /**
     * Load a Connector-backed Catalog
     * Reuses the ClassLoader isolation mechanism of PluginManager (connectors.md §2.3)
     */
    private CatalogHandle loadConnectorCatalog(CatalogDescriptor descriptor) {
        ConnectorFactory factory = pluginManager.getConnectorFactory(descriptor.getProvider());
        if (factory == null) {
            throw new CatalogProviderNotFoundException(descriptor.getProvider());
        }
        Connector connector = factory.create(
            descriptor.getName(), descriptor.getProperties(), createContext());
        return new ConnectorCatalogHandle(descriptor, connector);
    }

    /**
     * Load a Federation Catalog (external Metastore)
     * See §3.8 FederatedCatalog SPI in this document
     */
    private CatalogHandle loadFederationCatalog(CatalogDescriptor descriptor) {
        FederatedCatalogFactory adapterFactory = FederatedCatalogFactory.get(descriptor.getProvider());
        FederatedCatalog adapter = adapterFactory.create(descriptor.getProperties());
        return new FederationCatalogHandle(descriptor, adapter);
    }

    /**
     * Multi-Coordinator synchronization — ZooKeeper Watch
     * See distributed.md §3.4 (Shared State Layer)
     */
    private void watchCatalogChanges() {
        PathChildrenCache cache = new PathChildrenCache(
            curator, "/pangrova/catalogs", true);
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED, CHILD_UPDATED -> {
                    String catalogName = ZKPaths.getNodeFromPath(event.getData().getPath());
                    CatalogDescriptor desc = catalogStore.get(catalogName);
                    if (desc != null && !catalogs.containsKey(catalogName)) {
                        loadCatalog(desc);
                    }
                }
                case CHILD_REMOVED -> {
                    String catalogName = ZKPaths.getNodeFromPath(event.getData().getPath());
                    CatalogHandle handle = catalogs.remove(catalogName);
                    if (handle != null) handle.shutdown();
                }
            }
        });
        cache.start();
    }

    public CatalogHandle getCatalog(String name) {
        CatalogHandle handle = catalogs.get(name);
        if (handle == null) throw new CatalogNotFoundException(name);
        if (handle.getState() == CatalogState.SUSPENDED) {
            throw new CatalogSuspendedException(name);
        }
        return handle;
    }

    public Set<String> listCatalogs() {
        return Collections.unmodifiableSet(catalogs.keySet());
    }
}
```

### 3.3 CatalogHandle — Catalog Abstraction Handle

```java
/**
 * Catalog handle — the unified abstraction for Catalog operations
 *
 * Three implementations correspond to the three Catalog types:
 * - InternalCatalogHandle: system catalog (SHOW TABLES, system.runtime.*)
 * - ConnectorCatalogHandle: Connector-backed (iceberg, paimon)
 * - FederationCatalogHandle: external Metastore (hms, glue)
 */
public interface CatalogHandle {
    CatalogDescriptor getDescriptor();
    CatalogState getState();
    void setState(CatalogState state);

    ConnectorMetadata getMetadata(SessionContext session);
    ConnectorSplitManager getSplitManager();
    ConnectorPageSourceProvider getPageSourceProvider();
    default ConnectorPageSinkProvider getPageSinkProvider() {
        throw new UnsupportedOperationException("Write not supported");
    }

    /** health check */
    HealthStatus checkHealth();

    /** close and release resources */
    void shutdown();
}

/**
 * Connector-backed Catalog handle
 * Bridges to the Connector SPI (connectors.md §1.2)
 */
public class ConnectorCatalogHandle implements CatalogHandle {
    private final CatalogDescriptor descriptor;
    private final Connector connector;
    private volatile CatalogState state;

    @Override
    public ConnectorMetadata getMetadata(SessionContext session) {
        return connector.getMetadata(session.toConnectorSession());
    }

    @Override
    public HealthStatus checkHealth() {
        try {
            connector.getMetadata(SystemSession.INSTANCE).listSchemaNames(SystemSession.INSTANCE);
            return HealthStatus.HEALTHY;
        } catch (Exception e) {
            return HealthStatus.unhealthy(e.getMessage());
        }
    }
}

/**
 * Federation Catalog handle
 * Bridges to the FederatedCatalog SPI (this doc §3.8)
 * Adapts external Metastore metadata to the ConnectorMetadata interface
 */
public class FederationCatalogHandle implements CatalogHandle {
    private final CatalogDescriptor descriptor;
    private final FederatedCatalog adapter;
    private volatile CatalogState state;

    @Override
    public ConnectorMetadata getMetadata(SessionContext session) {
        return new FederationConnectorMetadata(adapter, session);
    }

    @Override
    public HealthStatus checkHealth() {
        try {
            adapter.listSchemas();
            return HealthStatus.HEALTHY;
        } catch (Exception e) {
            state = CatalogState.DEGRADED;
            return HealthStatus.unhealthy(e.getMessage());
        }
    }
}

/**
 * FederationConnectorMetadata — Bridges FederatedCatalog to ConnectorMetadata.
 *
 * Delegates read-only metadata operations to FederatedCatalog (§3.8) and throws
 * UnsupportedOperationException for DDL/write operations not supported through
 * the federation path.
 *
 * Resolves: IC-MD-005 — FederationConnectorMetadata class definition.
 *
 * Supported/Unsupported method mapping:
 *   Supported (delegated to FederatedCatalog):
 *     - listSchemaNames()      → adapter.listSchemas()
 *     - listTables(schema)     → adapter.listTables(schema)
 *     - getTableHandle()       → adapter.getTable() != null
 *     - getTableMetadata()     → adapter.getTable()
 *     - getTableStatistics()   → adapter.getTableStatistics()
 *     - getColumnHandles()     → derived from adapter.getTable().columns()
 *
 *   Unsupported (throws UnsupportedOperationException):
 *     - createSchema(), dropSchema()
 *     - createTable(), dropTable(), renameTable()
 *     - alterTable()
 *     - applyFilter(), applyProjection() (no pushdown for federation)
 */
public class FederationConnectorMetadata implements ConnectorMetadata {

    private final FederatedCatalog adapter;
    private final SessionContext session;

    public FederationConnectorMetadata(FederatedCatalog adapter, SessionContext session) {
        this.adapter = requireNonNull(adapter, "adapter is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return adapter.listSchemas();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
        List<String> schemas = schema.map(List::of).orElseGet(adapter::listSchemas);
        return schemas.stream()
            .flatMap(s -> adapter.listTables(s).stream()
                .map(t -> new SchemaTableName(s, t)))
            .toList();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        try {
            TableMetadata meta = adapter.getTable(tableName.getSchemaName(), tableName.getTableName());
            return new FederationTableHandle(tableName, meta);
        } catch (TableNotFoundException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        TableMetadata meta = handle.getMetadata();
        return MetadataModelConverter.toConnectorTableMetadata(meta);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        return handle.getMetadata().columns().stream()
            .collect(Collectors.toMap(
                ColumnMetadata::name,
                col -> new FederationColumnHandle(col.name(), col.type())));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        SchemaTableName name = handle.getTableName();
        return adapter.getTableStatistics(name.getSchemaName(), name.getTableName());
    }

    // ── Unsupported DDL/Write operations ──

    @Override
    public void createSchema(ConnectorSession s, String schema, Map<String, Object> props) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }

    @Override
    public void dropSchema(ConnectorSession s, String schema) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }

    @Override
    public void createTable(ConnectorSession s, ConnectorTableMetadata meta, boolean ignoreExisting) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }

    @Override
    public void dropTable(ConnectorSession s, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }

    @Override
    public void renameTable(ConnectorSession s, ConnectorTableHandle table, SchemaTableName newName) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }

    @Override
    public void alterTable(ConnectorSession s, ConnectorTableHandle table, List<SchemaChange> changes) {
        throw new UnsupportedOperationException("Federation catalogs are read-only");
    }
}
```

### 3.4 Catalog Health Checks

```java
/**
 * Catalog health monitoring
 *
 * Metrics are exposed to Prometheus (see observability.md)
 * The degraded state triggers alerts (see the degradation strategy in resilience.md)
 */
public class CatalogHealthMonitor {

    private final CatalogRegistry registry;
    private final ScheduledExecutorService scheduler;
    private final MetricsProvider metrics;

    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
    private static final int DEGRADED_THRESHOLD = 3; // consecutive failure count

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAll,
            CHECK_INTERVAL.toSeconds(), CHECK_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    private void checkAll() {
        for (CatalogHandle handle : registry.getAllHandles()) {
            HealthStatus status = handle.checkHealth();

            metrics.recordCatalogHealth(handle.getDescriptor().getName(), status);

            if (!status.isHealthy()) {
                int failCount = incrementFailCount(handle.getDescriptor().getName());
                if (failCount >= DEGRADED_THRESHOLD) {
                    handle.setState(CatalogState.DEGRADED);
                    metrics.recordCatalogDegraded(handle.getDescriptor().getName());
                }
            } else {
                resetFailCount(handle.getDescriptor().getName());
                if (handle.getState() == CatalogState.DEGRADED) {
                    handle.setState(CatalogState.ACTIVE);
                }
            }
        }
    }
}
```

#### 3.4.1 Error Propagation from External Catalogs



When a catalog enters `DEGRADED` state, the behavior for in-flight and new queries is:

| Scenario | Behavior | Rationale |
|----------|----------|-----------|
| `resolveTable()` on DEGRADED catalog | Target behavior: serve a bounded-staleness cache entry; otherwise surface a metadata/catalog backend error. | Avoid silently serving arbitrarily old metadata. |
| `listSchemas()` / `listTables()` on DEGRADED catalog | Same cache-or-fail strategy as `resolveTable()`. | Consistent behavior across all metadata operations. |
| DDL on DEGRADED catalog (`CREATE TABLE`, `ALTER TABLE`) | Fail with a metadata/catalog backend error. DDL requires authoritative state. | DDL against stale metadata risks schema corruption. |
| Connector internal error (non-DEGRADED) | Map to `PangrovaException` with `ErrorCategory.EXTERNAL_ERROR` (see [resilience.md](./resilience.md)). Propagate to query execution layer as query failure. | Connector errors are not retryable at metadata level. |
| External catalog timeout | Count toward DEGRADED threshold. If within threshold, retry once with halved timeout. If exceeded, transition to DEGRADED. | Graceful degradation with bounded retry. |

```java
// CatalogRegistry.getCatalog() — updated to handle DEGRADED state
public CatalogHandle getCatalog(String name) {
    CatalogHandle handle = catalogs.get(name);
    if (handle == null) throw new CatalogNotFoundException(name);
    if (handle.getState() == CatalogState.SUSPENDED) {
        throw new CatalogSuspendedException(name);
    }
    // DEGRADED catalogs remain accessible — MetadataService decides
    // whether to serve from cache or fail based on staleness policy.
    // See MetadataService.resolveTable() cache-or-fail logic.
    return handle;
}
```

MetadataCache staleness tolerance configuration:

| Property | Default | Description |
|----------|---------|-------------|
| `metadata.degraded.max-staleness` | `5m` | Maximum acceptable cache age when serving from DEGRADED catalog |
| `metadata.degraded.ddl-allowed` | `false` | Whether DDL is allowed on DEGRADED catalogs (always false in production) |
| `metadata.cache.async-refresh` | `true` | Non-blocking cache refresh on TTL expiry |

Error category mapping from connector exceptions to Pangrova error model
(see [resilience.md](./resilience.md) for full ErrorCategory definition):

| Connector Exception | Pangrova ErrorCategory | Retryable |
|---------------------|---------------------|-----------|
| `CatalogBackendException` (timeout) | `EXTERNAL_ERROR` | Yes (bounded) |
| `CatalogBackendException` (auth failure) | `PERMISSION_DENIED_ERROR` | No |
| `CatalogBackendException` (not found) | `USER_ERROR` | No |
| `ConnectorException` (internal) | `EXTERNAL_ERROR` | No |
| `UnsupportedOperationException` | `NOT_SUPPORTED_ERROR` | No |


### 3.5 MetadataCache — Tiered Metadata Cache

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                         Metadata Cache Architecture                                                              │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                  │
│  Difference from the data cache (caching.md):                                                             │
│  • The 08 cache is a data-plane cache (Row Group level, C++ layer, RAM/SSD)                                      │
│  • This section cache is a control-plane cache (metadata level, Java layer, Caffeine/Redis)                      │
│                                                                                                                  │
│  Two-tier cache:                                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────┐                                │
│  │                                                                              │                                │
│  │ ┌─────────────────────┐  Hit                                                 │                                │
│  │ │  L1: Caffeine       │ ────────────────────► Return Metadata                │                                │
│  │ │  (JVM-local)        │                                                      │                                │
│  │ │  TTL: 60s           │                                                      │                                │
│  │ └──────┬──────────────┘                                                      │                                │
│  │        │ Miss                                                                │                                │
│  │        ▼                                                                     │                                │
│  │ ┌─────────────────────┐  Hit                                                 │                                │
│  │ │  L2: Redis          │ ──► Fill L1 ────────► Return Metadata                │                                │
│  │ │  (cross-Coordinator │                                                      │                                │
│  │ │   shared)           │                                                      │                                │
│  │ │  TTL: 300s          │                                                      │                                │
│  │ └──────┬──────────────┘                                                      │                                │
│  │        │ Miss                                                                │                                │
│  │        ▼                                                                     │                                │
│  │ ┌─────────────────────┐                                                      │                                │
│  │ │  Metastore          │ ──► Fill L2 + L1 ───► Return Metadata                │                                │
│  │ │  (PostgreSQL)       │                                                      │                                │
│  │ └──────┬──────────────┘                                                      │                                │
│  │        │ Miss                                                                │                                │
│  │        ▼                                                                     │                                │
│  │ FederatedCatalog (external backend) ──► Fill Metastore + L2 + L1               │                                │
│  │                                                                              │                                │
│  └──────────────────────────────────────────────────────────────────────────────┘                                │
│                                                                                                                  │
│  Invalidation policy:                                                                                            │
│  • DDL operations: immediately invalidate L1 + L2 (broadcast via Redis Pub/Sub)                                  │
│  • Security-policy changes: invalidate the cache of affected tables (see security.md)                         │
│  • Tenant changes: invalidate the Catalog cache bound to the tenant (see multi-tenancy.md)                    │
│  • TTL expiry: refresh asynchronously without blocking reads                                                     │
│                                                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


```java
/**
 * Two-tier metadata cache
 *
 * L1: Caffeine (JVM-local, low latency, visible to a single Coordinator)
 * L2: Redis (shared across Coordinators; see distributed.md §3.4)
 *
 * Invalidation broadcast: Redis Pub/Sub ensures L1 is invalidated consistently across all Coordinators
 */
public class TieredMetadataCache implements MetadataCache {

    private final Cache<String, List<String>> schemaCache;     // L1 schema list
    private final Cache<String, TableMetadata> tableCache;     // L1 table metadata
    private final RedisTemplate<String, String> redis;         // L2
    private final ObjectMapper objectMapper;
    private final MetricsProvider metrics;

    private static final String CACHE_PREFIX = "pangrova:metadata:";
    private static final String INVALIDATION_CHANNEL = "pangrova:metadata:invalidation";

    public TieredMetadataCache(MetadataCacheConfig config,
                                RedisTemplate<String, String> redis,
                                MetricsProvider metrics) {
        this.schemaCache = Caffeine.newBuilder()
            .maximumSize(config.getMaxSize())
            .expireAfterWrite(config.getTtlSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getRefreshAfterWriteSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
        this.tableCache = Caffeine.newBuilder()
            .maximumSize(config.getMaxSize())
            .expireAfterWrite(config.getTtlSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getRefreshAfterWriteSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
        this.redis = redis;
        this.metrics = metrics;

        // subscribe to invalidation broadcasts
        subscribeInvalidation();
    }

    @Override
    public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
        String key = buildKey(catalog, schema, table);

        // L1 lookup
        TableMetadata cached = tableCache.getIfPresent(key);
        if (cached != null) {
            metrics.createCounter("metadata.cache.l1.hits", "L1 cache hits", "1").add(1);
            return Optional.of(cached);
        }

        // L2 lookup
        String json = redis.opsForValue().get(CACHE_PREFIX + key);
        if (json != null) {
            metrics.createCounter("metadata.cache.l2.hits", "L2 cache hits", "1").add(1);
            TableMetadata metadata = deserialize(json);
            tableCache.put(key, metadata);  // backfill L1
            return Optional.of(metadata);
        }

        metrics.createCounter("metadata.cache.misses", "Cache misses", "1").add(1);
        return Optional.empty();
    }

    @Override
    public void putTable(String catalog, String schema, String table, TableMetadata metadata) {
        String key = buildKey(catalog, schema, table);
        tableCache.put(key, metadata);                                    // L1
        redis.opsForValue().set(CACHE_PREFIX + key, serialize(metadata),
            Duration.ofSeconds(300));                                      // L2
    }

    @Override
    public void invalidateTable(String catalog, String schema, String table) {
        String key = buildKey(catalog, schema, table);
        tableCache.invalidate(key);                                       // L1 local
        redis.delete(CACHE_PREFIX + key);                                 // L2
        redis.convertAndSend(INVALIDATION_CHANNEL, key);                  // broadcast to other Coordinators
    }

    @Override
    public void invalidateCatalog(String catalog) {
        // clear all cache under this Catalog
        String pattern = catalog + ".*";
        tableCache.asMap().keySet().removeIf(k -> k.startsWith(catalog + "."));
        schemaCache.invalidate(catalog);

        // Redis: scan and delete
        Set<String> keys = redis.keys(CACHE_PREFIX + pattern);
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
        redis.convertAndSend(INVALIDATION_CHANNEL, "catalog:" + catalog);
    }

    /**
     * Subscribe to Redis invalidation broadcasts and apply invalidations from other Coordinators to the local L1
     */
    private void subscribeInvalidation() {
        redis.getConnectionFactory().getConnection().subscribe(
            (message, pattern) -> {
                String key = new String(message.getBody());
                if (key.startsWith("catalog:")) {
                    String catalog = key.substring("catalog:".length());
                    tableCache.asMap().keySet().removeIf(k -> k.startsWith(catalog + "."));
                    schemaCache.invalidate(catalog);
                } else {
                    tableCache.invalidate(key);
                }
            },
            INVALIDATION_CHANNEL.getBytes());
    }

    @Override
    public CacheStats getStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats l1Stats = tableCache.stats();
        return new CacheStats(
            l1Stats.hitCount(), l1Stats.missCount(),
            l1Stats.evictionCount(), tableCache.estimatedSize());
    }
}
```


### 3.6 MetadataSyncService — Metadata Synchronization

The sync service borrows four explicit persistence ports: `CatalogDeploymentRepository`,
`TableProjectionRepository`, `MetadataProjectionTransactions`, and `MetadataSyncRepository`.
Server assembles their PostgreSQL implementations on the selected metadata DataSource;
Core does not choose a backend or own the shared pool.

The periodic caller passes the descriptor from the actual federation handle together
with its adapter. Incremental reads return `MetadataChangeBatch`. Only an error-free
batch with a source-certified `completeThrough` can advance a checkpoint. A certified
empty batch may advance; an unbounded inventory, partial failure, or full enumeration
cannot manufacture a checkpoint from local wall time. Progress belongs to a catalog
incarnation and revision, not its reusable name.

The synchronization repository atomically checks current deployment and expected
checkpoint, records the run, and advances progress. An unknown COMMIT is confirmed
using the original run receipt; if confirmation fails, the same invocation does not
retry incremental sync or fall back to full sync. Partial results are failures for
`syncWithRetry`.

Each projection write carries the original handle's `CatalogDeploymentIdentity`.
`MetadataProjectionTransactions` locks and verifies that deployment, rejects foreign
catalog targets, then commits the projection, invalidation and immutable operation
receipt together. Unknown commit results are confirmed with that original operation ID;
unconfirmed results terminate that invocation without a new mutation or full-sync fallback.
The periodic Server scheduler still lacks a durable pending-operation barrier across
invocations/restarts; this recovery boundary remains unfinished and must retain the
original operation identity before another round can be authorized.
The complete external scan is **not** one transaction: source-version conditional writes,
concurrent same-deployment inventory, omitted deletions, and the scheduler's full
lease/lifecycle remain unfinished. The former leader-only and
wall-clock-watermark examples were not evidence that those guarantees existed.
Those guarantees are bounded by the current Catalog Persistence ports and tests.

### 3.7 SchemaEvolutionValidator — Schema Change Validation

```java
/**
 * Schema change compatibility validator
 *
 * This is the authoritative implementation of schema evolution validation rules.
 * data-governance.md §2.3 delegates to this component for compatibility checks.
 * This component acts as a pre-interceptor for MetadataService.alterTable().
 *
 * Safe changes (allowed):
 *   int → bigint, float → double,
 *   decimal(p1,s) → decimal(p2,s) where p2 > p1,
 *   varchar(n1) → varchar(n2) where n2 > n1,
 *   non-nullable → nullable
 *
 * Unsafe changes (rejected):
 *   bigint → int, double → float,
 *   nullable → non-nullable,
 *   any narrowing type change
 */
public class SchemaEvolutionValidator {

    private static final Map<String, Set<String>> SAFE_PROMOTIONS = Map.of(
        "INT", Set.of("BIGINT"),
        "FLOAT", Set.of("DOUBLE"),
        "DECIMAL", Set.of("DECIMAL"),  // precision increase
        "VARCHAR", Set.of("VARCHAR")   // length increase
    );

    public ValidationResult validate(TableMetadata current, List<SchemaChange> changes) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        Map<String, ColumnMetadata> columnMap = current.columns().stream()
            .collect(Collectors.toMap(ColumnMetadata::name, Function.identity()));

        for (SchemaChange change : changes) {
            validateSingleChange(change, columnMap, errors, warnings);
        }

        return new ValidationResult(errors.isEmpty(), errors, warnings);
    }

    private void validateSingleChange(SchemaChange change,
                                       Map<String, ColumnMetadata> columns,
                                       List<String> errors,
                                       List<String> warnings) {
        switch (change) {
            case SchemaChange.AddColumn add ->
                warnings.add("Adding column: " + add.name());

            case SchemaChange.DropColumn drop -> {
                if (!columns.containsKey(drop.name())) {
                    errors.add("Column not found: " + drop.name());
                } else {
                    warnings.add("Dropping column: " + drop.name());
                }
            }

            case SchemaChange.RenameColumn rename -> {
                if (!columns.containsKey(rename.oldName())) {
                    errors.add("Column not found: " + rename.oldName());
                }
                if (columns.containsKey(rename.newName())) {
                    errors.add("Column already exists: " + rename.newName());
                }
            }

            case SchemaChange.ChangeType typeChange -> {
                ColumnMetadata col = columns.get(typeChange.name());
                if (col == null) {
                    errors.add("Column not found: " + typeChange.name());
                } else if (!isPromotionSafe(col.type(), typeChange.newType())) {
                    errors.add("Incompatible type change: " + typeChange.name() +
                        " from " + col.type() + " to " + typeChange.newType());
                }
            }

            case SchemaChange.ChangeNullability nullChange -> {
                ColumnMetadata col = columns.get(nullChange.name());
                if (col == null) {
                    errors.add("Column not found: " + nullChange.name());
                } else if (col.nullable() && !nullChange.nullable()) {
                    errors.add("Cannot change nullable to non-nullable: " +
                        nullChange.name());
                }
            }
        }
    }

    private boolean isPromotionSafe(ColumnType from, ColumnType to) {
        if (from.equals(to)) return true;
        Set<String> allowed = SAFE_PROMOTIONS.get(from.baseType());
        if (allowed == null || !allowed.contains(to.baseType())) return false;
        // DECIMAL/VARCHAR: precision/length must increase
        if ("DECIMAL".equals(from.baseType())) {
            return to.precision() >= from.precision() && to.scale() == from.scale();
        }
        if ("VARCHAR".equals(from.baseType())) {
            return to.length() >= from.length();
        }
        return true;
    }
}
```


### 3.8 FederatedCatalog SPI — External Catalog Federation Interface

`FederatedCatalog` lives in `pangrova-metadata/pangrova-metadata-api` under
`org.pangrova.metadata.api.catalog`.
It exposes schema/table enumeration, table metadata and statistics, incremental
`readChangesSince(Instant)` returning `MetadataChangeBatch`, and adapter close.
The caching decorator forwards incremental batches without caching them.

A complete-through bound must represent every change (including deletions) in the
requested interval and exclude late commits into that interval. A successful SELECT,
an empty result, or the current database time is insufficient. An adapter without
that guarantee explicitly returns an absent bound. Pull-only providers reject a push subscription with an explicit unsupported-operation error. Federation
factory discovery is plugin-based; a configured provider name alone does not establish
an installed implementation or its capabilities.

## 4. Unified Metadata Model

### 4.1 TableMetadata — Cross-Catalog Common Model

```java
/**
 * Unified table metadata model
 *
 * All FederatedCatalogs convert external-backend metadata into this model.
 * This model is the basic unit for Metastore persistence and cache storage.
 *
 * Relationship to ConnectorTableMetadata (connectors.md §1.2):
 * - ConnectorTableMetadata is the Connector SPI internal model, oriented toward query execution
 * - TableMetadata is the metadata-management-layer model, oriented toward federated storage and caching
 * - MetadataService.resolveTable() handles conversion between the two
 */
public record TableMetadata(
    String catalog,
    String schema,
    String table,
    List<ColumnMetadata> columns,
    Map<String, String> partitionSpec,
    List<String> sortOrder,
    Map<String, String> properties,
    Instant createdAt,
    Instant updatedAt
) {
    public String qualifiedName() {
        return catalog + "." + schema + "." + table;
    }
}

public record ColumnMetadata(
    String name,
    ColumnType type,
    boolean nullable,
    String comment,
    Map<String, String> tags    // data-classification tags (see data-governance.md §5)
) {}

/**
 * Column type — supports parameterized types (DECIMAL(p,s), VARCHAR(n))
 */
public record ColumnType(
    String baseType,            // INT, BIGINT, FLOAT, DOUBLE, DECIMAL, VARCHAR, ...
    int precision,              // DECIMAL precision; 0 for other types
    int scale,                  // DECIMAL scale; 0 for other types
    int length                  // VARCHAR length; 0 for other types
) {
    public static ColumnType of(String baseType) {
        return new ColumnType(baseType, 0, 0, 0);
    }

    public static ColumnType decimal(int precision, int scale) {
        return new ColumnType("DECIMAL", precision, scale, 0);
    }

    public static ColumnType varchar(int length) {
        return new ColumnType("VARCHAR", 0, 0, length);
    }

    @Override
    public String toString() {
        if (precision > 0) return baseType + "(" + precision + "," + scale + ")";
        if (length > 0) return baseType + "(" + length + ")";
        return baseType;
    }
}
```


### 4.2 TableMetadata ↔ ConnectorTableMetadata Conversion Contract

`MetadataService.resolveTable()` handles conversion between `TableMetadata` (the metadata-management layer) and `ConnectorTableMetadata` ([connectors.md §1.2](./connectors.md)). The two models serve different purposes, their field sets are not fully aligned, and conversion adds or drops some information.

#### Field Mapping

| TableMetadata Field | ConnectorTableMetadata Field | Direction | Notes |
|---|---|---|---|
| `catalog` | (implicit, from CatalogHandle) | → only | ConnectorTableMetadata has no catalog name; it is provided by the CatalogHandle context |
| `schema` + `table` | `SchemaTableName` | ↔ | lossless in both directions |
| `columns` | `List<ColumnMetadata>` | ↔ | see the type-mapping table below |
| `columns[].tags` | — | → lost | **Information loss**: ConnectorTableMetadata has no data-classification tags; they are dropped during conversion |
| `partitionSpec` | `ConnectorTableLayout.partitioning` | → transform | Map format is converted to the Connector's PartitioningHandle |
| `sortOrder` | `ConnectorTableLayout.sortOrder` | → transform | String list is converted to a SortItem list |
| `properties` | `Map<String, Object> properties` | ↔ | value type is widened from String to Object |
| `createdAt` / `updatedAt` | — | → lost | **Information loss**: ConnectorTableMetadata has no audit timestamps |
| — | `ConnectorTableHandle` | ← only | **Information gain**: created by the Connector; includes runtime state such as snapshot ID and delete-file information |

#### Type Mapping (ColumnType ↔ Connector Type)

| ColumnType | Connector Type | Reversible |
|---|---|---|
| `INT` | `IntegerType` | ✓ |
| `BIGINT` | `BigIntType` | ✓ |
| `FLOAT` | `RealType` | ✓ |
| `DOUBLE` | `DoubleType` | ✓ |
| `DECIMAL(p,s)` | `DecimalType(p,s)` | ✓ |
| `VARCHAR(n)` | `VarcharType(n)` | ✓ |
| `BOOLEAN` | `BooleanType` | ✓ |
| `DATE` | `DateType` | ✓ |
| `TIMESTAMP` | `TimestampType` | ✓ (precision may differ) |
| Nested (STRUCT/ARRAY/MAP) | `RowType/ArrayType/MapType` | ✓ (recursive) |

#### Information Loss and Compensation

Information lost during conversion is compensated by the following mechanisms:

| Lost Field | Compensation | When Needed |
|---|---|---|
| `tags` (data classification) | classification authority must be read from Governance; table projections do not grant policy authority | when RLS/Column Masking needs tag information ([security.md §5](./security.md)) |
| `createdAt/updatedAt` | available as source metadata or local table-projection observations; actual query routing determines freshness | auditing and data-governance scenarios |
| `ConnectorTableHandle` | created by `ConnectorMetadata.getTableHandle()`; includes runtime information such as the Iceberg snapshot ID | required during query execution |

#### Conversion Implementation

```java
/**
 * TableMetadata ↔ ConnectorTableMetadata converter
 *
 * One-way conversion: resolveTable() obtains ConnectorTableMetadata from ConnectorMetadata,
 * then maps the selected source observation. Local projections do not replace current governance or source authority.
 *
 * No reverse conversion is provided: ConnectorTableMetadata → TableMetadata is handled by MetadataSyncService,
 * which builds TableMetadata directly from the external backend via the FederatedCatalog.
 */
public class MetadataModelConverter {

    public static ResolvedTable merge(
            ConnectorTableMetadata connectorMeta,
            ConnectorTableHandle tableHandle,
            Optional<TableMetadata> cachedMeta) {
        // ConnectorTableMetadata provides: schema, columns, properties, partitioning
        // TableMetadata adds: tags, createdAt, updatedAt
        // ConnectorTableHandle provides: snapshot ID, runtime state
        return new ResolvedTable(connectorMeta, tableHandle,
            cachedMeta.map(TableMetadata::columnTags).orElse(Map.of()));
    }
}
```

### 4.3 MetadataChange — Synchronization Change Model

```java
/**
 * Metadata change event — the input unit for MetadataSyncService
 */
public record MetadataChange(
    ChangeType type,
    String schema,
    String table,
    TableMetadata metadata,     // non-null for ADD/UPDATE, null for DELETE
    Instant changeTime
) {
    public enum ChangeType { ADD, UPDATE, DELETE }

    public String describe() {
        return type + " " + schema + "." + table;
    }
}

/**
 * Synchronization result
 */
public record SyncResult(
    String catalogName,
    Instant syncTime,
    int changesApplied,
    int errors,
    List<String> errorMessages,
    long durationMs
) {}
```

### 4.4 TableMetadata JSON Serialization

```java
/**
 * TableMetadata serializer
 *
 * Used for Metastore persistence (PostgreSQL JSONB columns) and Redis caching.
 * Guarantees round-trip: serialize(deserialize(json)) == json
 */
public class TableMetadataSerializer {

    private final ObjectMapper mapper;

    public TableMetadataSerializer() {
        this.mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String serialize(TableMetadata metadata) {
        try {
            return mapper.writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            throw new MetadataSerializationException("Serialize failed", e);
        }
    }

    public TableMetadata deserialize(String json) {
        try {
            return mapper.readValue(json, TableMetadata.class);
        } catch (JsonProcessingException e) {
            throw new MetadataSerializationException(
                "Deserialize failed: " + json.substring(0, Math.min(100, json.length())), e);
        }
    }
}
```

Serialization example:

```json
{
  "catalog": "iceberg_prod",
  "schema": "warehouse",
  "table": "orders",
  "columns": [
    {"name": "order_id", "type": {"baseType": "BIGINT"}, "nullable": false, "comment": "Primary key", "tags": {}},
    {"name": "customer_id", "type": {"baseType": "BIGINT"}, "nullable": false, "comment": null, "tags": {}},
    {"name": "amount", "type": {"baseType": "DECIMAL", "precision": 18, "scale": 2}, "nullable": false, "comment": "Order amount", "tags": {}},
    {"name": "email", "type": {"baseType": "VARCHAR", "length": 255}, "nullable": true, "comment": null, "tags": {"pii_type": "PII_EMAIL"}}
  ],
  "partitionSpec": {"date": "day(created_at)"},
  "sortOrder": ["created_at DESC"],
  "properties": {"format-version": "2", "write.format.default": "parquet"},
  "createdAt": "2024-01-01T00:00:00Z",
  "updatedAt": "2024-06-15T12:30:00Z"
}
```

### 4.7 Table Control Plane (Ownership and Admission)

At startup the coordinator selects the `postgresql` `TableStoreFactory` through
`ServiceLoader` and supplies the metadata data source. The factory creates the
ownership, statistics, governance binding and maintenance repositories together.
`CatalogRegistry` receives them before it opens catalogs. SQL maintenance and
scheduled maintenance share the same policy service and persisted run history.
Tests select a fresh memory store set or provide a PostgreSQL authority explicitly.

Catalog visibility is not ownership. The control plane adds the management state that a
connector cannot answer: who owns a table, which format identity it has, and whether
Pangrova may write, drop, or maintain it.

| Mode | Source | Persisted | Query | Governance | Maintenance | DML / DROP |
|---|---|---|---|---|---|---|
| `OBSERVED` | visible in the catalog, no control record | no | yes | observe only | no | no |
| `LINKED` | explicit adoption of an external table | yes | yes | yes | only policy-authorized non-destructive operations | no |
| `MANAGED` | created or written by Pangrova | yes | yes | yes | yes | yes |

- `table_control_registry` stores only `LINKED` / `MANAGED`; `OBSERVED` is computed on every
  resolve and never written. A connector remains the authority for schemas, snapshots, and
  statistics.
- A control record carries a stable `table_id`, the format UUID (`format_table_uuid`), the
  last observed `version_token`, and a `metadata_revision` that increments on every mutation.
  Drop/recreate under the same name therefore produces a different identity; stale handles
  and write intents cannot silently target the new table.
- Registration and adoption also capture the authoritative catalog incarnation and revision.
  Admission, controlled resolution (including cache hits), governance bindings, statistics,
  observations and rename/drop completion reject a deployment mismatch even when the physical
  table UUID has not changed. A stale local catalog handle is rejected against the durable
  descriptor before control authority can be used. Catalog replacement never rebinds an old
  control record automatically; a newly created table receives a new stable table ID.
  SQL DROP and the trusted Metadata API check the captured deployment before physical
  deletion, including shadow mode; completion retains its revision/deployment checks.
  This is not an atomic Catalog lease across connector I/O.
- Existing records with no catalog binding fail closed. The background observer may backfill
  both missing fields only for an explicitly legacy-write-allowlisted table whose live format
  UUID and location match the identity already recorded. It cannot overwrite an existing or
  partial deployment binding. Operators must confirm the intended current deployment before
  adding an entry to this migration allowlist. A LINKED record can be explicitly unlinked
  and adopted again to obtain a new stable ID without inheriting its previous bindings.
- `CREATE TABLE` registers `MANAGED` in the same DDL path. If the physical table was created
  but registration failed, the record is stored as `ORPHANED_MANAGED` and the DDL fails.
  `DROP TABLE` retires the captured live record (`DROPPED`). `ALTER TABLE ... RENAME`
  persists its original source and operation identity, then reserves both names through
  `table_control_name_reservation`. Registry changes, reservations and invalidations are
  atomic. Both names reject access and CREATE while RENAMING; successful completion
  releases the source name. An ambiguous connector exception retains the intent and
  reservations until the outcome can be proved.
- Ownership is checked before the connector is touched: `DdlExecutor` for DROP/ALTER,
  `DefaultMetadataService` for the REST/metadata path, and `QuerySchedulingEngine` for
  distributed DML. Connectors are never asked to infer writability from catalog type.
- Capabilities are a separate, fail-closed contract. A connector returns `TableCapabilities`
  from `getTableCapabilities`; the default (`TableCapabilities.UNSUPPORTED`) declares nothing,
  and undeclared always means unsupported. Ownership admits a write; capabilities then decide
  whether the format can execute it. `INSERT` requires `supportsWrite`, and
  `UPDATE`/`DELETE`/`MERGE` additionally require `supportsDelete`. A missing capability fails
  before connector allocation with `CONNECTOR_UNSUPPORTED_OPERATION`. Iceberg keeps
  `supportsEqualityDeleteRead=false` and fails closed on equality delete files until the read
  path is verified against a reference engine; maintenance operations and system tables are
  likewise declared per format rather than inferred.
- `ALTER TABLE ... ADOPT` creates a `LINKED` record from the connector's live identity and
  version. It fails closed when the connector cannot report a format UUID, when the table is
  already `MANAGED`, or when an existing record's identity no longer matches (drop/recreate).
  Re-adopting the same identity is idempotent. ADOPT never changes the physical table.
- `ALTER TABLE ... UNLINK` retires a `LINKED` record only; the physical table stays and returns
  to `OBSERVED`. It rejects `OBSERVED` (nothing to unlink) and `MANAGED` (use `DROP TABLE`).
- Resolution caches a controlled entry only while its `metadata_revision` and version token
  still match the registry; `OBSERVED` entries use the short-lived local cache.
- `TableVersionObserver` reconciles every live `LINKED`/`MANAGED` record with the connector on
  the leader's metadata-sync cadence. A new `version_token` advances `metadata_revision`,
  appends `table_control_version_observation`, writes `invalidation_log`, and emits
  `ExternalCommitObservedEvent`. An observation is not a Pangrova write commit and never enters
  the write commit journal.
- A changed `format_table_uuid` or location transitions the record to `QUARANTINED`; writes and
  maintenance stay blocked until an operator confirms the replacement. A table the catalog no
  longer resolves becomes `STALE`. An unreachable catalog aborts the pass instead: discovery
  failure is never treated as proof of deletion.
- `OBSERVED` tables are never persisted or observed in the background. Resolution captures the
  live version token and its cache entry expires after 30 seconds (or the configured resolution
  TTL if shorter), so a drop/recreate is picked up without writing control state.
- `SHOW TABLE CONTROLS [FROM|IN catalog[.schema[.table]]]` reads the control plane:
  - without a scope it lists persisted `LINKED`/`MANAGED` records across all catalogs;
  - with a catalog/schema scope it merges the live catalog listing in as `OBSERVED` rows;
  - with a single-table scope it additionally resolves that table's live `format_table_uuid`
    and `version_token`.
  Control records missing from the live listing stay visible so `STALE`/`QUARANTINED` state is
  never hidden. This statement never writes control state. Rows also carry the collected
  statistics summary (`stats_version`, `stats_row_count`, `stats_data_size`,
  `stats_computed_at`), with `-1` meaning none are available.
- `ANALYZE TABLE` collects connector statistics only for an `ACTIVE` `LINKED`/`MANAGED` record and
  persists them in `table_control_statistics`, keyed by `table_id` and bound to the version
  token observed during collection. The collection aborts if the version changes mid-run, if the
  connector reports no version token, or if it reports no usable statistics; nothing is stored
  under a fabricated zero. The planner (`CatalogStatisticsProvider`) prefers a persisted row only
  while its token still matches the control record and otherwise reads live connector
  statistics. `UNLINK` and `DROP TABLE` purge the row with the other table-scoped state, so a
  reused name cannot inherit old cardinalities.
- `TableControlPolicyBinder` stores governance artifacts (column tag, column mask, row filter,
  quality rule, lineage) in `table_control_governance_binding`, keyed by `table_id` and the
  artifact reference. Binding requires an `ACTIVE` `LINKED`/`MANAGED` record: an `OBSERVED`
  table has no stable identity, and a `QUARANTINED`/`STALE` record may describe a replaced
  table. `requireCurrentBinding` re-resolves the live record and rejects a binding
  left over from a dropped/recreated table even though the name matches; callers that
  resolve a binding by name use it instead of trusting the name. Query-time tag and
  mask enforcement is name-keyed, so a recreated table can only inherit over-masking
  (never a policy bypass), and every control-plane operation on it fails until the
  record is `ACTIVE` again.
  `UNLINK` and `DROP TABLE` purge every binding for the retired `table_id`, so a new table
  created under the same name starts without inherited governance.

Staged rollout (system properties):

| Property | Default | Effect |
|---|---|---|
| `pangrova.table-control.enabled` | `true` | Write control records in shadow mode |
| `pangrova.table-control.external-readonly` | `false` | Enforce rejection of DML/DROP on `OBSERVED`/`LINKED` |
| `pangrova.table-control.legacy-write-allowlist` | empty | Comma-separated `catalog.schema.table` names lazily registered as `MANAGED` during migration |

Enforcement must use `JdbcTableControlStore` (wired whenever the metadata datasource exists)
so every coordinator reaches the same ownership decision. The in-memory store is limited to
tests and metadata-less modes.


---

## 5. Metadata Persistence

### 5.1 Canonical PostgreSQL initialization

`pangrova-schema-migrations` is the sole owner of `db/migration` resources and the
checksum manifest. Server and PostgreSQL tests use that bundle to initialize an empty
database. Repositories never create parallel schemas. The new project does not need
historical data conversion, dual writes, or compatibility stores.

Catalog current/history rows, schema/table projections, and invalidation outbox remain
separate facts. V107 adds synchronization progress and immutable run receipts; V108 adds per-operation projection receipts. Runtime
code no longer reads or writes the historical `sync_history` table. The earlier
MySQL-style schema/full-text examples have been removed; the canonical SQL resources
are the executable PostgreSQL specification.

### 5.2 Independent domain ports and PostgreSQL implementations

| Capability | Contract in Catalog Persistence API | PostgreSQL implementation |
|---|---|---|
| Current deployment and exact revision history | `CatalogDeploymentRepository` | `PostgresCatalogDeploymentRepository` |
| Read rebuildable table projections | `TableProjectionRepository` | `PostgresTableProjectionRepository` |
| Read observed schema names | `SchemaProjectionRepository` | `PostgresSchemaProjectionRepository` |
| Deployment-fenced projection + invalidation + receipt | `MetadataProjectionTransactions` | `PostgresMetadataProjectionTransactions` |
| Source progress, diagnostics and completion receipts | `MetadataSyncRepository` | `PostgresMetadataSyncRepository` |

投影事务契约由 `pangrova-persistence-workflow-api` 拥有，PostgreSQL 协调者位于
`pangrova-persistence-workflow-postgresql`；Catalog Persistence 保留部署、投影读取和同步端口。

The former composite MetadataStore interface and PostgresMetadataStore implementation have
been removed. Deployment-only consumers do not implement or borrow projection methods.
Server selects providers explicitly; each provider borrows the same externally owned
DataSource and closes only the connections it acquires.

Deployment current/history publication and its invalidation are atomic. Each table
mutation and its invalidation are atomic, including its operation receipt and current deployment check; schema observation batches use the same atomic port. A schema list is an observed upsert batch, not a complete snapshot or a
request to delete omitted schemas. Failed/unknown persistence outcomes must not become
empty results or successful local-cache notifications.

Provider-internal `PostgresCatalogPersistenceSupport` shares transaction handling.
Its package is excluded from consumer exports. RuntimeException/Error during rollback
is attached to the original failure, and shared resources are not closed by repositories.
This helper is not a domain SQL callback API. The projection workflow has its own typed
transaction coordinator; package-private SQL participants borrow its connection and
never commit or close it. Table/schema repositories are read-only. Receipt JSON stays
inside the PostgreSQL Provider and has an explicit format version. A replay confirms
past execution, including after deployment replacement; it never grants current authority.
Schema observations do not reconcile omitted schemas. PostgreSQL timestamp-column
precision remains unchanged; operation JSON retains request identity for exact replay.
The schema read port currently has component tests but no production consumer; it is
not evidence that a new end-user schema-list endpoint has been delivered.

The existing table-projection substring search primitive is implemented but currently
has no production caller. It is not a full-text search feature or an advertised SQL
entry point. Inventory search requirements and its eventual endpoint/retirement remain
an explicit capability-review item.

### 5.3 Catalog deployment revision lifecycle

Canonical write tasks bind a catalog name, incarnation ID, revision, provider and
fingerprint. The authenticated deployment endpoint reads the immutable
`catalog_descriptor_revisions` snapshot for that exact identity; it never substitutes
the current `catalog_definitions` row. Current-row compare-and-swap and historical
insert commit in one metadata PostgreSQL transaction. V35 backfills the current row of
every existing catalog; revisions overwritten before V35 cannot be reconstructed and
exact requests for them fail closed.

Catalog `DROP` deletes only the current row. `DROP`/re-`CREATE` gets a new incarnation,
so old and new snapshots remain distinguishable. Historical rows are not deleted
automatically; this is intentional capacity debt. Operators should monitor total
historical row count, `pg_total_relation_size(catalog_descriptor_revisions)`, growth
per catalog/incarnation and oldest `recorded_at`. Manual deletion is unsafe until a
durable revision lease/GC protocol exists:

1. Reserve the exact revision in the same transaction that commits a dispatchable
   `TaskRequest`/outbox record; never transmit before that commit.
2. Renew the reservation under the live execution anchor and exact owner fence;
   retain references needed by write-commit reconciliation after a task reply.
3. Mark the reservation terminal only after replay and write-commit paths are
   impossible; unknown owner, deadline or journal outcome retains the revision.
4. GC may delete a non-current revision only when no live or recoverable reference
   exists and the safety horizon has elapsed; a lease/GC race must preserve the
   snapshot or reject dispatch before it leaves the Coordinator.

A missing snapshot remains a hard error; never return a newer revision or use
provider-level defaults as a compatibility path.

The shared `X-Internal-Auth` secret is not tenant-scoped and the endpoint does not
itself require TLS. Any Worker holding that secret can request another tenant's
descriptor, and descriptor properties can contain connector secrets. Workload mTLS,
exact task/tenant/descriptor authorization, and secret references resolved at the
Worker remain required security work.

## 6. SQL DDL Support

### 6.1 Catalog DDL

```sql
-- register a Connector-backed Catalog
CREATE CATALOG iceberg_prod
USING iceberg
WITH (
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://data-lake/warehouse',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = '${S3_ACCESS_KEY}',
    's3.secret-access-key' = '${S3_SECRET_KEY}'
);

-- register a Federation Catalog (external Metastore)
CREATE CATALOG hive_federation
USING hms
WITH (
    'uri' = 'thrift://hive-metastore:9083'
);

CREATE CATALOG glue_analytics
USING glue
WITH (
    'region' = 'us-east-1',
    'catalog-id' = '123456789012'
);

-- list registered Catalogs
SHOW CATALOGS;
-- output:
-- | name            | type       | provider | state  | last_sync_time       |
-- |-----------------|------------|----------|--------|----------------------|
-- | system          | INTERNAL   | built-in | ACTIVE | -                    |
-- | iceberg_prod    | CONNECTOR  | iceberg  | ACTIVE | 2024-06-15T12:30:00Z |
-- | hive_federation | FEDERATION | hms      | ACTIVE | 2024-06-15T12:29:45Z |
-- | glue_analytics  | FEDERATION | glue     | DEGRADED| 2024-06-15T12:25:00Z |

-- show Catalog details
DESCRIBE CATALOG iceberg_prod;

-- publish revised provider properties; tasks bound to an older revision retain its identity
ALTER CATALOG iceberg_prod SET PROPERTIES ('uri' = 'https://catalog.example.com/api');

-- unload a Catalog
DROP CATALOG hive_federation;

-- force a full sync
CALL system.sync_catalog('hive_federation', 'FULL');

-- view synchronization history
SELECT * FROM system.metadata.sync_history
WHERE catalog_name = 'hive_federation'
ORDER BY sync_time DESC LIMIT 10;
```


### 6.2 Metadata Queries (System Tables)

```sql
-- search tables across Catalogs
SELECT * FROM system.metadata.tables
WHERE table_name LIKE '%orders%';

-- view a table's column information
SELECT * FROM system.metadata.columns
WHERE catalog_name = 'iceberg_prod'
  AND schema_name = 'warehouse'
  AND table_name = 'fact_orders';

-- search by tag (integrates with data classification in data-governance.md §5)
SELECT * FROM system.metadata.columns
WHERE tags['pii_type'] IS NOT NULL;

-- view Catalog health status
SELECT * FROM system.metadata.catalog_health;
-- output:
-- | catalog_name   | state    | last_check_time      | consecutive_failures |
-- |----------------|----------|----------------------|----------------------|
-- | iceberg_prod   | ACTIVE   | 2024-06-15T12:30:00Z | 0                    |
-- | glue_analytics | DEGRADED | 2024-06-15T12:30:00Z | 5                    |

-- view cache statistics
SELECT * FROM system.metadata.cache_stats;
-- output:
-- | level | hits   | misses | evictions | hit_rate | size  |
-- |-------|--------|--------|-----------|----------|-------|
-- | L1    | 125000 | 3200   | 800       | 97.5%    | 8500  |
-- | L2    | 2800   | 400    | 200       | 87.5%    | 9200  |
```

### 6.3 Schema Evolution DDL

```sql
-- add a column
ALTER TABLE iceberg_prod.warehouse.orders
ADD COLUMN shipping_address VARCHAR(500) AFTER customer_id;

-- type promotion (safe change)
ALTER TABLE iceberg_prod.warehouse.orders
ALTER COLUMN amount TYPE DECIMAL(20, 4);

-- unsafe change — rejected by SchemaEvolutionValidator
ALTER TABLE iceberg_prod.warehouse.orders
ALTER COLUMN amount TYPE INT;
-- ERROR: Incompatible type change: amount from DECIMAL(18,2) to INT

-- change nullability (only non-nullable → nullable is allowed)
ALTER TABLE iceberg_prod.warehouse.orders
ALTER COLUMN customer_id SET NULL;

-- view schema change history
SELECT * FROM iceberg_prod.warehouse.orders$schema_history;
```


---

## 7. Observability Integration

### 7.1 Metadata Metrics

All metrics are exposed to Prometheus via the MetricsProvider SPI (see [observability.md §2.1](./observability.md)).

| Metric | Type | Description |
|--------|------|------|
| `pangrova.metadata.cache.l1.hits` | Counter | L1 (Caffeine) cache hits |
| `pangrova.metadata.cache.l1.misses` | Counter | L1 cache misses |
| `pangrova.metadata.cache.l2.hits` | Counter | L2 (Redis) cache hits |
| `pangrova.metadata.cache.l2.misses` | Counter | L2 cache misses |
| `pangrova.metadata.cache.evictions` | Counter | cache evictions |
| `pangrova.metadata.cache.size` | Gauge | current number of cache entries |
| `pangrova.metadata.catalog.health` | Gauge | Catalog health status (1=ACTIVE, 0.5=DEGRADED, 0=DOWN) |
| `pangrova.metadata.catalog.count` | Gauge | total number of registered Catalogs |
| `pangrova.metadata.sync.duration_ms` | Histogram | distribution of sync durations |
| `pangrova.metadata.sync.changes` | Counter | number of synced changes (by catalog label) |
| `pangrova.metadata.sync.errors` | Counter | number of sync errors (by catalog label) |
| `pangrova.metadata.resolve.latency_ms` | Histogram | resolveTable latency distribution |
| `pangrova.metadata.schema.validation.rejected` | Counter | number of rejected schema changes |


### 7.2 Metadata Operation Tracing

The following decorator is a proposed instrumentation pattern. The current source
tree does not contain this class.

```java
/**
 * Metadata operation tracing
 *
 * Each MetadataService method creates a Span,
 * associated with the query-level Trace (see observability.md §4.1).
 */
public class TracedMetadataService implements MetadataService {

    private final MetadataService delegate;
    private final TracingProvider tracing;

    @Override
    public ResolvedTable resolveTable(SessionContext session, TableRef tableRef) {
        Span span = tracing.startSpan("metadata.resolveTable", SpanKind.INTERNAL);
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("catalog", tableRef.getCatalog());
            span.setAttribute("schema", tableRef.getSchema());
            span.setAttribute("table", tableRef.getTable());

            ResolvedTable result = delegate.resolveTable(session, tableRef);

            span.setAttribute("cache.hit", result.isFromCache());
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
```

### 7.3 Grafana Dashboard Recommendations

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    Metadata Management Dashboard                                 │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Row 1: Catalog Overview                                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐                │
│  │ Catalog Count    │  │ Active / Degraded│  │ Sync Lag (max)   │                │
│  │     [Stat]       │  │    [Stat]        │  │    [Stat]        │                │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘                │
│                                                                                  │
│  Row 2: Cache Performance                                                        │
│  ┌──────────────────────────────────┐  ┌──────────────────────────────────┐      │
│  │ Cache Hit Rate (L1 vs L2)       │  │ Cache Size Over Time             │       │
│  │         [Time Series]           │  │         [Time Series]            │       │
│  └──────────────────────────────────┘  └──────────────────────────────────┘      │
│                                                                                  │
│  Row 3: Sync Health                                                              │
│  ┌──────────────────────────────────┐  ┌──────────────────────────────────┐      │
│  │ Sync Duration by Catalog        │  │ Sync Errors Over Time            │       │
│  │         [Heatmap]               │  │         [Time Series]            │       │
│  └──────────────────────────────────┘  └──────────────────────────────────┘      │
│                                                                                  │
│  Row 4: Resolve Latency                                                          │
│  ┌──────────────────────────────────┐  ┌──────────────────────────────────┐      │
│  │ resolveTable P50/P95/P99        │  │ Schema Validation Rejections     │       │
│  │         [Histogram]             │  │         [Bar Chart]              │       │
│  └──────────────────────────────────┘  └──────────────────────────────────┘      │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```


---

## 8. Configuration Reference

Configuration examples use YAML. Only keys listed in the
[configuration key reference](../configuration/reference.md) are part of the
verified typed runtime surface.

```yaml
# metadata-config.yaml
metadata:
  enabled: true
  jdbc-url: jdbc:postgresql://localhost:5432/pangrova_meta
  jdbc-username: ${METASTORE_USER}
  jdbc-password: ${METASTORE_PASSWORD}

  # ── Metadata cache ──
  cache:
    max-size: 10000                                # max number of L1 entries
    ttl-seconds: 300                               # L2 (Redis) TTL
    l1-ttl-seconds: 60                             # L1 (Caffeine) TTL
    refresh-after-write-seconds: 60                # async refresh interval

  # ── Metadata sync ──
  sync:
    enabled: true
    interval-seconds: 60                           # incremental sync interval
    retry-max-attempts: 3
    retry-backoff-ms: 1000                         # exponential backoff base
    full-sync-on-register: true                    # automatically full-sync when registering a new Catalog

  # ── Catalog health checks ──
  health:
    check-interval-seconds: 30
    degraded-threshold: 3                          # consecutive failure count triggers DEGRADED

  # ── Schema Evolution ──
  schema-evolution:
    compatibility-check: true                      # enable compatibility checks
    allow-incompatible-changes: false              # whether to allow incompatible changes (forbidden in production)

  # ── Federation Catalog registration (loaded at startup) ──
  catalogs:
    iceberg_prod:
      type: connector
      provider: iceberg
      properties:
        uri: http://iceberg-rest:8181
        warehouse: s3://data-lake/warehouse
    hive_federation:
      type: federation
      provider: hms
      properties:
        uri: thrift://hive-metastore:9083
    glue_analytics:
      type: federation
      provider: glue
      properties:
        region: us-east-1
        catalog-id: '123456789012'
```


---

## 9. Event-Driven Architecture

### 9.1 MetadataEventDispatcher

Metadata changes are broadcast over an event bus, decoupling downstream consumers.

```java
/**
 * Metadata event dispatcher
 *
 * Consumers:
 * - MetadataCache: invalidate the cache
 * - QueryResultCache: invalidate associated query results (see query-optimization.md §3.3)
 * - LineageEventEmitter: record lineage (see data-governance.md §3)
 * - SecurityPolicyRefresher: refresh security policy (see security.md)
 * - StreamingMetadataTracker: update streaming-table metadata (see streaming.md)
 */
public class MetadataEventDispatcher {

    private final List<MetadataEventListener> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService asyncExecutor;

    public void addListener(MetadataEventListener listener) {
        listeners.add(listener);
    }

    public void dispatch(MetadataEvent event) {
        for (MetadataEventListener listener : listeners) {
            asyncExecutor.submit(() -> {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    // event consumption failures do not affect the main flow
                    log.warn("Event listener failed: {}", listener.getClass().getSimpleName(), e);
                }
            });
        }
    }
}

/**
 * Metadata event types
 */
public sealed interface MetadataEvent {
    record CatalogCreatedEvent(CatalogDescriptor descriptor) implements MetadataEvent {}
    record CatalogDroppedEvent(String catalogName) implements MetadataEvent {}
    record CatalogStateChangedEvent(String catalogName, CatalogState oldState, CatalogState newState) implements MetadataEvent {}
    record TableCreatedEvent(TableRef tableRef, TableMetadata metadata) implements MetadataEvent {}
    record TableDroppedEvent(TableRef tableRef) implements MetadataEvent {}
    record TableAlteredEvent(TableRef tableRef, List<SchemaChange> changes) implements MetadataEvent {}
    record MetadataSyncedEvent(String catalogName, MetadataChange change) implements MetadataEvent {}
}

public interface MetadataEventListener {
    void onEvent(MetadataEvent event);
}
```


---

## 10. Error Handling

### 10.1 Exception Hierarchy

```java
/**
 * Base class for metadata exceptions
 */
public class MetadataException extends RuntimeException {
    public MetadataException(String message) { super(message); }
    public MetadataException(String message, Throwable cause) { super(message, cause); }
}

// Catalog-related
public class CatalogNotFoundException extends MetadataException { ... }
public class CatalogAlreadyExistsException extends MetadataException { ... }
public class CatalogSuspendedException extends MetadataException { ... }
public class CatalogProviderNotFoundException extends MetadataException { ... }
public class CatalogNotEmptyException extends MetadataException { ... }

// Table-related
public class TableNotFoundException extends MetadataException { ... }

// external-backend-related
public class CatalogBackendException extends MetadataException { ... }

// persistence-related
public class MetastoreException extends MetadataException { ... }

// serialization-related
public class MetadataSerializationException extends MetadataException { ... }

// Schema-change-related (not an exception, a structured result)
public record ValidationResult(boolean compatible, List<String> errors, List<String> warnings) {}
```



**Exception → PangrovaErrorCode Mapping**:

| Exception | Error Code | Code | Retryable |
|-----------|-----------|------|-----------|
| CatalogNotFoundException | CATALOG_NOT_FOUND | 1701 | No |
| CatalogAlreadyExistsException | CATALOG_ALREADY_EXISTS | 1702 | No |
| CatalogSuspendedException | CATALOG_SUSPENDED | 1703 | No |
| CatalogProviderNotFoundException | CATALOG_PROVIDER_NOT_FOUND | 1704 | No |
| CatalogBackendException | CATALOG_BACKEND_ERROR | 1705 | Yes (external) |
| MetastoreException | METASTORE_ERROR | 1706 | Yes (transient) |
| MetadataSerializationException | METADATA_SERIALIZATION_ERROR | 1707 | No |
| TableNotFoundException | TABLE_NOT_FOUND | 1101 | No |

> Note: `CatalogBackendException` and `MetastoreException` are retryable because they represent transient external failures. All other metadata errors are user errors (wrong name, already exists, etc.) and should not be retried. Error codes registered in [resilience.md §2](./resilience.md).

### 10.2 Error Handling Strategy

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                         Error Handling Strategy                                                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                        │
│  Error scenario                       │ Handling                       │ Degradation behavior          │
│  ──────────────────────────────────────────────────────────────────────────────────────────────────────│
│  External backend unreachable (query) │ Return cached data (if any)    │ DEGRADED state                │
│  External backend unreachable (sync)  │ Exponential-backoff retry (3x) │ Log error, skip               │
│  External backend unreachable (DDL)   │ Throw CatalogBackendException  │ Do not update Metastore       │
│  Metastore DB connection failure      │ Throw MetastoreException       │ Cache only                    │
│  Redis connection failure             │ Fall back to L1-only cache     │ Skip L2 read/write            │
│  Incompatible schema change           │ Return ValidationResult        │ Do not apply change           │
│  JSON deserialization failure         │ Throw SerializationException   │ Re-fetch from external backend│
│  Duplicate Catalog name               │ Throw AlreadyExistsException   │ Keep the existing Catalog     │
│                                                                                                        │
│  Principles:                                                                                           │
│  • External backend errors → wrapped as Pangrova exceptions without leaking internal details             │
│  • Internal errors (Metastore/Cache) → log and degrade rather than fail when possible                  │
│  • Schema validation errors → returned as a structured result, no exception thrown                     │
│                                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


---

## 11. Module Structure

```text
pangrova-metadata/
  pangrova-metadata-api/       # metadata.api values and provider contracts
  pangrova-metadata-codec/     # shared table metadata encoding
  pangrova-metadata-catalog/      # domain orchestration, caches and remaining adapters
pangrova-persistence/
  pangrova-persistence-catalog-api/
    .../api/catalog/          # deployment authority
    .../api/projection/       # table and schema projection ports
    .../api/synchronization/  # checkpoints and run receipts
    .../api/dependency/       # dependency graph port
    .../api/invalidation/     # maintenance port
  pangrova-persistence-catalog-postgresql/
    .../postgresql/catalog/
    .../postgresql/projection/
    .../postgresql/synchronization/
    .../postgresql/invalidation/
    .../postgresql/transaction/ # internal, not consumer-exported
pangrova-schema-migrations/  # sole canonical initialization resource bundle
```

This is the currently implemented ownership, not a claim that all Metadata adapters,
graph providers or Coordinator assembly have reached their final locations. The
[modularization ADR](domain-modularization-adr.md) defines the complete target and the
current tests and `.pangrova/modules.yaml` record the verified state.

## 12. Observability Integration

This section defines the metrics and tracing integration points for the metadata management layer. For the full observability design, see [observability.md](./observability.md).

### 12.1 Metrics

| Metric | Type | Source Component | Description |
|--------|------|-----------------|-------------|
| `pangrova.metadata.catalog.refresh_total` | counter | CatalogRegistry | Catalog refresh operations |
| `pangrova.metadata.catalog.refresh_latency_ms` | histogram | CatalogRegistry | Catalog refresh latency |
| `pangrova.metadata.schema.cache_hit_ratio` | gauge | SchemaCache | Schema cache hit ratio |
| `pangrova.metadata.table.resolve_latency_ms` | histogram | FederatedCatalog | Table resolution latency |
| `pangrova.metadata.event.published_total` | counter | MetadataEventBus | Metadata change events published |

### 12.2 Tracing

Metadata management emits the following spans (see [14 §4](./observability.md) for span schema):

- `metadata.catalog.resolve` — Catalog resolution span
- `metadata.table.get` — Table metadata fetch span
- `metadata.schema.list` — Schema listing span
- `metadata.stats.collect` — Statistics collection span



> **Cross-reference (§3.1 MetadataService):** `MetadataService` resolves tables and
> Planner builds its Calcite root using `MetadataSchemaBuilder`. The registry implements the
> borrowed `CatalogReadAccess` contract. Table ownership checks remain in Table.
> `FederationCatalogHandle` wraps the `FederatedCatalog` SPI described in §3.8.


> **FederatedCatalog Authoritative Location**: FederatedCatalog SPI is defined in §3.8 of this document (07). All former references to "11 §6.2" have been corrected to point here. See also [00 §5.1](./overview.md) SPI summary table.


> **MetadataService implementation:** `MetadataService` is an interface and
> `DefaultMetadataService` is its current implementation. The traced decorator in
> §7.2 is proposed and is not present in the source tree.


### Engine creation declarations

`DefaultMetadataService.createTable` forwards typed scalar declarations to connector
schema builders. DECIMAL uses the supplied precision and scale; VARCHAR retains its
optional length. Missing/invalid parameters, unknown types and structured declarations
that the Metadata model cannot represent are rejected before physical creation. The
service does not fabricate format field IDs. This bridge does not establish REST
catalog protocol compatibility, recursive schema/nullability support or field-bound
governance; each requires its own provider contract and qualification.

### Durable synchronization recovery

Catalog Persistence records immutable projection and completion intents before execution.
`MetadataSyncService` resolves them under their original IDs before reading another source batch,
including when the service and providers are recreated. An unresolved intent rejects new IDs;
Pending and OutcomeUnknown errors stop the invocation without a full-sync fallback. Projection,
parent schema observation, invalidation, receipt and terminal intent state commit together;
checkpoint, completion receipt and terminal intent state also commit together. Obsolete deployment
intents are fenced and rejected, never reauthorized with a current descriptor.

This is per-operation recovery, not a lease or an atomic publication of an entire scan. A new scan
may create new observations after pending work is recovered. Generic cache invalidation is durable;
process-local schema-evolution event recovery, retention and scan lifecycle remain incomplete.
