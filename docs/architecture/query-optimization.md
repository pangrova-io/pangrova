---
title: "Query Optimization"
sidebar_label: "Query Optimization"
---

# Pangrova — Query Optimization

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** Rule- and cost-based optimization, materialized views, the query-result cache, admission control, and smart routing.

## 1. Query Optimization Capabilities Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Query Optimization Capabilities                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Capability matrix:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Capability           │ Note                        │ Implementation    │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Materialized View    │ Precompute acceleration     │ Incremental refresh + query rewrite│  │
│  │  Query Result Cache   │ Result cache                │ Semantic fingerprint + TTL│   │
│  │  Query Throttling     │ Query throttling            │ Token bucket + priority│   │
│  │  Query Routing        │ Smart routing               │ Load-aware + affinity│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Materialized View

### 2.1 Materialized View Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Materialized View Architecture                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Materialized view lifecycle:                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  CREATE MV ──► Initial Build ──► Incremental Refresh ──► Query Rewrite │   │
│  │       │              │                   │                      │        │   │
│  │       ▼              ▼                   ▼                      ▼        │   │
│  │  • Define query   • Full compute      • Incremental update    • Transparent rewrite│   │
│  │  • Refresh policy  • Write to storage   • Change capture        • Cost estimation│   │
│  │  • Partition strategy • Metadata registration • Merge compute      • Freshness check│   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Refresh strategies:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Strategy          │ Trigger condition     │ Applicable scenario        │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  ON DEMAND         │ Manual trigger        │ Large tables, low-frequency updates│   │
│  │  ON SCHEDULE       │ Scheduled trigger     │ Reports, T+1 scenarios     │   │
│  │  ON COMMIT         │ After base table commit│ Small tables, high real-time requirements│   │
│  │  INCREMENTAL       │ Based on change log   │ Aggregation queries that support increments│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Materialized View Management

Materialized views and Dynamic Tables share one durable materialization
kernel. There is no in-memory view manager, no separate metadata store, and no
merge-on-read path.

```
DDL (StatementDispatcher)
  └─ MaterializationDdlRegistration
        ├─ KernelDynamicTableService        (DYNAMIC_TABLE)
        └─ KernelMaterializedViewService    (MATERIALIZED_VIEW)
              └─ MaterializationService (pangrova-materialization)
                    ├─ repository: objects, definitions, attempts, generations
                    ├─ catalog:    published head read model
                    ├─ planner:    AST -> planned definition in the query cluster
                    ├─ execution:  native CTAS into __pg_mat_<32 hex>
                    └─ publication: commit evidence + CAS head swap
```

Lifecycle:

| Command | Durable effect |
|---|---|
| `CREATE MATERIALIZED VIEW ... AS SELECT` | Persist the object, definition revision and schedule, plan once under the definer identity, execute native CTAS, commit evidence, publish revision 1. Optional `TARGET_LAG` / `REFRESH_MODE` select scheduling and refresh strategy |
| `REFRESH MATERIALIZED VIEW` | Re-plan the stored definition against current catalog state, build a new candidate generation, verify the commit evidence, CAS-publish the new head |
| `ALTER MATERIALIZED VIEW ... REFRESH / SUSPEND / RESUME / SET TARGET_LAG` | Manual refresh, schedule suspension/resumption, or durable target-lag change without touching the definition or published head |
| `SHOW` / `DESCRIBE` | Read only the published head; never scan an in-flight candidate. Both require `SELECT` on the object, and `SHOW` omits objects the session cannot read |
| `DROP` | Drop in one transaction: clear the published head, retire the head generation for GC, and settle every in-flight attempt. Physical tables stay until GC proves them unpinned. A plain drop fails while a live materialization consumes the object; `CASCADE` drops transitive dependents first, deepest dependent first |

Guarantees:

- Every published head is backed by a committed Iceberg snapshot whose
  fingerprint and manifest hash are recorded as `CommitEvidence`.
- Publishing is a CAS on the object revision; a stale attempt cannot overwrite
  a newer head.
- Physical generations live in `<catalog>.__pangrova_materializations` as
  `__pg_mat_<32 hex>`; direct SQL access is rejected during authorization and
  the namespace is hidden from `SHOW SCHEMAS` / `SHOW TABLES` / `DESCRIBE`.
- The definition is compiled in the query cluster, with the session properties
  captured at create time, and never recursively rewrites through another
  materialization.
- Definition revisions record their upstream materialization edges. The graph
  is acyclic (enforced when a revision is recorded), a plain `DROP` is rejected
  while live dependents exist, and `DROP ... CASCADE` drops transitively
  dependent objects before their upstream. Database triggers reject both a
  dropped upstream and a dependency insert racing a drop.
- Materialized views use the same kernel capabilities as Dynamic Tables:
  omitting `TARGET_LAG` creates a `DISABLED` (manual) schedule, a positive lag
  creates an `ACTIVE` durable schedule, and `REFRESH_MODE = AUTO` persists
  incremental refresh with the same fail-closed strategy rules. `PT0S` is
  rejected as an ambiguous target lag.
- A redefinition advances the definition revision; the next refresh
  bootstraps one full generation before incremental refresh resumes, because
  the published generation no longer describes the new output.
- Recovery only settles attempts whose execution anchor is no longer live;
  quarantined attempts are retried after the quiescing grace even while their
  anchor lives. A live coordinator is never robbed of its in-flight attempt.
- Reader-pin insertion serializes with generation collection, so a generation
  selected by GC cannot be dropped after a concurrent reader pinned it.
- A scheduled refresh that loses to a concurrent publication releases its
  lease and retries after the base backoff without counting a failure.
  Reconciliation, source-pin reclamation, and GC run on their own executor, so
  a refresh that lasts for minutes never delays recovery or reclamation.
- Policy fingerprints include the effective row filter and column masks for
  each source, so a policy change fails the next refresh with `policyDrift`
  instead of silently serving data computed under the old policy.
- Definition revisions persist the definer principal together with its groups.
  Planning, policy fingerprinting, and native execution all rebuild the definer
  identity from that persisted state, so group-derived grants and RLS behavior
  stay identical across manual and scheduled refreshes.
- An ambiguous commit is quarantined rather than guessed: only evidence that
  proves the commit or proves its absence settles the attempt. `DROP` settles
  every remaining attempt of the object, so a quarantined name can be recreated.
- A replayed command id is scoped to the object that recorded it and re-checks
  the caller's authority before returning the stored outcome.

### 2.3 Query Rewrite

Transparent rewrite runs after optimization, on the plan that has already passed
authorization and policy application. Candidates are published
`MATERIALIZED_VIEW` generations of the authenticated tenant, read through the
unified materialization kernel; there is no separate in-memory view registry.

```
CompilationPhase
  ├─ compileForPolicyApplication      (no optimization facts)
  ├─ AccessControl + column masks + row filters
  │     └─ observed policies → rewrite disabled when non-empty
  ├─ time-travel extraction
  │     └─ snapshot overrides → rewrite disabled
  └─ optimizeProtectedQuery(plan, allowRewrite)
        └─ MaterializedViewRewritePass
              ├─ candidates: published, same catalog+schema, sources intersect
              ├─ per-candidate guards
              │     ├─ generation age <= pangrova.mv_rewrite_max_staleness
              │     ├─ requester holds unfiltered SELECT on the view
              │     └─ deterministic SPJG definition (no outer join,
              │        DISTINCT, grouping sets, window, sort, set op)
              ├─ compile definition in the query cluster (no recursive rewrite)
              ├─ RelOptMaterializations.useMaterializedViews
              └─ post-check: output shape unchanged and scans restricted to
                    original sources plus the exact published generation
```

Any failed guard, unplannable definition, or unexpected substitution returns
the original plan; a rewrite never falls back to an approximate result. The
rewritten plan scans the physical generation table stored under
`<catalog>.__pangrova_materializations.__pg_mat_<32 hex>`, and the reader pin
for that generation is leased through the same path as Dynamic Table reads.

Session properties:

| Property | Default | Effect |
|---|---|---|
| `pangrova.mv_rewrite` | `on` | `off`/`false`/`0` disables transparent rewrite for the session |
| `pangrova.mv_rewrite_max_staleness` | `30m` | Plain seconds or ISO-8601 duration; invalid or non-positive values disable rewrite |

`EXPLAIN` reports the physical generation scan when a rewrite applies, and a
trailing `materialized_view_rewrite: skipped reason=<reason>` line when it does
not; the reasons distinguish `no_published_candidate`, `stale`,
`definition_unplannable`, `ineligible`, `physical_table_unresolved`,
`no_equivalent_substitution`, `post_check_failed`, `policies_applied`,
`time_travel`, and `rewrite_disabled`. The definition planner used to build the
MV never rewrites through another materialization, so a chain of views cannot
recurse.

### 2.4 Materialized View SQL

```sql
-- Create a materialized view (initial generation is built and published).
-- TARGET_LAG and REFRESH_MODE are optional; omitting TARGET_LAG means manual refresh.
CREATE MATERIALIZED VIEW iceberg.sales.sales_daily_summary AS
SELECT
    date_trunc('day', order_time) AS order_date,
    product_category,
    SUM(amount) AS total_amount,
    COUNT(*) AS order_count
FROM iceberg.sales.orders
GROUP BY 1, 2;

-- A positive target lag schedules automatic refreshes; AUTO persists incremental mode.
CREATE MATERIALIZED VIEW iceberg.sales.sales_hourly_summary
TARGET_LAG = 'PT10M' REFRESH_MODE = AUTO AS
SELECT date_trunc('hour', order_time) AS order_hour, SUM(amount) AS total_amount
FROM iceberg.sales.orders
GROUP BY 1;

-- Recompute and publish a new generation
REFRESH MATERIALIZED VIEW iceberg.sales.sales_daily_summary;

-- Lifecycle and scheduling changes
ALTER MATERIALIZED VIEW iceberg.sales.sales_daily_summary REFRESH;
ALTER MATERIALIZED VIEW iceberg.sales.sales_daily_summary SUSPEND;
ALTER MATERIALIZED VIEW iceberg.sales.sales_daily_summary RESUME;
ALTER MATERIALIZED VIEW iceberg.sales.sales_daily_summary SET TARGET_LAG = 'PT30M';

-- Inspect the published head and its definition revision
SHOW MATERIALIZED VIEWS;
DESCRIBE MATERIALIZED VIEW iceberg.sales.sales_daily_summary;

-- Remove the object; physical generations stay until GC proves them unpinned.
-- A plain drop fails when a live materialization still consumes this view;
-- CASCADE drops transitive dependents first, deepest dependent first.
DROP MATERIALIZED VIEW iceberg.sales.sales_daily_summary;
DROP MATERIALIZED VIEW iceberg.sales.sales_daily_summary CASCADE;

-- Dynamic Table with full snapshot refresh on creation and manual refresh
CREATE DYNAMIC TABLE iceberg.sales.orders_rollup
TARGET_LAG = 'PT5M' REFRESH_MODE = SNAPSHOT AS
SELECT product_category, SUM(amount) AS total_amount
FROM iceberg.sales.orders
GROUP BY product_category;

-- Manual-only Dynamic Table: omit TARGET_LAG; 'PT0S' is rejected as ambiguous.
CREATE DYNAMIC TABLE iceberg.sales.orders_rollup_live
AS SELECT * FROM iceberg.sales.orders;

ALTER DYNAMIC TABLE iceberg.sales.orders_rollup REFRESH;
ALTER DYNAMIC TABLE iceberg.sales.orders_rollup SUSPEND;
ALTER DYNAMIC TABLE iceberg.sales.orders_rollup RESUME;

-- Drop the table and every transitive dependent, dependents first.
-- A plain DROP fails while a live materialization still depends on the table.
DROP DYNAMIC TABLE iceberg.sales.orders_rollup CASCADE;
```

---



## 3. Query Result Cache

### 3.1 Result Cache Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Query Result Cache Architecture                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Cache flow:                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Query ──► Semantic Fingerprint ──► Cache Lookup ──► Hit/Miss          │   │
│  │    │              │                      │               │               │   │
│  │    │              ▼                      ▼               ▼               │   │
│  │    │       • SQL normalization       • Fingerprint match   Hit: return cached result│   │
│  │    │       • Parameter extraction    • Freshness check     Miss: execute query│   │
│  │    │       • Hash computation        • Permission check │               │   │
│  │    │                                                     ▼               │   │
│  │    └─────────────────────────────────────────────► Cache result         │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Cache levels:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Level              │ Storage location│ Capacity    │ Latency            │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  L1 (Local)         │ JVM Heap      │ 100MB       │ < 1ms              │   │
│  │  L2 (Distributed)   │ Redis         │ 10GB        │ < 10ms             │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Semantic Fingerprint Computation

The cache fingerprint must include security dimensions to ensure cached results are strictly isolated across different tenants, users, roles, RLS policies, and column-mask policies, preventing data leakage across security boundaries.

**Complete fingerprint composition**:

```
CacheFingerprint = hash(
    normalized_sql,          // Normalized SQL text
    catalog + schema,        // Target catalog and schema
    session_properties,      // Session properties
    data_version,            // Data version (snapshot_id)
    tenant_id,               // Tenant ID — tenant isolation
    user_principal,          // User principal — user-level isolation
    active_roles_hash,       // Hash of the active role set — role-level isolation
    rls_policy_hash,         // Hash of the matched RLS policy — row-level security isolation
    column_mask_hash         // Hash of the matched column-mask policy — column-level security isolation
)
```

```java
/**
 * Query semantic fingerprint calculator
 *
 * The fingerprint covers three dimensions: query semantics + data version + security context,
 * ensuring cached results do not leak across tenant, user, role, or security-policy boundaries.
 */

public class QueryFingerprintCalculator {

    public QueryFingerprint calculate(String sql, QueryContext context) {
        String normalizedSql = normalize(sql);
        List<Object> parameters = extractParameters(sql);
        String baseFingerprint = hash(normalizedSql);
        String contextHash = hash(context.getCatalog(), context.getSchema(),
                                   context.getSessionProperties());
        String dataVersion = getDataVersion(extractTables(sql));

        // Security dimensions — prevent cross-boundary cache leakage
        String tenantId = context.getTenantId();
        String userPrincipal = context.getUserPrincipal();
        String activeRolesHash = hash(context.getActiveRoles());
        String rlsPolicyHash = computeRlsPolicyHash(context, extractTables(sql));
        String columnMaskHash = computeColumnMaskHash(context, extractTables(sql));

        return QueryFingerprint.builder()
            .baseFingerprint(baseFingerprint)
            .contextHash(contextHash)
            .dataVersion(dataVersion)
            .parameters(parameters)
            .tenantId(tenantId)
            .userPrincipal(userPrincipal)
            .activeRolesHash(activeRolesHash)
            .rlsPolicyHash(rlsPolicyHash)
            .columnMaskHash(columnMaskHash)
            .build();
    }
}
```

### 3.3 Cache Invalidation Triggers (Security Policy Changes)

When security policies change, related caches must be proactively invalidated to prevent cached results under old policies from continuing to be hit after new policies take effect.

| Trigger event | Invalidation scope | Note |
|----------|----------|------|
| RLS policy change | All cache entries for the table | RLS rules affect row-level filtering; all cached results for the same table may become stale |
| Column mask change | All cache entries for the table | Column masks affect returned data content; all cached results for the same table may become stale |
| Role assignment change | All cache entries for the user | Role changes affect active_roles_hash; all cache fingerprints for the user are invalidated |

**Invalidation mechanism**:

```
SecurityPolicyChangeListener:
  onRlsPolicyChange(table)       → cacheManager.invalidateByTable(table)
  onColumnMaskChange(table)      → cacheManager.invalidateByTable(table)
  onRoleAssignmentChange(user)   → cacheManager.invalidateByUser(user)
```

**Design points**:
- Policy change events are observed via `SecurityPolicyChangeListener`, published by the security module (see `security.md`)
- Per-table invalidation uses a cache tag mechanism: each cache entry is associated with the list of tables it involves, and is invalidated in bulk by tag on change
- Per-user invalidation uses a user index: cache entries are given a secondary index by `tenant_id + user_principal`
- Both L1 (JVM Heap) and L2 (Redis) must perform invalidation; L2 broadcasts invalidation events to all Coordinator nodes via Redis Pub/Sub
-
- **Redis unavailability degradation**: When Redis Pub/Sub is unreachable, L1 cache TTL automatically shrinks to `result-cache.l1.degraded-ttl` (default 10s) to bound the window during which stale security-policy results may be served. This ensures security policy changes propagate within the degraded TTL even without Redis broadcast. See also [distributed.md §3.4](./distributed.md) — Redis failure impact should be understood as "performance degradation; security cache invalidation may be delayed up to degraded-TTL"

Note: the security module is responsible for publishing `SecurityPolicyChangeEvent` (RLS / column masking / role changes); the Query Result Cache only consumes events and performs invalidation, without directly perceiving security policy details.
```

### 3.4 Cache Control SQL

```sql
-- Enable/disable result cache
SET SESSION query_result_cache = true;

-- Disable cache for a query
SELECT /*+ NO_CACHE */ * FROM orders WHERE order_date = '2024-01-01';

-- Force cache refresh for a query
SELECT /*+ REFRESH_CACHE */ * FROM orders WHERE order_date = '2024-01-01';

-- View cache statistics
SHOW CACHE STATS;

-- Clear the cache
CALL system.clear_query_cache();
```

---

## 4. Query Throttling

### 4.1 Throttling Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Query Throttling Architecture                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Throttling dimensions:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Dimension          │ Throttling method     │ Configuration example      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Global             │ Global concurrency limit│ max_concurrent_queries=100│   │
│  │  Per User           │ Per-user concurrency limit│ user_max_concurrent=10    │   │
│  │  Per Tenant         │ Per-tenant concurrency limit│ tenant_max_concurrent=50  │   │
│  │  Per Resource Queue │ Queue-level limit     │ queue_max_concurrent=20   │   │
│  │  Per Query Type     │ Query-type limit      │ ddl_max_concurrent=5      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Throttling algorithms:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Algorithm          │ Characteristics       │ Applicable scenario        │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Token Bucket       │ Allows bursts         │ General queries (Guava RateLimiter)│  │
│  │  Leaky Bucket       │ Smooth throttling     │ Write operations           │   │
│  │  Sliding Window     │ Precise control       │ API calls (Planned)        │   │
│  │  Concurrency Limit  │ Concurrency control   │ Resource-intensive (Semaphore)│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Throttling Service

```java
/**
 * Query throttling service
 */
public class QueryThrottlingService {

    private final Map<String, RateLimiter> userLimiters = new ConcurrentHashMap<>();
    private final Map<String, RateLimiter> tenantLimiters = new ConcurrentHashMap<>();
    private final Semaphore globalConcurrency;

    public ThrottleResult tryAcquire(QueryContext context) {
        String user = context.getUser();
        String tenant = context.getTenant();

        // 1. Check global concurrency
        if (!globalConcurrency.tryAcquire()) {
            return ThrottleResult.rejected("Global concurrency limit exceeded");
        }

        // 2. Check tenant throttling
        if (!getTenantLimiter(tenant).tryAcquire()) {
            globalConcurrency.release();
            return ThrottleResult.rejected("Tenant rate limit exceeded");
        }

        // 3. Check user throttling
        //
        if (!getUserLimiter(user).tryAcquire()) {
            globalConcurrency.release();
            getTenantLimiter(tenant).release();  // release tenant token to prevent quota leak
            return ThrottleResult.rejected("User rate limit exceeded");
        }

        return ThrottleResult.acquired(new ThrottlePermit(user, tenant));
    }

    //
    // Rejection handling:
    //   - HTTP response: 429 Too Many Requests + Retry-After header (seconds until next token)
    //   - Error code: PANGROVA-THROTTLED (to be registered in resilience.md error code table)
    //   - No queue: rejected queries are NOT enqueued; client must retry after Retry-After
    //   - Audit: throttle rejection events logged to observability.md audit trail
}
```

### 4.3 Query Complexity Throttling

```java
/**
 * Query complexity estimator
 */
public class QueryComplexityEvaluator {

    public ComplexityScore evaluate(ExecutionPlan plan) {
        long estimatedRows = plan.getEstimatedRows();
        long estimatedBytes = plan.getEstimatedBytes();
        int joinCount = countJoins(plan);
        int subqueryDepth = getMaxSubqueryDepth(plan);
        int aggregationCount = countAggregations(plan);

        double score =
            0.3 * normalize(estimatedRows, 10_000_000_000L) +
            0.3 * normalize(estimatedBytes, 1024L * 1024 * 1024 * 1024) +
            0.2 * normalize(joinCount, 10) +
            0.1 * normalize(subqueryDepth, 5) +
            0.1 * normalize(aggregationCount, 20);

        return new ComplexityScore(score, Map.of(
            "estimatedRows", estimatedRows,
            "estimatedBytes", estimatedBytes,
            "joinCount", joinCount
        ));
    }
}
```

### 4.4 Adaptive Throttling

```java
/**
 * Adaptive throttling controller
 */
public class AdaptiveThrottlingController {

    private volatile double currentLimit;

    @Scheduled(fixedRate = 10_000)
    public void adjust() {
        ClusterMetrics metrics = metricsCollector.collect();

        double cpuUsage = metrics.getAvgCpuUsage();
        double memoryUsage = metrics.getAvgMemoryUsage();
        double errorRate = metrics.getQueryErrorRate();

        if (cpuUsage > 0.9 || memoryUsage > 0.9 || errorRate > 0.1) {
            currentLimit = Math.max(currentLimit * 0.8, config.getMinLimit());
        } else if (cpuUsage < 0.5 && memoryUsage < 0.5 && errorRate < 0.01) {
            currentLimit = Math.min(currentLimit * 1.1, config.getMaxLimit());
        }

        updateLimiters(currentLimit);
    }
}
```

### 4.5 Throttling Configuration SQL

```sql
-- View current throttling configuration
SHOW THROTTLING CONFIG;

-- Set user throttling
SET USER RATE LIMIT 'analyst' = 100 QUERIES PER MINUTE;

-- Set tenant throttling
SET TENANT RATE LIMIT 'tenant_a' = 500 QUERIES PER MINUTE;

-- View throttling status
SHOW THROTTLING STATUS;

-- View queued queries
SELECT * FROM system.queued_queries ORDER BY priority DESC;
```

---

## 5. Smart Query Routing

### 5.1 Routing Strategies

```java
/**
 * Smart query router
 */
public class SmartQueryRouter {

    public WorkerInfo route(Query query, QueryContext context) {
        List<WorkerInfo> candidates = workerManager.getHealthyWorkers();

        // 1. Data affinity routing
        if (query.hasDataLocality()) {
            WorkerInfo localWorker = findLocalWorker(query, candidates);
            if (localWorker != null) return localWorker;
        }

        // 2. Cache affinity routing
        if (query.isCacheable()) {
            WorkerInfo cachedWorker = findCachedWorker(query, candidates);
            if (cachedWorker != null) return cachedWorker;
        }

        // 3. Load balancing routing
        return loadBalancer.select(candidates);
    }
}
```

---

## 6. CBO Statistics Collection

### 6.1 Statistics Collection Trigger Mechanism

CBO (Cost-Based Optimization) relies on accurate table/column statistics. Pangrova supports three statistics collection strategies:

Correctness boundary: the current CBO statistics are cost estimates, not
authoritative bounds tied to the query's exact source snapshot. They must never
rewrite a table scan into an empty relation, including for a literal outside an
observed min/max range. The unsafe planner-wide elimination rule was removed.
Connector file/row-group pruning must use the query-bound storage metadata;
future planner elimination requires an exact snapshot/coverage contract and
comparison-specific proof. `StatisticsPruningSafetyTest` verifies the real SQL
compiler retains sources despite stale cost statistics.

| Trigger | Mechanism | Applicable Scenario | Overhead |
|---------|-----------|---------------------|----------|
| Manual `ANALYZE` | User explicitly runs `ANALYZE TABLE` | Ad-hoc, first-time import, schema change | On-demand, no background cost |
| Auto-on-Write | Post-commit hook after write operations | Frequently updated tables, streaming ingestion | Low — incremental sampling |
| Periodic Scheduled | Cron-based background job | Large tables with infrequent writes | Configurable — off-peak execution |

### 6.2 Manual Collection

Pangrova implements `ANALYZE TABLE` for tables with an ACTIVE LINKED or MANAGED
control record. The collected statistics are persisted against the stable
`table_id` and the version token observed during collection; the planner uses them
only while the control record still carries that token. OBSERVED tables have no
stable identity and are rejected.

```sql
-- Full statistics collection
ANALYZE TABLE iceberg.sales.orders;

-- Same behavior; COMPUTE STATISTICS is accepted for compatibility
ANALYZE TABLE iceberg.sales.orders COMPUTE STATISTICS;
```

Column-subset and partition-scoped collection are not implemented. Use
`SHOW MATERIALIZATIONS`/connector system tables to inspect stored state; there is
no `SHOW STATS` statement.

### 6.3 Auto-on-Write Collection

After a successful write commit (INSERT / MERGE / CTAS), Pangrova automatically collects lightweight statistics on the affected data files.

```
Write Commit Flow:
  CommitTask.commit()
    → onCommitSuccess()
      → StatisticsCollector.collectIncremental(newDataFiles)
        → sample rows (default 1% or min 10000 rows)
        → update column stats (min, max, NDV, null_count, avg_size)
        → merge into table-level statistics
```

**Collected metrics per column**:

| Metric | Description | Collection Method |
|--------|-------------|-------------------|
| `row_count` | Total row count | Exact from file metadata |
| `null_count` | NULL value count | Exact from file metadata |
| `min` / `max` | Min/max values | Exact from Parquet/ORC column stats |
| `ndv` | Number of distinct values | HyperLogLog sketch (mergeable) |
| `avg_size` | Average value size in bytes | Sampled |
| `histogram` | Equi-height histogram (optional) | Sampled, 256 buckets |

**Auto-collection config**:

```yaml
statistics:
  auto-collect-on-write:
    enabled: true
    sample-ratio: 0.01          # 1% sampling for NDV/histogram
    min-sample-rows: 10000
    collect-histogram: false    # histogram disabled by default (expensive)
    max-collect-time: 30s       # timeout per table
```

### 6.4 Periodic Scheduled Collection

For tables that receive external writes (e.g., Spark ETL) or where auto-on-write is insufficient, a background scheduler periodically refreshes statistics.

```yaml
statistics:
  scheduled-collection:
    enabled: true
    cron: "0 3 * * *"          # daily at 03:00
    max-concurrent-tables: 4
    stale-threshold: 24h       # re-collect if stats older than this
    priority-tables:            # high-priority tables collected first
      - iceberg.sales.orders
      - iceberg.sales.lineitem
```

**Staleness detection**: The scheduler compares `statistics.last_updated_snapshot` with the table's current snapshot ID. If they differ and the time since last collection exceeds `stale-threshold`, the table is enqueued for re-collection.

### 6.5 Statistics Storage

Statistics storage location depends on the Connector type. The design principle is: statistics co-locate with the metadata they describe, ensuring transactional consistency.

| Connector | Storage Location | Rationale |
|-----------|-----------------|-----------|
| Iceberg | Iceberg TableMetadata `properties` | Statistics and table metadata share the same atomic commit (snapshot), ensuring consistency. External engines (Spark/Trino) can also read Pangrova-produced statistics. |
| Paimon | Paimon TableMetadata `options` | Same rationale as Iceberg — co-located with Paimon snapshot metadata. |
| Federation (HMS/Glue) | MetadataStore (PostgreSQL) `table_statistics` table | External Metastore may not support custom properties. Pangrova stores statistics in its own PostgreSQL, keyed by `(catalog, schema, table, snapshot_id)`. |

**Iceberg Statistics Storage** (primary path):

```
TableMetadata
  └── properties
        ├── pangrova.stats.row_count = "1000000"
        ├── pangrova.stats.last_updated_snapshot = "3847291038"
        ├── pangrova.stats.last_updated_time = "2025-02-09T03:00:00Z"
        └── pangrova.stats.columns
              ├── order_id  → { min, max, ndv, null_count, avg_size }
              ├── amount    → { min, max, ndv, null_count, avg_size, histogram }
              └── order_time → { min, max, ndv, null_count, avg_size }
```

**Federation Statistics Storage** (PostgreSQL fallback):

```sql
CREATE TABLE table_statistics (
    catalog_name VARCHAR(128) NOT NULL,
    schema_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    snapshot_id VARCHAR(64),              -- NULL for non-versioned sources
    row_count BIGINT,
    column_stats JSON,                    -- { "col_name": { min, max, ndv, ... } }
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY (catalog_name, schema_name, table_name)
);
```

**Statistics Read Path** (CBO Optimizer):



```
CBO Optimizer requests statistics
  → StatisticsProvider.getTableStatistics(catalogSchemaTable)
    → if Iceberg/Paimon: read from TableMetadata.properties (via ConnectorMetadata.getTableStatistics() — see 04 §1.2)
    → if Federation: read from PostgreSQL table_statistics (via MetadataStore)
    → if not found: return UNKNOWN (CBO falls back to heuristic estimation)
```

**StatisticsCollector as Connector SPI**: `StatisticsCollector.collectIncremental()` (§6.3) is not a standalone class but delegates to `ConnectorMetadata.getTableStatistics()` ([connectors.md §1.2](./connectors.md)) for connector-specific collection. Each connector implements its own collection strategy (Iceberg: file metadata scan; Paimon: snapshot stats; JDBC: `SHOW TABLE STATUS` or sampling). The `StatisticsProvider` in the read path is a thin wrapper over `ConnectorMetadata.getTableStatistics()`.

**Timeout and failure handling** (§6.3 `max-collect-time: 30s`): If collection exceeds the timeout, the partial results collected so far are stored with a `confidence_score < 1.0` (see OPT-004 §8.1 staleness-aware CBO). The table is re-enqueued for full collection in the next scheduled cycle. No data is discarded — partial stats are always better than no stats.

> **Cross-reference**: Statistics are stored alongside table metadata managed by [metadata.md](./metadata.md). The `MetadataCache` (§3.5) also caches statistics to avoid repeated reads from Iceberg metadata files or PostgreSQL.

---

## 7. Configuration Summary



```yaml
# Query optimization configuration
query-optimization:

  # Materialized views
  materialized-view:
    enabled: true
    auto-refresh-threads: 4
    max-concurrent-refresh: 10
    query-rewrite:
      enabled: true
      max-staleness: 1h

  # Query result cache
  result-cache:
    enabled: true
    l1:
      max-size: 100MB
      ttl: 5m
    l2:
      type: redis
      max-size: 10GB
      ttl: 1h
    max-cacheable-rows: 100000

  # Query throttling
  #
  # Implementation: globalConcurrency uses java.util.concurrent.Semaphore (Concurrency Limit);
  # user/tenant limiters use Guava RateLimiter (Token Bucket with SmoothBursty).
  # Sliding Window is planned for future API-level rate limiting (not in MVP).
  throttling:
    enabled: true
    global-max-concurrent: 100
    default-user-rate-limit: 100/min
    default-user-burst-size: 20          # max burst before rate limiting kicks in
    default-tenant-rate-limit: 500/min
    default-tenant-burst-size: 100       # max burst for tenant
    adaptive:
      enabled: true
      min-limit: 10
      max-limit: 200

  # Query routing
  routing:
    data-locality: true
    cache-affinity: true
    load-balance-algorithm: weighted-round-robin
```

---

## 8. Statistics Collection Enhancement (OPT-004)



### 8.1 Architecture Description

The existing three-strategy statistics model (§6) has four gaps: inaccurate sampling for skewed columns, stale statistics between scheduled collections, histograms disabled by default, and unreliable heuristic fallback. OPT-004 introduces four improvements that layer on top of the existing infrastructure without replacing it.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                  Enhanced Statistics Collection Pipeline                      │
│                                                                              │
│  ┌────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐  │
│  │ File-Level Stats   │   │ Adaptive Sampling     │   │ Auto Histogram   │  │
│  │ (Iceberg/Parquet   │   │ (1%-10% based on      │   │ (equi-height,    │  │
│  │  footer metadata)  │   │  cardinality & churn)  │   │  256 buckets)    │  │
│  └────────┬───────────┘   └──────────┬────────────┘   └────────┬─────────┘  │
│           │                          │                          │            │
│           ▼                          ▼                          ▼            │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Unified ColumnStatistics Store                     │   │
│  │  (min, max, null_count, row_count, NDV, avg_size, histogram,        │   │
│  │   confidence_score)                                                  │   │
│  └──────────────────────────────────┬──────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│                        ┌──────────────────────────┐                        │
│                        │ Staleness-Aware CBO       │                        │
│                        │ (confidence-weighted       │                        │
│                        │  cardinality estimates)    │                        │
│                        └──────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Improvement 1 — Adaptive Sampling Rate**: Replaces the fixed 1% sampling rate (§6.3) with a dynamic rate (1%–10%) based on column cardinality, table change ratio, and whether prior statistics exist. High-cardinality or heavily-modified tables receive higher sampling rates for more accurate NDV estimates.

**Improvement 2 — File-Level Statistics Integration**: Reads per-column min/max/null_count directly from Iceberg/Parquet file footer metadata. This is zero-I/O-cost (metadata only) and always accurate. NDV falls back to sampling only when file-level HLL sketches are unavailable.

**Improvement 3 — Automatic Histogram Collection**: Enables equi-height histogram collection (256 buckets) by default for low-cardinality columns and columns used in join keys or filter predicates. Histograms allow the optimizer to detect data skew and produce more accurate selectivity estimates.

**Improvement 4 — Staleness-Aware CBO**: Attaches a confidence score (0.1–1.0) to statistics based on snapshot staleness and table change rate. The `PangrovaCostModel` uses confidence to widen cost estimate ranges, producing more conservative (safer) plan choices when statistics are stale.

**Affected SPI Interfaces**:
- `ConnectorMetadata.getTableStatistics()` — enhanced to return a confidence score
- `PangrovaCostModel` — updated to use confidence-weighted cardinality estimates
- New `StatisticsCollector` SPI for connector-specific collection strategies
- Proposed configuration namespace: `pangrova.stats.auto-histogram`,
  `pangrova.stats.adaptive-sampling`, `pangrova.stats.staleness-confidence`.
  These keys are not in the current typed configuration surface.

### 8.2 Pseudocode

**Adaptive Sampling Rate**:

```pseudocode
function computeSamplingRate(table: Table, column: Column):
    previousNDV = table.stats.getColumnNDV(column)
    tableRowCount = table.stats.rowCount
    changeRatio = table.getRecentChangeRatio()  // rows changed / total rows

    if previousNDV == null:
        return 0.05  // 5% for first collection (higher accuracy)

    if changeRatio > 0.5:
        return 0.10  // 10% for heavily modified tables

    if previousNDV / tableRowCount > 0.8:
        // High cardinality — need more samples for accurate NDV
        return 0.05

    return 0.01  // 1% for stable, low-cardinality columns
```

**File-Level Statistics Integration**:

```pseudocode
function collectFileBasedStats(table: IcebergTable):
    // Iceberg/Parquet files contain per-column min/max/null_count in footer
    // Use these directly instead of sampling — always accurate, zero I/O cost
    stats = ColumnStatistics()

    for file in table.currentSnapshot().dataFiles():
        for column in table.schema().columns():
            fileStats = file.columnStats(column.id)
            stats.updateMin(column, fileStats.lowerBound)
            stats.updateMax(column, fileStats.upperBound)
            stats.addNullCount(column, fileStats.nullCount)
            stats.addRowCount(fileStats.recordCount)

    // NDV estimation: use HLL merge across file-level HLLs (if available)
    // or fall back to sampling for NDV only
    for column in table.schema().columns():
        if NOT stats.hasNDV(column):
            stats.setNDV(column, estimateNDVFromSampling(table, column))

    return stats
```

**Automatic Histogram Collection**:

```pseudocode
function shouldCollectHistogram(column: Column, stats: ColumnStatistics):
    ndv = stats.getNDV(column)
    rowCount = stats.rowCount

    // Collect histogram for columns likely to have skewed distributions
    if ndv < rowCount * 0.01:
        return true  // Low cardinality — likely skewed (e.g., status, category)

    if column.isJoinKey OR column.isFilterPredicate:
        return true  // Columns used in joins/filters benefit most from histograms

    return false

function collectEquiHeightHistogram(table, column, numBuckets = 256):
    sampleRate = 0.05
    samples = table.sample(sampleRate).select(column).sort(column)

    bucketSize = samples.count / numBuckets
    boundaries = []
    for i in range(1, numBuckets):
        boundaries.append(samples.getRow(i * bucketSize).getValue(column))

    return Histogram(type = EQUI_HEIGHT, boundaries = boundaries, rowsPerBucket = bucketSize)
```

**Staleness-Aware CBO**:

```pseudocode
function getStatisticsWithConfidence(table: Table):
    stats = table.getStatistics()
    staleness = currentSnapshot - stats.lastUpdatedSnapshot

    if staleness == 0:
        return (stats, confidence = 1.0)

    // Compute confidence based on staleness and table change rate
    changeRate = table.getAverageChangeRate()  // rows/hour
    estimatedChangedRows = changeRate * staleness.hours
    changeRatio = estimatedChangedRows / stats.rowCount

    confidence = max(0.1, 1.0 - changeRatio)

    // CBO uses confidence to widen cost estimates
    // Low confidence → wider cost range → more conservative plan choices
    return (stats, confidence)
```

### 8.3 Trade-offs

| Aspect | Current (§6) | With OPT-004 |
|--------|--------------|---------------|
| Performance | CBO with potentially stale/inaccurate stats; heuristic fallback produces bad plans | Fresher stats via file-level integration; adaptive sampling improves NDV accuracy; histograms enable skew detection |
| Complexity | Simple three-strategy model | Medium — adaptive sampling logic, file-level stats integration, histogram management |
| Risk | Low | Low — improvements are additive; existing strategies remain as fallback |
| Storage overhead | Minimal (stats in table metadata) | Moderate — histograms add ~2KB per column per table |
| Collection overhead | 1% sampling + daily full scan | Adaptive sampling (1%-10%); file-level stats are free (metadata only) |

### 8.4 Migration Path

1. **Phase 1 — File-Level Stats Integration**: Read Iceberg/Parquet file-level column stats (min/max/null_count) directly from metadata. Zero I/O cost, always accurate. Biggest ROI.
2. **Phase 2 — Adaptive Sampling**: Replace fixed 1% sampling with adaptive rate based on column cardinality and table change rate.
3. **Phase 3 — Automatic Histogram**: Enable histogram collection for low-cardinality and join-key columns. Default ON for new tables.
4. **Phase 4 — Staleness-Aware CBO**: Integrate confidence scoring into `PangrovaCostModel`. Conservative plan selection when stats are stale.

> **Cross-reference:** The base statistics collection infrastructure is defined in
> §6 above. The OPT-004 keys are proposed; consult the
> [configuration key reference](../configuration/reference.md) for implemented
> settings. `PangrovaCostModel` is part of the CBO pipeline described in
> [query execution](./query-execution.md).


---

## 9. Dynamic Partition Pruning & Runtime Filter (OPT-002)




### 9.1 Architecture Description

Static partition pruning (performed by Calcite optimization rules at planning time) is the only pruning mechanism currently available. No runtime filter propagation exists — the `DynamicFilterSink` placeholder in the native reader pipeline (see [native-execution.md](./native-execution.md)) receives no filters because none are generated. This means star-schema queries scan all fact-table partitions even when a dimension filter could eliminate 90%+ of I/O.

OPT-002 introduces a two-phase runtime filter framework: filter generation on the build side of a join, and multi-level filter application on the probe side. The framework leverages the enhanced statistics from OPT-004 (§8) — specifically file-level column stats and histograms — to estimate filter selectivity and decide filter type.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Runtime Filter Framework                                 │
│                                                                              │
│  Build Side (dimension table)          Probe Side (fact table)               │
│  ┌──────────────────────┐             ┌────────────────────────────┐        │
│  │ Scan dimension table  │             │ Scan fact table             │        │
│  │ WHERE filter_predicate│             │ WITH RuntimeFilter applied  │        │
│  └──────────┬───────────┘             └──────────────┬─────────────┘        │
│             │                                         │                      │
│             ▼                                         │                      │
│  ┌──────────────────────┐    filter propagation       │                      │
│  │ RuntimeFilter         │───────────────────────────▶│                      │
│  │ Generator             │  (via Coordinator or        │                      │
│  │ (min-max for Phase 1) │   direct Worker push)       │                      │
│  └──────────────────────┘                             │                      │
│                                                       ▼                      │
│                                        ┌────────────────────────────┐        │
│                                        │ HashJoin                    │        │
│                                        │ (reduced probe-side data)   │        │
│                                        └────────────────────────────┘        │
│                                                                              │
│  Filter Application Levels:                                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Level 1: Partition Pruning — skip entire partitions via metadata       │  │
│  │ Level 2: Row Group Pruning — skip row groups using column stats       │  │
│  │ Level 3: Row-Level Filtering — filter individual rows within groups   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Phase 1 Scope** (this section): Min/max range filter for partition pruning. The Coordinator collects build-side min/max values after the build stage completes and pushes them to probe-side `ConnectorSplitSource` instances for partition-level and row-group-level pruning.

> **Note**: Advanced capabilities — bloom filter generation, cross-stage propagation, and multi-join filter chaining — are specified in Task 6.5 and will extend this section in a future phase.

**Affected SPI Interfaces**:
- New `RuntimeFilter` Protobuf message in `TaskRequest` for filter delivery
- `ConnectorSplitSource` — new `applyDynamicFilter(RuntimeFilter)` method for partition pruning
- `DynamicFilterSink` in native reader pipeline — activated to receive and apply filters
- `PangrovaCostModel` — updated to estimate runtime filter selectivity in join costing (uses OPT-004 histograms from §8 when available)

### 9.2 Pseudocode

**Filter Generation (Build Side)**:

```pseudocode
class RuntimeFilterGenerator:
    function onBuildSideComplete(buildSideData: RecordBatch[]):
        joinKeys = extractJoinKeys(buildSideData)

        // Phase 1: generate min/max range filter
        // (bloom filter and IN_LIST deferred to Task 6.5)
        minVal = joinKeys.min()
        maxVal = joinKeys.max()
        return RuntimeFilter(type = MIN_MAX, min = minVal, max = maxVal,
                             column = joinKeyColumn)
```

**Filter Application (Probe Side)**:

```pseudocode
class RuntimeFilterApplicator:
    function applyToScan(scan: TableScan, filter: RuntimeFilter):
        // Level 1: Partition pruning (skip entire partitions)
        if scan.table.isPartitioned AND filter.column == scan.table.partitionKey:
            prunedPartitions = scan.partitions.filter(p =>
                filter.mightContain(p.partitionValue))
            scan.setPartitions(prunedPartitions)

        // Level 2: Row group pruning (skip row groups within files)
        for file in scan.files:
            for rowGroup in file.rowGroups:
                stats = rowGroup.columnStats(filter.column)
                if NOT filter.overlaps(stats.min, stats.max):
                    rowGroup.skip()

        // Level 3: Row-level filtering (within row groups)
        scan.addFilter(RuntimeRowFilter(filter))
```

**Filter Propagation (Coordinator-Mediated)**:

```pseudocode
function propagateRuntimeFilter(buildStageId, probeStageId, filter):
    // Coordinator broadcasts filter to probe-side Workers
    for worker in getWorkersForStage(probeStageId):
        worker.applyRuntimeFilter(probeStageId, filter)

    // Alternative: Workers pull filter from Coordinator on scan init
    coordinator.registerRuntimeFilter(probeStageId, filter)
```

### 9.3 Trade-offs

| Aspect | Current (static pruning only) | With OPT-002 Phase 1 (min/max runtime filter) |
|--------|-------------------------------|------------------------------------------------|
| Performance | Full fact table scan for star-schema joins; 10-100x unnecessary I/O | 90%+ I/O reduction for selective dimension filters; 3-10x query speedup |
| Complexity | Simple — static pruning only | Medium — filter generation, Coordinator-mediated propagation, multi-level application |
| Risk | None | Low — runtime filter is additive; worst case is no pruning (filter too broad) |
| Memory overhead | None | Negligible for min/max filters (two scalar values per filter) |
| Latency overhead | None | Build side must complete before probe-side scan starts (pipeline breaker at stage boundary) |

### 9.4 Migration Path

1. **Phase 1 — Min/Max Runtime Filter** (this task): Implement min/max range filter for partition pruning only. Coordinator collects build-side min/max and pushes to probe-side `ConnectorSplitSource`. Simplest form with highest ROI.
2. **Phase 2 — Bloom Filter** (Task 6.5): Add bloom filter generation for high-cardinality join keys. Push to native `DynamicFilterSink` for row-group and row-level filtering.
3. **Phase 3 — Cross-Stage Propagation** (Task 6.5): Enable filter propagation across shuffle boundaries (build stage → probe stage via Coordinator).
4. **Phase 4 — Multi-Join Filter Chaining** (future): Support filter propagation through multiple join levels (e.g., fact → dim1 → dim2).

> **Cross-reference**: This section depends on the statistics foundation in §8 (OPT-004) — specifically file-level column stats for row-group pruning and histograms for selectivity estimation. The `DynamicFilterSink` in the native reader pipeline is defined in [native-execution.md](./native-execution.md). Filter delivery uses the `RuntimeFilter` Protobuf message added to `TaskRequest` (see [query-execution.md](./query-execution.md)).

### 9.5 Phase 2 — Bloom Filter Runtime Filter

For high-cardinality join keys (NDV > 1000), min/max range filters become too broad. Phase 2 adds Bloom filter generation on the build side and multi-level application on the probe side.

#### 9.5.1 Bloom Filter Generation

```pseudocode
class BloomFilterGenerator:
    function onBuildSideComplete(buildSideData: RecordBatch[]):
        joinKeys = extractJoinKeys(buildSideData)

        if joinKeys.ndv > 1000:
            bloomFilter = BloomFilter(
                expectedItems = joinKeys.ndv,
                fpp           = config.dpp.bloomFilterFpp   // default 0.01
            )
            for key in joinKeys:
                bloomFilter.add(key)
            return RuntimeFilter(type = BLOOM, filter = bloomFilter)

        if joinKeys.ndv <= 100:
            return RuntimeFilter(type = IN_LIST, values = joinKeys.distinct())

        return RuntimeFilter(type = MIN_MAX, min = joinKeys.min(), max = joinKeys.max())
```

#### 9.5.2 Multi-Level Filter Application

Bloom filters enable three levels of pruning beyond partition-level:

```pseudocode
class BloomFilterApplicator:
    function applyToScan(scan: TableScan, filter: RuntimeFilter):
        // Level 1: Partition pruning (same as Phase 1 min/max)
        if scan.table.isPartitioned AND filter.column == scan.table.partitionKey:
            scan.setPartitions(scan.partitions.filter(p => filter.mightContain(p.value)))

        // Level 2: Row group pruning (uses OPT-004 file-level column stats from §8)
        for file in scan.files:
            for rowGroup in file.rowGroups:
                stats = rowGroup.columnStats(filter.column)
                if NOT filter.overlaps(stats.min, stats.max):
                    rowGroup.skip()

        // Level 3: Row-level filtering (native DynamicFilterSink)
        scan.addNativeFilter(RuntimeRowFilter(filter))
```

The native `DynamicFilterSink` (see [native-execution.md](./native-execution.md)) is activated to receive the serialized Bloom filter and apply row-level filtering during scan execution.

#### 9.5.3 Bloom Filter Trade-offs

| Aspect | Min/Max (Phase 1) | Bloom Filter (Phase 2) |
|--------|-------------------|----------------------|
| Selectivity | Low for high-cardinality keys | High — false positive rate configurable (default 1%) |
| Memory | Negligible (2 scalars) | ~1.2 bytes per element × NDV |
| Serialization | Trivial | Bloom filter serialized via `RuntimeFilter` Protobuf |
| Pruning levels | Partition only | Partition + row group + row level |

### 9.6 Phase 3 — Cross-Stage Filter Propagation

Phase 1-2 filters apply within a single stage boundary. Phase 3 enables filter propagation across shuffle boundaries, allowing a build stage's filter to reach a probe stage scheduled on different Workers.

#### 9.6.1 Propagation Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Coordinator                                │
│                                                                   │
│  ┌─────────────────┐     register      ┌──────────────────────┐  │
│  │ Build Stage      │────────────────▶ │ RuntimeFilterRegistry │  │
│  │ (completed)      │                  │ (stageId → filter)    │  │
│  └─────────────────┘                  └──────────┬───────────┘  │
│                                                   │ broadcast    │
│                                    ┌──────────────┼──────────┐  │
│                                    ▼              ▼          ▼  │
│                               Worker-1       Worker-2    Worker-N │
│                               (probe stage tasks apply filter)   │
└──────────────────────────────────────────────────────────────────┘
```

#### 9.6.2 Propagation Pseudocode

```pseudocode
// Coordinator side: register filter after build stage completes
function onBuildStageComplete(buildStageId, probeStageId, filter: RuntimeFilter):
    coordinator.runtimeFilterRegistry.register(probeStageId, filter)

    // Broadcast to all Workers assigned to probe stage
    for worker in getWorkersForStage(probeStageId):
        worker.applyRuntimeFilter(probeStageId, filter)

// Worker side: apply filter before scan starts
function onRuntimeFilterReceived(stageId, filter: RuntimeFilter):
    activeTasks = getTasksForStage(stageId)
    for task in activeTasks:
        if task.state == WAITING_FOR_SPLITS OR task.state == SCANNING:
            task.scanOperator.applyDynamicFilter(filter)
```

#### 9.6.3 Cross-Stage Trade-offs

| Aspect | Same-Stage (Phase 1-2) | Cross-Stage (Phase 3) |
|--------|----------------------|---------------------|
| Scope | Build and probe in same stage | Build and probe in different stages separated by shuffle |
| Latency | No network hop for filter | Filter broadcast adds ~10-50 ms network latency |
| Complexity | Low — filter applied locally | Medium — Coordinator mediates, Workers must handle late-arriving filters |
| Benefit | Moderate — applies to co-located joins | High — applies to the common case of shuffled star-schema joins |

---



## 10. Observability Integration

This section defines the metrics and tracing integration points for the query optimization layer. For the full observability design, see [observability.md](./observability.md).

### 10.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.optimizer.rules.applied` | CBO/RBO Engine | Optimization rules applied per query |
| `pangrova.optimizer.optimization.latency` | Planner | Total optimization time |
| `pangrova.optimizer.cache.hit_rate` | Result Cache | Query result cache hit ratio |
| `pangrova.optimizer.cache.evictions` | Result Cache | Cache eviction count |
| `pangrova.optimizer.dynamic_filter.generated` | DPP Engine | Dynamic filters generated |
| `pangrova.optimizer.dynamic_filter.timeout` | DPP Engine | Dynamic filter timeout count |
| `pangrova.optimizer.statistics.fetch.latency` | Statistics Collector | Statistics fetch latency |

### 10.2 Tracing Spans

From the span hierarchy defined in [14 §8.2](./observability.md):

- `query.optimize` — Top-level optimization span (Coordinator, INTERNAL)
  - `query.optimize.rbo` — Rule-based optimization phase
  - `query.optimize.cbo` — Cost-based optimization phase
  - `query.optimize.statistics` — Statistics collection phase

### 10.3 Log Events

- Optimization rule application details (DEBUG)
- Cache hit/miss events with cache key fingerprint (DEBUG)
- Statistics staleness warnings (WARN)
- Dynamic filter generation and propagation events (DEBUG)

See [14 §7](./observability.md) for structured logging format.



> **Cross-reference (§2.2 incrementalRefresh)**: The `getChanges()` method in INCREMENTAL MV refresh delegates to [streaming.md §3](./streaming.md) for change detection — specifically `IncrementalScanPlanner` (Iceberg snapshot diff) or `ChangelogReader` (Paimon changelog). See 10 §3 for the authoritative CDC/incremental query design.

---



## 11. Distribution-Aware CBO Cost Model (OPT-011)

This section extends the `PangrovaCostModel` (§6) with data distribution property tracking and topology-aware network costing, enabling the optimizer to eliminate unnecessary shuffles for co-partitioned tables and select optimal join strategies based on actual data layout.

### 11.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Distribution-Aware CBO                          │
│                                                                  │
│  ConnectorMetadata                                               │
│    .getTableDistribution()                                       │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────────┐    ┌──────────────────────────────────┐    │
│  │ DataDistribution │───▶│ DistributionAwareCostModel       │    │
│  │  • partitioning  │    │  extends PangrovaCostModel          │    │
│  │  • sortOrder     │    │                                    │    │
│  │  • locality      │    │  estimateExchangeCost()            │    │
│  └─────────────────┘    │  estimateJoinCost()                │    │
│                          │    ├─ co-located? → no shuffle     │    │
│                          │    ├─ pre-sorted? → merge join     │    │
│                          │    └─ default → hash join + shuffle│    │
│                          └──────────────────────────────────┘    │
│                                     │                            │
│                                     ▼                            │
│                          ┌──────────────────────┐                │
│                          │ TopologyAwareNetwork  │                │
│                          │  local = 0            │                │
│                          │  rackLocal = 10       │                │
│                          │  crossRack = 100      │                │
│                          └──────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: The current `PangrovaCostModel` uses three static unit costs (CPU 1.0/row, I/O 10.0/byte, Network 100.0/byte) and does not account for data distribution properties. This causes unnecessary shuffles when tables are already co-partitioned on join keys, and misses merge join opportunities when data is pre-sorted.

### 11.2 Pseudocode

**Distribution Property Model**:

```pseudocode
class DataDistribution:
    partitioning: Partitioning     // HASH(columns), RANGE(columns), SINGLE, ANY
    sortOrder: List<SortColumn>    // sorted columns with ASC/DESC
    locality: Locality             // NODE_LOCAL, RACK_LOCAL, REMOTE

enum Partitioning:
    HASH(columns: List<Column>, bucketCount: int)
    RANGE(columns: List<Column>)
    SINGLE    // all data on one node
    ANY       // unknown distribution
```

**Distribution-Aware Cost Model**:

```pseudocode
class DistributionAwareCostModel extends PangrovaCostModel:
    function estimateExchangeCost(
        requiredDist: Partitioning,
        currentDist: DataDistribution,
        dataSize: long
    ):
        if currentDist.partitioning.satisfies(requiredDist):
            return Cost(cpu = 0, io = 0, network = 0)  // no shuffle needed

        if currentDist.partitioning.isCompatible(requiredDist):
            mismatchRatio = computeMismatchRatio(currentDist, requiredDist)
            return Cost(network = dataSize * mismatchRatio * networkCostPerByte)

        return Cost(network = dataSize * networkCostPerByte)  // full shuffle

    function estimateJoinCost(join: JoinNode):
        leftDist = join.left.getDistribution()
        rightDist = join.right.getDistribution()

        if leftDist.partitioning.isCoLocated(rightDist.partitioning, join.keys):
            return Cost(cpu = join.leftRows + join.rightRows, network = 0)

        if leftDist.sortOrder.satisfies(join.keys)
           AND rightDist.sortOrder.satisfies(join.keys):
            return Cost(cpu = join.leftRows + join.rightRows,
                        network = estimateExchangeCost(...))

        return super.estimateJoinCost(join)  // default hash join
```

**Topology-Aware Network Cost**:

```pseudocode
class TopologyAwareNetworkCost:
    localCost = 0.0           // same Worker
    rackLocalCost = 10.0      // same rack
    crossRackCost = 100.0     // cross rack

    function getNetworkCostPerByte(source: Worker, target: Worker):
        if source == target:       return localCost
        if source.rackId == target.rackId: return rackLocalCost
        return crossRackCost
```

**Connector Distribution Metadata Extension**:

```pseudocode
interface ConnectorMetadata:
    // New: return distribution properties for a table
    function getTableDistribution(table: TableHandle): DataDistribution
        // Iceberg: extract partition spec (bucket, identity, truncate)
        // Paimon: extract bucket key and sort order
```

### 11.3 Trade-offs

| Aspect | Current (Static Cost) | Distribution-Aware |
|--------|----------------------|-------------------|
| Performance | Unnecessary shuffles for co-partitioned tables; missed merge join | Eliminates redundant shuffles; selects optimal join strategy |
| Complexity | Simple three-dimensional cost model | Medium — distribution tracking, connector metadata integration |
| Risk | Low | Low — distribution-unaware fallback produces same plan as current |
| Accuracy | Static costs ignore cluster topology | Topology-aware costs reflect real network latency |

### 11.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Distribution Property Model | Define `DataDistribution` class. Implement `ConnectorMetadata.getTableDistribution()` for Iceberg (partition spec → distribution). |
| 2 | Co-Located Join Detection | Extend `PangrovaCostModel` to detect co-located joins and eliminate unnecessary Exchange operators. |
| 3 | Sort Order Awareness | Detect existing sort order and prefer merge join when data is pre-sorted. |
| 4 | Topology-Aware Costing | Integrate rack topology into network cost estimation. Requires Worker rack ID in heartbeat. |

> **Cross-reference**: This optimization depends on the statistics foundation in §8 (OPT-004) for file-level column stats. The `ConnectorMetadata` SPI extension aligns with [connectors.md](./connectors.md). Topology-aware costing integrates with Worker Performance Scoring (OPT-013 in [distributed.md](./distributed.md)) which adds rack ID to Worker heartbeat.


---



## 12. CTE Materialization Strategy (OPT-019)

Current CTE execution uses Calcite expansion: every reference retains its real source
relations through Substrait serialization. Reference counting and cost hints are advisory;
they do not publish temporary tables or advertise a Native materialization capability.
The earlier synthetic temp-scan rewrite was removed because no producer, Native reader,
or durable query-scoped ownership contract existed for it. `CteCompilationContractTest`
checks repeated and single-use SQL on the same compiler; this is a compilation contract,
not distributed execution evidence.

The remaining materialization architecture and pseudocode below are an unimplemented
design. A future implementation must use query-scoped Native producer/consumer stages,
canonical exchange contracts, resource admission, complete-result semantics, cancellation
and recovery. It must not replace source subtrees before those stages are wired.

### 12.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              CTE Materialization Decision                         │
│                                                                  │
│  WITH cte AS (expensive_subquery)                                │
│  SELECT * FROM cte t1 JOIN cte t2 ON ...                         │
│                                                                  │
│  Calcite Plan Analysis:                                          │
│    referenceCount(cte) = 2                                       │
│    cteCost = estimateCost(cte)                                   │
│                                                                  │
│    inlineCost = cteCost × 2        (compute twice)               │
│    materializeCost = cteCost + overhead  (compute once + store)  │
│                                                                  │
│    materializeCost < inlineCost?                                 │
│      YES → materialize to Worker memory as virtual table         │
│      NO  → inline (Calcite default)                              │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Calcite defaults to inlining CTEs — each reference re-computes the subquery. For `WITH cte AS (expensive_agg) SELECT * FROM cte t1 JOIN cte t2`, the expensive aggregation runs twice.

### 12.2 Pseudocode

**CTE Materialization Rule**:

```pseudocode
class CTEMaterializationRule extends CalciteRule:
    function onMatch(call: RelOptRuleCall):
        cte = call.rel(0)
        referenceCount = countReferences(cte, call.queryPlan)

        if referenceCount <= 1:
            return  // inline — no benefit

        cteCost = estimateCost(cte)
        materializeCost = cteCost + MATERIALIZATION_OVERHEAD
        inlineCost = cteCost * referenceCount

        if materializeCost < inlineCost:
            materializedPlan = createMaterializeNode(cte)
            for ref in cte.references:
                ref.replaceWith(TableScan(materializedPlan.outputTable))
```

**Materialization Execution**:

```pseudocode
class CTEMaterializer:
    function materialize(cte: CTENode):
        stage = createStage(cte.plan)
        stage.outputMode = MATERIALIZE_IN_MEMORY
        stage.ttl = query.remainingTimeout
        virtualTable = registerVirtualTable(stage.output)
        return virtualTable
```

### 12.3 Trade-offs

| Aspect | Inline (Current Default) | Cost-Based Materialization |
|--------|--------------------------|---------------------------|
| Performance | Multi-referenced CTEs computed N times | Expensive CTEs computed once; N-1 redundant computations eliminated |
| Complexity | Simple — Calcite default | Medium — cost estimation, materialization stage, virtual table |
| Risk | Repeated computation retains ordinary query execution risks | Requires correctness and failure validation; cost benefit alone is insufficient |
| Memory | None | Materialized results held in Worker memory for query duration |

### 12.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Documentation | Document current CTE behavior (inline) in this section. |
| 2 | Reference Counting | CTE reference counting in Calcite plan analysis. Log multi-referenced CTEs. |
| 3 | Cost-Based Materialization | `CTEMaterializationRule` with cost-based decision. |

> **Cross-reference**: the unimplemented materialization design would use the existing
> `PangrovaCostModel` (§6) and Native Worker resource admission from
> [resource-management.md](./resource-management.md). No temporary-table storage is
> currently created by CTE planning. ANTLR4 `WITH` support is documented in
> [query-execution.md](./query-execution.md).
