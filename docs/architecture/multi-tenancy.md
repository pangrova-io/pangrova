---
title: "Multi-Tenancy"
sidebar_label: "Multi-Tenancy"
---

# Pangrova — Multi-Tenancy

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The tenant model, resource isolation, and quota management.

## 1. Multi-Tenancy Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Multi-Tenancy Architecture                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Isolation Levels:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Level              │ Isolation Method            │ Use Case            │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Namespace          │ Catalog/Schema isolation    │ Lightweight tenancy │   │
│  │  Resource Pool      │ Resource pool isolation     │ Resource guarantee  │   │
│  │  Worker Group       │ Physical node isolation     │ Strong isolation    │   │
│  │  Cluster            │ Dedicated cluster           │ Full isolation      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Recommended: Namespace + Resource Pool (balances isolation and utilization)   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Tenant Model

### 2.1 Tenant Definition

```java
/**
 * Tenant
 */
public class Tenant {
    private final String tenantId;
    private final String name;
    private final TenantStatus status;

    // Resource quota
    private final ResourceQuota quota;

    // Namespace
    private final List<String> allowedCatalogs;
    private final List<String> allowedSchemas;

    // Configuration
    private final Map<String, String> properties;

    // Metadata
    private final Instant createdAt;
    private final String createdBy;
}

/**
 * Resource quota
 */
public class ResourceQuota {
    private final long maxMemoryBytes;           // Maximum memory
    private final int maxConcurrentQueries;      // Maximum concurrent queries
    private final int maxQueuedQueries;          // Maximum queued queries
    private final long maxCpuTimePerQueryMs;     // Maximum CPU time per query
    private final long maxScanBytesPerQuery;     // Maximum scan bytes per query
    private final long maxDailyQueryCount;       // Daily query count limit
    private final long maxDailyScanBytes;        // Daily scan bytes limit
    private final long maxStorageBytes;          // Storage quota
}
```

### 2.2 Tenant Storage



```sql
-- Tenant table
CREATE TABLE tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE, SUSPENDED, DELETED

    -- Resource quota
    max_memory_bytes BIGINT DEFAULT 10737418240,   -- 10GB
    max_concurrent_queries INT DEFAULT 10,
    max_queued_queries INT DEFAULT 100,
    max_cpu_time_per_query_ms BIGINT DEFAULT 3600000,  -- 1 hour
    max_scan_bytes_per_query BIGINT DEFAULT 107374182400,  -- 100GB
    max_daily_query_count BIGINT DEFAULT 10000,
    max_daily_scan_bytes BIGINT DEFAULT 1099511627776,  -- 1TB
    max_storage_bytes BIGINT DEFAULT 1099511627776,  -- 1TB

    -- Namespace
    allowed_catalogs JSON,                         -- ["iceberg", "paimon"]
    allowed_schemas JSON,                          -- ["iceberg.tenant_a.*"]

    -- Resource pool
    resource_pool VARCHAR(64),

    -- Configuration
    properties JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Tenant-user mapping
CREATE TABLE tenant_users (
    tenant_id VARCHAR(64) NOT NULL,
    user_principal VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'MEMBER',    -- OWNER, ADMIN, MEMBER
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_principal)
);
```

---

## 3. Resource Isolation

### 3.1 Resource Pool Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Resource Pool Architecture                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Worker Cluster                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Resource Pool: default (50% cluster resources)                  │   │   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │   │
│  │  │  │Worker-1 │  │Worker-2 │  │Worker-3 │  │Worker-4 │            │   │   │
│  │  │  │ 50% CPU │  │ 50% CPU │  │ 50% CPU │  │ 50% CPU │            │   │   │
│  │  │  │ 50% Mem │  │ 50% Mem │  │ 50% Mem │  │ 50% Mem │            │   │   │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Resource Pool: premium (30% cluster resources)                  │   │   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │   │
│  │  │  │Worker-1 │  │Worker-2 │  │Worker-3 │  │Worker-4 │            │   │   │
│  │  │  │ 30% CPU │  │ 30% CPU │  │ 30% CPU │  │ 30% CPU │            │   │   │
│  │  │  │ 30% Mem │  │ 30% Mem │  │ 30% Mem │  │ 30% Mem │            │   │   │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Resource Pool: batch (20% cluster resources)                    │   │   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │   │
│  │  │  │Worker-1 │  │Worker-2 │  │Worker-3 │  │Worker-4 │            │   │   │
│  │  │  │ 20% CPU │  │ 20% CPU │  │ 20% CPU │  │ 20% CPU │            │   │   │
│  │  │  │ 20% Mem │  │ 20% Mem │  │ 20% Mem │  │ 20% Mem │            │   │   │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Resource Pool Configuration

```java
/**
 * Resource pool
 */
public class ResourcePool {
    private final String poolId;
    private final String name;

    // Resource configuration
    private final double cpuShare;               // CPU share (0-1)
    private final long memoryBytes;              // Memory quota
    private final int maxConcurrentQueries;      // Maximum concurrency
    private final int queueCapacity;             // Queue capacity

    // Scheduling configuration
    private final SchedulingPolicy policy;       // FIFO, FAIR, PRIORITY
    private final int priority;                  // Pool priority
    private final boolean preemptible;           // Whether preemptible

    // Elasticity configuration
    private final double minShare;               // Minimum guaranteed share
    private final double maxShare;               // Maximum available share
    private final boolean elasticEnabled;        // Whether elasticity is enabled
}

/**
 * Resource pool manager
 */
public class ResourcePoolManager {

    private final Map<String, ResourcePool> pools;
    private final Map<String, PoolState> poolStates;

    /**
     * Assign a resource pool to a query
     */
    public ResourcePool assignPool(QueryContext context) {
        // 1. Get the resource pool configured for the tenant
        String tenantPool = context.getTenant().getResourcePool();
        if (tenantPool != null && pools.containsKey(tenantPool)) {
            return pools.get(tenantPool);
        }

        // 2. Fallback by query type
        // Note: ResourcePool is tenant-level isolation (§3.1). Query-type scheduling
        // (interactive/batch) is handled by ResourceQueue in 06 §8.
        // Here we only map to named pools defined in §6 config.
        if (context.isBatch()) {
            return pools.get("batch");
        }

        // 3. Default pool
        return pools.get("default");
    }

    /**
     * Check whether the resource pool has available resources
     */
    public boolean hasAvailableResources(ResourcePool pool, ResourceRequirement requirement) {
        PoolState state = poolStates.get(pool.getPoolId());

        return state.getRunningQueries() < pool.getMaxConcurrentQueries() &&
               state.getUsedMemory() + requirement.getMemory() <= pool.getMemoryBytes();
    }

    /**
     * Elastic resource borrowing
     */
    public long borrowResources(ResourcePool pool, long requestedBytes) {
        if (!pool.isElasticEnabled()) {
            return 0;
        }

        // Borrow resources from idle pools
        long available = calculateAvailableElasticResources();
        long borrowed = Math.min(requestedBytes, available);

        poolStates.get(pool.getPoolId()).addBorrowedMemory(borrowed);
        return borrowed;
    }
}
```

---

## 4. Quota Management

### 4.1 Quota Check

```java
/**
 * Quota checker
 */
public class QuotaChecker {

    private final TenantStore tenantStore;
    private final UsageStore usageStore;

    /**
     * Check quota before query submission
     */
    public QuotaCheckResult checkBeforeSubmit(QueryContext context) {
        Tenant tenant = context.getTenant();
        ResourceQuota quota = tenant.getQuota();
        TenantUsage todayUsage = usageStore.getTodayUsage(tenant.getTenantId());

        List<String> violations = new ArrayList<>();

        // Check concurrent query count
        int runningQueries = getRunningQueryCount(tenant.getTenantId());
        if (runningQueries >= quota.getMaxConcurrentQueries()) {
            violations.add("Concurrent query limit exceeded: " +
                           runningQueries + "/" + quota.getMaxConcurrentQueries());
        }

        // Check queued query count
        int queuedQueries = getQueuedQueryCount(tenant.getTenantId());
        if (queuedQueries >= quota.getMaxQueuedQueries()) {
            violations.add("Query queue limit exceeded: " +
                           queuedQueries + "/" + quota.getMaxQueuedQueries());
        }

        // Check daily query count
        if (todayUsage.getQueryCount() >= quota.getMaxDailyQueryCount()) {
            violations.add("Daily query count limit exceeded: " +
                           todayUsage.getQueryCount() + "/" + quota.getMaxDailyQueryCount());
        }

        // Check daily scan bytes
        if (todayUsage.getScanBytes() >= quota.getMaxDailyScanBytes()) {
            violations.add("Daily scan bytes limit exceeded: " +
                           formatBytes(todayUsage.getScanBytes()) + "/" +
                           formatBytes(quota.getMaxDailyScanBytes()));
        }

        return violations.isEmpty()
            ? QuotaCheckResult.pass()
            : QuotaCheckResult.fail(violations);
    }

    /**
     * Check quota during query execution
     *
     *
     * Field mapping: quota fields → QueryStats fields
     *   maxCpuTimePerQueryMs  → stats.getCpuTimeMs()
     *   maxScanBytesPerQuery  → stats.getInputBytes()
     *
     * NOTE: "scanBytes" (raw bytes scanned from storage, pre-filter) vs "inputBytes"
     * (bytes processed by operators, post-filter) are semantically different.
     * Current implementation uses inputBytes as a conservative lower bound.
     * For precise scan-level enforcement, a dedicated stats.getScanBytes() field
     * should be added to QueryStats (tracked separately from inputBytes).
     */
    public void checkDuringExecution(QueryContext context, QueryStats stats) {
        Tenant tenant = context.getTenant();
        ResourceQuota quota = tenant.getQuota();

        // Check per-query CPU time
        // Error code: TENANT_QUOTA_EXCEEDED(1602) — see resilience.md §2.2
        if (stats.getCpuTimeMs() > quota.getMaxCpuTimePerQueryMs()) {
            throw new TenantQuotaExceededException(
                PangrovaErrorCode.TENANT_QUOTA_EXCEEDED, "Query CPU time limit exceeded");
        }

        // Check per-query scan bytes (quota.scanBytes maps to stats.inputBytes)
        if (stats.getInputBytes() > quota.getMaxScanBytesPerQuery()) {
            throw new TenantQuotaExceededException(
                PangrovaErrorCode.TENANT_QUOTA_EXCEEDED, "Query scan bytes limit exceeded");
        }
    }
}
```

### 4.2 Quota Alerting

```java
/**
 * Quota alerting service
 */
public class QuotaAlertService {

    private static final double WARNING_THRESHOLD = 0.8;
    private static final double CRITICAL_THRESHOLD = 0.95;

    @Scheduled(fixedRate = 60000)
    public void checkQuotaUsage() {
        for (Tenant tenant : tenantStore.getAllActiveTenants()) {
            TenantUsage usage = usageStore.getTodayUsage(tenant.getTenantId());
            ResourceQuota quota = tenant.getQuota();

            // Check usage ratio for each quota item
            checkAndAlert(tenant, "daily_queries",
                          usage.getQueryCount(), quota.getMaxDailyQueryCount());
            checkAndAlert(tenant, "daily_scan_bytes",
                          usage.getScanBytes(), quota.getMaxDailyScanBytes());
            checkAndAlert(tenant, "storage",
                          usage.getStorageBytes(), quota.getMaxStorageBytes());
        }
    }

    private void checkAndAlert(Tenant tenant, String metric, long used, long limit) {
        double ratio = (double) used / limit;

        if (ratio >= CRITICAL_THRESHOLD) {
            alertService.sendAlert(Alert.builder()
                .severity(Severity.CRITICAL)
                .title("Quota Critical: " + metric)
                .message(String.format("Tenant %s %s usage at %.1f%%",
                         tenant.getName(), metric, ratio * 100))
                .build());
        } else if (ratio >= WARNING_THRESHOLD) {
            alertService.sendAlert(Alert.builder()
                .severity(Severity.WARNING)
                .title("Quota Warning: " + metric)
                .message(String.format("Tenant %s %s usage at %.1f%%",
                         tenant.getName(), metric, ratio * 100))
                .build());
        }
    }
}
```

---

## 5. Tenant Management SQL

```sql
-- Create a tenant
CREATE TENANT finance_team
WITH (
    max_memory = '20GB',
    max_concurrent_queries = 20,
    max_daily_scan_bytes = '10TB',
    resource_pool = 'premium',
    allowed_catalogs = ARRAY['iceberg'],
    allowed_schemas = ARRAY['iceberg.finance.*']
);

-- Modify tenant quota
ALTER TENANT finance_team SET max_concurrent_queries = 30;
ALTER TENANT finance_team SET resource_pool = 'default';

-- Suspend a tenant
ALTER TENANT finance_team SET status = 'SUSPENDED';

-- Resume a tenant
ALTER TENANT finance_team SET status = 'ACTIVE';

-- Delete a tenant
DROP TENANT finance_team;

-- View tenants
SHOW TENANTS;
SHOW TENANT finance_team;

-- View tenant usage
SELECT * FROM system.tenant_usage WHERE tenant_id = 'finance_team';

-- Add a user to a tenant
GRANT TENANT finance_team TO USER alice WITH ROLE ADMIN;
REVOKE TENANT finance_team FROM USER bob;

-- View tenant users
SHOW USERS IN TENANT finance_team;
```

---

## 6. Configuration Summary

```yaml
# multi-tenancy-config.yaml
multi-tenancy:
  enabled: true

  # Default quota
  default-quota:
    max-memory: 10GB
    max-concurrent-queries: 10
    max-queued-queries: 100
    max-cpu-time-per-query: 1h
    max-scan-bytes-per-query: 100GB
    max-daily-query-count: 10000
    max-daily-scan-bytes: 1TB
    max-storage: 1TB

  # Resource pool configuration
  resource-pools:
    default:
      cpu-share: 0.5
      memory: 50%
      max-concurrent-queries: 100
      scheduling-policy: FAIR

    premium:
      cpu-share: 0.3
      memory: 30%
      max-concurrent-queries: 50
      scheduling-policy: PRIORITY
      priority: 10

    batch:
      cpu-share: 0.2
      memory: 20%
      max-concurrent-queries: 20
      scheduling-policy: FIFO
      preemptible: true

  # Alert configuration
  alerts:
    quota-warning-threshold: 0.8
    quota-critical-threshold: 0.95
```


---

## 7. Cross-Reference: Result Cache Tenant Isolation

> **See also:** [`query-optimization.md`](query-optimization.md) — Section 3.2 (QueryFingerprintCalculator) and Section 3.3 (Cache Invalidation Triggers)

The query result cache enforces tenant isolation by including `tenant_id` as a mandatory dimension in the cache fingerprint. This guarantees that:

- Cached query results from one tenant are **never** served to another tenant, even for semantically identical queries.
- Each tenant's cache entries are independently managed, supporting per-tenant cache eviction and quota enforcement.
- Role assignment changes within a tenant trigger invalidation of the affected user's cache entries, maintaining consistency with the tenant's RBAC configuration.

This cache-level tenant isolation complements the namespace isolation (Catalog/Schema) and resource pool isolation defined in this document, forming a complete multi-tenancy boundary that spans compute, storage, and caching layers.

---



## 8. Observability Integration

This section defines the metrics and tracing integration points for the multi-tenancy layer. For the full observability design, see [observability.md](./observability.md).

### 8.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.tenant.queries.running` | TenantQuotaManager | Running queries per tenant |
| `pangrova.tenant.memory.used` | TenantResourceIsolation | Memory usage per tenant |
| `pangrova.tenant.memory.quota` | TenantQuotaManager | Memory quota per tenant |
| `pangrova.tenant.cpu.used` | TenantResourceIsolation | CPU usage per tenant |
| `pangrova.tenant.throttle.count` | TenantQuotaManager | Throttled requests per tenant |
| `pangrova.tenant.queue.pending` | TenantResourceQueue | Pending requests per tenant |

All tenant metrics include a `tenant.id` attribute label for per-tenant filtering.

### 8.2 Tracing Spans

Tenant context is propagated as span attributes on all query-related spans:

- `tenant.id` attribute added to `query.execute` span
- `tenant.resource_group` attribute added to `task.execute` span

### 8.3 Log Events

- Tenant quota exceeded events (WARN)
- Tenant throttle activation/deactivation events (INFO)
- Tenant resource isolation boundary violations (ERROR)

See [14 §7](./observability.md) for structured logging format.







## 9. Security Integration

Multi-tenancy and security are tightly coupled cross-cutting concerns. This section describes the integration points with [security.md](./security.md).

### 9.1 Tenant Resolution in Authentication Flow

After authentication ([12 §2](./security.md)), the tenant context is resolved from the `tenant_users` table (§2.2):

```
Authenticator.authenticate() → UserIdentity
  → TenantResolver.resolve(UserIdentity.principal) → TenantContext
  → Session binds (UserIdentity + TenantContext)
```

The `TenantResolver` queries `tenant_users` to determine the user's tenant membership and role (OWNER/ADMIN/MEMBER).

### 9.2 Tenant Roles vs Global RBAC Roles

Tenant roles (§2.2 `tenant_users.role`) and global RBAC roles ([12 §3](./security.md) `roles` table) are independent systems:

| Aspect | Tenant Roles (§2.2) | Global RBAC Roles (12 §3) |
|--------|---------------------|---------------------------|
| Scope | Within a single tenant | Cluster-wide |
| Purpose | Tenant administration (quota mgmt, user mgmt) | Data access control (SELECT, INSERT, DDL) |
| Storage | `tenant_users.role` | `user_roles` + `privileges` |
| Values | OWNER, ADMIN, MEMBER | Custom roles (data_analyst, data_engineer, etc.) |

A user's effective permissions = Global RBAC privileges ∩ Tenant namespace restrictions (`allowedCatalogs`/`allowedSchemas`).

### 9.3 Tenant-Level RLS and Column Masking

RLS policies ([12 §4](./security.md)) and column masks ([12 §5](./security.md)) apply within the tenant's namespace boundary. The `tenant_id` is available as `user_attribute('tenant_id')` in policy expressions, enabling tenant-scoped policies.

### 9.4 Tenant Audit

All audit events ([12 §6](./security.md)) include `tenant_id` in the `properties` map, enabling per-tenant audit log filtering. See also [14 §4.4](./observability.md) for resource audit.


## 10. Scheduling Layer Clarification

ResourcePool (§3.2) and ResourceQueue ([06 §8](./resource-management.md)) operate at different scheduling layers:

| Layer | Component | Location | Responsibility |
|-------|-----------|----------|----------------|
| Query Scheduling | ResourceQueue (06 §8) | Coordinator | Queue queries by type (Interactive/Batch/Background), control submission order and timeout. Preemption occurs here (INTERACTIVE preempts BACKGROUND). |
| Resource Allocation | ResourcePool (§3.2) | Worker | Allocate Worker resources by tenant. `SchedulingPolicy` (FIFO/FAIR/PRIORITY) controls task execution order within a pool. `preemptible=true` means the pool's resources can be borrowed by other pools (elastic resources). |

The two layers interact at query submission: Coordinator's ResourceQueue determines when a query can start, then assigns tasks to Workers whose ResourcePool enforces the tenant's resource share.


> **Resource-quota implementation:** `ResourceQuota` is the persisted quota model.
> Current runtime accounting is performed by `TenantResourceTracker`; the
> per-tenant pool model described in the resource-management target design is not
> an implemented class.


> **Catalog-Tenant Binding (§2.1)**: `Tenant.allowedCatalogs` and `CatalogDescriptor.allowedTenants` ([07 §2.3](./metadata.md)) form a bidirectional access control matrix. The authoritative binding is stored in PostgreSQL `tenant_catalog_access` table. Both fields are derived views: `Tenant.allowedCatalogs` is populated by `SELECT catalog_name FROM tenant_catalog_access WHERE tenant_id = ?`, and `CatalogDescriptor.allowedTenants` by the inverse query. Updates go through `TenantService.grantCatalogAccess(tenantId, catalogName)`.


> **Configuration status:** The detailed multi-tenancy YAML in §6 is target design.
> The currently verified runtime surface exposes only `tenant.enabled`; see the
> [configuration key reference](../configuration/reference.md).
