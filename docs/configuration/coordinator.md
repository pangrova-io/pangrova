---
title: "Coordinator"
description: "Coordinator configuration keys and JVM system properties"
---

# Coordinator

All coordinator-side configuration keys, as defined by `CoordinatorOptions`
in the `pangrova-config` module. The bundled defaults ship in
`pangrova-config/src/main/resources/coordinator.yml`.

> **Protocol mapping.** The Coordinator exposes an HTTP (REST) interface
> (control plane). For the full protocol mapping, see
> [System Overview §4.6](../architecture/overview.md#46-canonical-protocol-mapping).


## coordinator

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.id` | string | `"coordinator-1"` | Unique identifier for this coordinator instance. Used in HA leader election |
| `coordinator.host` | string | `"0.0.0.0"` | Bind address for the coordinator HTTP server |
| `coordinator.port` | int | `8080` | Listen port for the coordinator HTTP server |
| `coordinator.role` | string | `"COMBINED"` | Deployment role: `QUERY` (serve queries only), `MAINTENANCE` (background tasks only), or `COMBINED` (both) |

## coordinator.ha

Controls high-availability leader election via a JDBC lease store.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.ha.enabled` | bool | `false` | Enable coordinator high-availability mode |
| `coordinator.ha.jdbc-url` | string | — | JDBC URL for the HA coordination state store. Required when HA is enabled |
| `coordinator.ha.ddl-leader-routing` | bool | `false` | When `true`, DDL statements are routed exclusively to the leader coordinator (HTTP 307 redirect). When `false` (default), DDL can execute on any coordinator — metadata consistency is guaranteed by the connector's own concurrency control and cross-node invalidation propagation |

### Distributed lease backend (JVM system properties)

By default the leader-election lease is stored in the HA JDBC database. To run
leader election on a fault-tolerant ZooKeeper quorum instead, set these JVM
system properties (`-D...`) on every coordinator:

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.coordination.lease.backend` | `jdbc` | Lease backend: `jdbc` (single DB) or `zookeeper` (quorum) |
| `pangrova.coordination.lease.zookeeper.connectString` | — | Required when backend=`zookeeper`. ZK ensemble, e.g. `zk1:2181,zk2:2181,zk3:2181` |
| `pangrova.coordination.lease.zookeeper.basePath` | `/pangrova/leases` | Parent znode under which lease nodes are stored |

## coordinator.query

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.query.timeout` | duration | `"5m"` | Maximum query execution time before timeout |
| `coordinator.query.max-concurrent` | int | `100` | Maximum number of concurrently executing queries |

## coordinator.worker

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.worker.heartbeat-timeout` | duration | `"30s"` | Duration after which a worker is considered dead if no heartbeat received |
| `coordinator.worker.max-retry` | int | `3` | Maximum number of task dispatch retries to a worker |

---

## State Store (`state-store`)

The state store persists distributed state (HA leases, task journals, DDL
locks). `IN_MEMORY` loses all state on restart and is intended for
development and tests.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `state-store.type` | enum | `IN_MEMORY` | Global backend type for all SPI state stores: `IN_MEMORY` or `JDBC` |

---

## Metadata (`metadata`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `metadata.enabled` | bool | `true` | Enable the metadata module |
| `metadata.jdbc-url` | string | — | JDBC URL for the metadata catalog store |
| `metadata.cache.l1-max-size` | int | `10000` | Maximum number of entries in the L1 (on-heap) metadata cache |
| `metadata.cache.l1-ttl` | duration | `"60s"` | Time-to-live for L1 metadata cache entries |

---

## Feature Flags

Feature flags control which subsystem modules are active. They are shared
between `coordinator.yml` and `worker.yml` where noted. All default to `true`
except `security.enabled`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `governance.enabled` | bool | `true` | Enable the governance module |
| `resilience.enabled` | bool | `true` | Enable the resilience module (circuit breaker, retry) |
| `streaming.enabled` | bool | `true` | Enable the streaming module |
| `tenant.enabled` | bool | `true` | Enable the multi-tenant module |
| `indexing.enabled` | bool | `true` | Enable the indexing module |
| `operations.enabled` | bool | `true` | Enable the operations module (compaction, maintenance) |
| `observability.enabled` | bool | `true` | Enable the observability module |
| `security.enabled` | bool | `false` | Enable the security module. When `true`, `security.auth.type` must be configured |
| `security.auth.type` | string | — | Authenticator type when security is enabled: `OAUTH2`, `LDAP`, or `NOOP` (development only) |

---

## Coordinator JVM System Properties

Additional coordinator behavior is tuned via JVM system properties
(`-D...` on the coordinator launch):

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.http.max-requests-per-minute` | `100` | HTTP request rate limit applied by the coordinator request-limit filter |
| `pangrova.shuffle.manager` | (auto) | Shuffle provider name (`native`, `celeborn`, ...). When unset, the highest-durability provider on the classpath is selected automatically |
| `pangrova.index.basePath` | `${java.io.tmpdir}/pangrova/indexes` | Base directory for index build artifacts |
| `pangrova.cache.result.redis-url` | — | Redis URL to enable the external query result cache |
| `pangrova.cache.result.ttl-seconds` | `3600` | TTL for externally cached query results |
| `pangrova.cache.result.max-entry-bytes` | `10485760` | Maximum size (bytes) of a single externally cached result |

---

## Example

Minimal production `coordinator.yml` (only real keys):

```yaml
coordinator:
  id: "${HOSTNAME:-coordinator-1}"
  host: "0.0.0.0"
  port: 8080
  ha:
    enabled: true
    jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST:-localhost}:5432/pangrova_ha"
  query:
    timeout: "10m"
    max-concurrent: 200
  worker:
    heartbeat-timeout: "30s"
    max-retry: 3

state-store:
  type: JDBC

metadata:
  enabled: true
  jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST:-localhost}:5432/pangrova_meta"
  cache:
    l1-max-size: 10000
    l1-ttl: "60s"

governance:
  enabled: true
resilience:
  enabled: true
streaming:
  enabled: true
tenant:
  enabled: true
indexing:
  enabled: true
operations:
  enabled: true
observability:
  enabled: true
security:
  enabled: false
```

**See Also:** [Configuration model](index.md), [Worker](worker.md), [Quick Reference](reference.md)
