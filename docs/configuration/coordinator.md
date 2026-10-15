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
| `coordinator.internal-auth-secret` | string | — | Shared internal control-plane secret, identical to `worker.internal-auth-secret`. The production process rejects absent, blank or fewer than 32 UTF-8 bytes, independently of client authentication. Inject through the YAML `${PANGROVA_INTERNAL_AUTH_SECRET}` placeholder. |

## coordinator.speculation

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.speculation.enabled` | bool | `false` | Enable the existing straggler detector and speculative task launcher. Copies use issued task authority, winner election and exact-attempt drain receipts; enabling this does not establish worker restart or publication-fencing guarantees. |

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
| `metadata.jdbc-username` | string | `""` | Database username supplied separately from the JDBC URL |
| `metadata.jdbc-password` | string | `""` | Database password supplied separately from the JDBC URL; use an environment placeholder |
| `metadata.cache.l1-max-size` | int | `10000` | Maximum number of entries in the L1 (on-heap) metadata cache |
| `metadata.cache.l1-ttl` | duration | `"60s"` | Time-to-live for L1 metadata cache entries |

Keep credentials out of `metadata.jdbc-url`: supply `metadata.jdbc-username` and
`metadata.jdbc-password` through the existing YAML environment placeholders.
The production `etc/coordinator.yml` requires `PANGROVA_DB_USERNAME` and
`PANGROVA_DB_PASSWORD`; missing variables reject configuration loading. Values
are passed unchanged to the metadata connection pool. Configuration summaries
redact the connection parameters. The bundled development defaults retain empty
credentials for deployments that deliberately use the JDBC driver's default
authentication mode.

---

## Feature Flags

Feature flags control which subsystem modules are active. They are shared
between `coordinator.yml` and `worker.yml` where noted. All default to `true`
except `security.enabled` and `tenant.enabled`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `governance.enabled` | bool | `true` | Enable the governance module |
| `resilience.enabled` | bool | `true` | Enable the resilience module (circuit breaker, retry) |
| `streaming.enabled` | bool | `true` | Enable the streaming module |
| `tenant.enabled` | bool | `false` | Enable the multi-tenant module; requires authenticated user identities |
| `indexing.enabled` | bool | `true` | Enable the indexing module |
| `operations.enabled` | bool | `true` | Enable the operations module (compaction, maintenance) |
| `observability.enabled` | bool | `true` | Enable the observability module |
| `security.enabled` | bool | `false` | Enable the security module. Requires an authoritative PostgreSQL metadata datasource and configured `security.auth.type`; missing/unavailable stores fail startup. |
| `security.auth.type` | string | — | Authenticator type when security is enabled: `OAUTH2`, `LDAP`, or `NOOP` (development only) |
| `security.auth.properties.*` | backend settings | — | OAuth2 requires `issuer-uri` and optionally `audience`; signatures are always verified. LDAP requires `ldap-url`, with kebab-case backend bind/search properties. Unknown keys, signature-bypass settings and properties for a different authenticator are rejected. |

Roles, user-role assignments, explicit identity-group-to-role mappings and grants
must be provisioned in the shared PostgreSQL authority (`pangrova_roles`,
`pangrova_user_roles`, `pangrova_group_roles`, `security_grants`). Identity groups
never implicitly confer a role. `security.grants` configuration is unsupported
and rejected, so restarting a coordinator cannot restore revoked permissions.
Every access check reads current roles and grants; unavailable SQL state fails
closed instead of reusing an earlier permission. RLS and column masking match
the independently resolved role set while retaining the authenticated identity's
original groups and attributes. SQL role/grant administration is not yet exposed
by this configuration surface; provision the persistent authority explicitly.
For storage profiles, provision `VIEW_STORAGE_PROFILE`,
`MANAGE_STORAGE_PROFILE`, or `READ_STORAGE_PROFILE` in `security_grants` with
`object_type = 'STORAGE_PROFILE'`, `catalog_name` equal to the exact profile
name (or `*` for a deliberate account-wide grant), and schema/table/column
scope columns set to `NULL`. Bind the grant's `role_name` through
`pangrova_roles` and `pangrova_user_roles` or `pangrova_group_roles`.
`MANAGE_STORAGE_PROFILE` also permits viewing the same profile. Reading source
object bytes requires a separate `READ_STORAGE_PROFILE` grant; viewing or managing
a profile does not imply it. This uses the existing PostgreSQL administrative
provisioning path; Pangrova SQL `GRANT` for storage profiles is not exposed.
Profile definitions are stored in the metadata PostgreSQL authority with immutable
incarnation and revision history. An unavailable authority fails profile operations;
there is no coordinator-local profile fallback.
Exact-name `VIEW_STORAGE_PROFILE`, `READ_STORAGE_PROFILE`, `ALL`, and `OWNER`
grants require an existing profile. Dropping it atomically revokes those grants,
so a later profile with the same name cannot inherit earlier metadata or
object-read permission.
`MANAGE_STORAGE_PROFILE` is a namespace permission and may be pre-granted for
profile creation; it remains after a drop.
An explicit `*` grant remains account-wide and applies to future names.

Query admission resolves the tenant from the authenticated user's membership when
`tenant.enabled=true`; it requires `security.enabled=true` with an authenticator
other than `NOOP`, and an absent identity
or membership is rejected. The client
cannot select a tenant through `sessionProperties.tenant_id`. With the tenant
module disabled, queries use the explicit `default` tenant identity so task
requests and distributed-write commits still carry a complete canonical owner.
Per-tenant throttling runs after this resolution. Relational queries require an
available Native Worker; when none is available, submission returns HTTP 503
(`COORDINATOR-0001`) instead of executing rows in the Coordinator.

---

## Coordinator JVM System Properties

Additional coordinator behavior is tuned via JVM system properties
(`-D...` on the coordinator launch):

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.http.max-requests-per-minute` | `100` | HTTP request rate limit applied by the coordinator request-limit filter |
| `pangrova.shuffle.manager` | (auto) | Shuffle provider name (`native`, `local-spill`, or `object-store`). When unset, the highest-durability provider on the classpath is selected automatically |
| `pangrova.index.basePath` | `${java.io.tmpdir}/pangrova/indexes` | Base directory for index build artifacts |
| `pangrova.cache.result.redis-url` | — | Redis URL to enable the external query result cache |
| `pangrova.cache.result.ttl-seconds` | `3600` | TTL for externally cached query results |
| `pangrova.cache.result.max-entry-bytes` | `10485760` | Maximum size (bytes) of a single externally cached result |

---

## Example

Production-oriented `coordinator.yml` (only real keys; provision the issuer and privileges before deployment):

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
  jdbc-username: "${PANGROVA_DB_USERNAME}"
  jdbc-password: "${PANGROVA_DB_PASSWORD}"
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
  enabled: false
indexing:
  enabled: true
operations:
  enabled: true
observability:
  enabled: true
security:
  enabled: true
  auth:
    type: OAUTH2
    properties:
      issuer-uri: "${PANGROVA_OIDC_ISSUER}"
      audience: "pangrova"
```

Authentication always verifies JWT signatures. Backend health never routes a
matching request to a different authenticator. Rejected credentials are tracked
separately from backend-health failures. `NOOP` is an explicit unsecured development
choice, not an availability fallback.

Deferred Native artifact work persists the authenticated principal, exact source
columns, target `ALTER` action, and actual security expressions observed during
compilation. It never persists bearer tokens or identity groups. Replay, page
claim/renewal, and publication read current direct roles, grants and the metadata
database's active tenant/membership authority. Raw index reads reject applicable
RLS/masks; securely transformed exports require identical current policy content,
otherwise they must be cancelled and securely recompiled. Group-only authority or
policies needing ephemeral identity attributes cannot authorize deferred work
without a trusted current identity-context resolver.

Publication reads authority and commits its manifest, active pointer and job CAS
on one PostgreSQL connection/transaction. Authority table mutation triggers share
the transaction gate, including direct SQL provisioning and revocation. Deleting
a role also removes its grants and assignments; recreating its name does not
restore authority. Authority tables disallow `TRUNCATE`; use gated `DELETE`.
Ordinary metadata reads remain readable after a builder is revoked; use of a
published artifact is authorized for the current query actor, not its old builder.

Each dispatched Native task's canonical deadline is the earlier of its initial
page lease expiry and configured task timeout. Database lease renewal does not
renew already dispatched Native credentials. Revocation stops new dispatch,
renewal and publication; cancellation of in-flight execution is best-effort,
bounded by that initial deadline. The current two-minute page lease is not a
validated large-page execution guarantee. Longer pages require generic fenced
execution-lease renewal acknowledged atomically by Native, not a widened timeout.

**See Also:** [Configuration model](index.md), [Worker](worker.md), [Quick Reference](reference.md)
