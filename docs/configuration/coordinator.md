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
| `coordinator.role` | string | `"COMBINED"` | Deployment role: `QUERY` (serve reads and writes without background maintenance), `MAINTENANCE` (background tasks only), or `COMBINED` (both). An unrecognized deployment role rejects startup. Writes to generated-column tables still require an active durable materialization owner. |
| `coordinator.internal-auth-secret` | string | — | Shared internal control-plane secret, identical to `worker.internal-auth-secret`. The production process rejects absent, blank or fewer than 32 UTF-8 bytes, independently of client authentication. Inject through the YAML `${PANGROVA_INTERNAL_AUTH_SECRET}` placeholder. |

## coordinator.transport

These keys configure bounded statement acknowledgements, not relational query
execution. Query rows are served directly by Native Workers.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.transport.port` | int | `0` | Acknowledgement listen port, in `0..65535`; zero selects an ephemeral port |
| `coordinator.transport.advertised-host` | string | `"localhost"` | Client-routable endpoint address; must not be a wildcard and must match the server certificate SAN when TLS is enabled |
| `coordinator.transport.allocator-limit-bytes` | long | `268435456` | Positive acknowledgement transport memory limit |
| `coordinator.transport.tls.enabled` | bool | `false` | Require mutual TLS for the acknowledgement server and outbound control-plane clients |
| `coordinator.transport.tls.certificate-chain-path` | string | `""` | Readable Coordinator PEM certificate chain |
| `coordinator.transport.tls.private-key-path` | string | `""` | Readable Coordinator PEM private key |
| `coordinator.transport.tls.root-ca-path` | string | `""` | Readable root CA for peer verification; the acknowledgement server requires client certificates |
| `coordinator.shuffle.tls.server-identity` | string | `""` | Certificate DNS/IP identity expected from shuffle servers |

TLS requires all four material/identity settings. Material supplied with TLS
disabled is rejected. `coordinator.host` is passed through bootstrap to the actual HTTP listener. It does not
set the advertised result address. Configure the advertised host explicitly for
clients on other machines; the loopback default is only locally reachable.

The Coordinator uses the same transport TLS certificate, private key, and root CA
for ticket-bearing private COPY task submissions. It dials the Worker's registered
HTTPS task-control port and verifies the certificate against the registered Worker
hostname. A missing TLS configuration, secure port, or internal service secret
rejects the submission before the bearer ticket is serialized or sent. The
ordinary task HTTP route remains for tasks without a COPY ticket. URI-source
`COPY INTO` SQL is still rejected until the source and commit chain is completed.

## coordinator.speculation

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.speculation.enabled` | bool | `false` | Enable the existing straggler detector and speculative task launcher. Copies use issued task authority, winner election and exact-attempt drain receipts; external Shuffle additionally requires explicit winning-producer attempt isolation. The current Pangrova remote lifecycle rejects this combination at startup. |

## coordinator.ha

Leader election uses the metadata DataSource and the `distributed_lease` table in
`pangrova_meta`. All Coordinators must share that metadata database. There is no
separate HA database and no `coordinator.ha.jdbc-url` config option.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.ha.enabled` | bool | `false` | Declared in `CoordinatorOptions`; not read by the current production bootstrap |
| `coordinator.ha.ddl-leader-routing` | bool | `false` | When `true`, DDL statements are routed exclusively to the leader coordinator (HTTP 307 redirect) |

### Distributed lease backend (JVM system properties)

The Coordinator distribution includes JDBC and ZooKeeper factories. Both the leader lease and the membership registry use the selected factory. JDBC is the default. To run
leader election on a fault-tolerant ZooKeeper quorum instead, set these JVM
system properties (`-D...`) on every coordinator. Explicit backend selection requires metadata and its JDBC data source for the durable journal. A loopback role with metadata disabled can use a process-local lease only when the backend property is absent.

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.speculation.single-task-duration-threshold-ms` | `0` (disabled) | When speculation is enabled, a one-task stage may launch one copy on another Worker after this elapsed time. Values below 10,000 ms still use the detector's 10-second minimum; peer-based stages retain the 75% completion rule. |
| `pangrova.coordination.lease.backend` | `jdbc` | Lease backend: `jdbc` (single DB) or `zookeeper` (quorum) |
| `pangrova.coordination.lease.zookeeper.connectString` | — | Required when backend=`zookeeper`. ZK ensemble, e.g. `zk1:2181,zk2:2181,zk3:2181` |
| `pangrova.coordination.lease.zookeeper.basePath` | `/pangrova/leases` | Parent znode under which lease nodes are stored |

## coordinator.query

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.query.timeout` | duration | `"5m"` | Maximum query execution time before timeout |
| `coordinator.query.placement-provider` | string | `load-balanced` | Installed worker placement strategy ID |
| `coordinator.query.max-concurrent` | int | `100` | Maximum number of concurrently executing queries |
| `coordinator.task.spill.max-bytes` | long | `8589934592` (8 GiB) | Per-task spill byte ceiling; zero disables task spilling. Requires Worker-local spill directory. |

Task spill budgets are signed 64-bit non-negative byte counts. Invalid, negative,
or overflowing values fail configuration before startup. The grant applies to
**each task**, not the sum of tasks in a query. A positive grant does not reserve
disk space: the Worker must configure its own `worker.spill.directory`, and Native
uses the smaller positive task and node limit when a node cap exists. A zero task
grant disables task spilling even if the Worker has a directory. The Coordinator
never supplies a filesystem path for another machine. These settings also apply
to Standalone, which uses the same Coordinator configuration loader.

## coordinator.shuffle

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.shuffle.mode` | enum | `NATIVE` | `NATIVE` serves shuffle from each Worker's own Flight server. `REMOTE` uses the selected external provider. Consumers require a committed partition receipt. The Pangrova provider declares blocking partition reads and rejects speculative-attempt isolation. |
| `coordinator.shuffle.provider` | string | `pangrova` | Selects the external control provider in REMOTE mode. Scope its options under `coordinator.shuffle.provider.<id>.*`. Startup checks the installed factory, configuration and required capabilities. Worker registration, refresh and recovery admit only a matching mode and external provider ID. |
| `coordinator.shuffle.endpoints` | string (comma-separated `host:port`) | `""` | Pangrova service addresses. Set this or `coordinator.shuffle.provider.pangrova.endpoints` once for REMOTE mode with the Pangrova provider. |

The production Pangrova remote service requires shared PostgreSQL execution authority, Coordinator
transport TLS plus `coordinator.shuffle.tls.server-identity` (the shuffle service
demands mutual TLS), matching `worker.shuffle.mode=REMOTE` on every Worker, and a
deployed shuffle service with a PostgreSQL database, an S3 bucket and the
capability secret. `BROADCAST` exchange is not supported in `REMOTE` mode.

## coordinator.worker

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.worker.heartbeat-timeout` | duration | `"30s"` | Duration after which a worker is considered dead if no heartbeat received |
| `coordinator.worker.max-retry` | int | `3` | Maximum number of task dispatch retries to a worker |

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
except `security.enabled`, `tenant.enabled` and `indexing.enabled`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `governance.enabled` | bool | `true` | Enable the governance module |
| `resilience.enabled` | bool | `true` | Enable the resilience module (circuit breaker, retry) |
| `streaming.enabled` | bool | `true` | Enable the streaming module |
| `tenant.enabled` | bool | `false` | Enable the multi-tenant module; requires authenticated user identities |
| `indexing.enabled` | bool | `false` | Enable the indexing module; configure `indexing.base-path` before enabling it |
| `indexing.base-path` | string | — (required when indexing is enabled) | Durable scalar `.idx` and Native artifact staging root; PostgreSQL stores only descriptors, mappings and manifests |
| `indexing.build.thread-pool-size` / `parallelism` / `batch-flush-size` | int | `4` / `4` / `500` | Scalar builder threads, per-index file concurrency and mapping batch size |
| `indexing.build.max-concurrent-per-tenant` | int | `2` | Maximum admitted scalar builds for one tenant |
| `indexing.cache.size-bytes` | long | `-1` | Coordinator scalar evaluation cache; negative selects `min(256 MiB, 12.5% of max heap)` |
| `indexing.eval.parallelism` / `timeout-ms` | int / long | `8` / `5000` | Scalar split evaluation concurrency and timeout |
| `indexing.eval.circuit-failure-threshold` / `circuit-open-seconds` | int | `5` / `60` | Scalar evaluation circuit breaker |
| `indexing.eval.include-stale` | bool | `true` | Permit conservative hints for STALE scalar indexes |
| `indexing.gc.interval-minutes` / `grace-period-minutes` | int | `60` / `1440` | Index GC interval and dropped-index grace period |
| `indexing.commit-listener.enabled` / `poll-batch-size` | bool / int | `false` / `64` | Durable commit-driven maintenance and poll batch; enable only after lifecycle qualification |
| `indexing.consistency.check-interval-minutes` / `rebuild-threshold` | int / double | `30` / `0.8` | Coverage check interval and refresh threshold |
| `indexing.consistency.auto-rebuild-enabled` | bool | `false` | Automatic replacement; enable only after lifecycle qualification |
| `indexing.consistency.stuck-detection-multiplier` | int | `2` | Stuck-generation threshold multiplier |
| `indexing.artifact.dispatch-lease` / `dispatch-retry-delay` | duration | `2m` / `5s` | Native build-page lease and retry backoff |
| `indexing.artifact.dispatch-max-in-flight` | int | `32` | Maximum Native build pages leased by one Coordinator |
| `indexing.artifact.publish-claim-ttl` / `publish-retry-delay` | duration | `30s` / `15s` | Atomic generation publication lease and retry backoff |
| `indexing.artifact.publish-poll-interval` / `publish-batch-size` | duration / int | `1s` / `64` | Native publication polling and batch bound |
| `operations.enabled` | bool | `true` | Construct the Coordinator recovery playbook for explicitly dispatched incidents |
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
original groups and attributes. SQL privilege administration is available with OAuth2/LDAP and PostgreSQL.
Provision initial roles, memberships and root authority explicitly; no administrator
role name is automatically trusted. NOOP/bypass cannot administer SQL privileges.
For storage profiles, provision `VIEW_STORAGE_PROFILE`,
`MANAGE_STORAGE_PROFILE`, or `READ_STORAGE_PROFILE` in `security_grants` with
`object_type = 'STORAGE_PROFILE'`, `catalog_name` equal to the exact profile
name (or `*` for a deliberate account-wide grant), and schema/table/column
scope columns set to `NULL`. Bind the grant's `role_name` through
`pangrova_roles` and `pangrova_user_roles` or `pangrova_group_roles`.
`MANAGE_STORAGE_PROFILE` also permits viewing the same profile. Reading source
object bytes requires a separate `READ_STORAGE_PROFILE` grant; viewing or managing
a profile does not imply it. Trusted provisioning can establish initial authority; subsequent grants can use
`GRANT READ_STORAGE_PROFILE ON STORAGE PROFILE profile_name TO ROLE role_name`
with current OWNER or WITH GRANT OPTION. See the [SQL privilege contract](../architecture/security.md#sql-privilege-administration).
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
| `pangrova.http.max-requests-per-minute` | `100` | Public HTTP requests per actual transport-peer IP per fixed one-minute window |

`indexing.base-path` is YAML-backed and may also be supplied through
`PANGROVA_INDEX_BASE_PATH`. The legacy
`-Dpangrova.index.basePath` property remains a process-level fallback for existing
standalone launch scripts; new deployments should use the YAML key. Indexing
stays disabled until the root and Worker artifact-cache prerequisites are
configured. The root must be readable and writable at the same path by every
Coordinator and Worker that builds or reads scalar artifacts; Native Workers
write artifact objects directly to that root. Automatic commit-driven refresh
and consistency rebuild remain disabled until lifecycle qualification passes;
fenced stuck-build recovery remains available as a fail-safe.

The server captures the actual connection peer before authentication. Caller-supplied
`X-Forwarded-For` is not trusted for authentication, brute-force tracking or this
request budget; no trusted-proxy address policy is currently configured. Behind a
reverse proxy, this limit therefore applies to the proxy's actual peer address.
Only a server-authenticated internal-service identity on its dedicated control
routes is excluded from the public budget, so user saturation cannot starve Worker
heartbeats, terminal receipts or deregistration. Internal credentials cannot access
user query routes. The request-body limit still applies to authenticated control:
exactly the configured limit is allowed; an additional byte, including in a chunked
request, returns structured HTTP 413 rather than HTTP 500.

---

## Example

Production-oriented `coordinator.yml` (only real keys; provision the issuer and privileges before deployment):

```yaml
coordinator:
  id: "${HOSTNAME:-coordinator-1}"
  host: "0.0.0.0"
  port: 8080
  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"
  ha:
    enabled: true
    # Leader election uses the metadata DataSource; no separate HA database.
  query:
    timeout: "10m"
    max-concurrent: 200
  worker:
    heartbeat-timeout: "30s"
    max-retry: 3


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

### External connector plugins

`connector.plugin-directory` is an optional local path. Each immediate subdirectory
contains the provider JAR and its dependency JARs. The role discovers these
alongside its bundled providers at startup. Install the matching plugin version
in each role that will execute the connector. [Plugin layout and lifecycle](../architecture/connectors.md#23-connector-plugin-deployment-and-lifetime)
