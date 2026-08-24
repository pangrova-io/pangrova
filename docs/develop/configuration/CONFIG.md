# Configuration Reference

This document is the authoritative reference for all Pangrova configuration settings. It covers both Coordinator and Worker configuration, including JVM tuning, security, observability, and catalog settings.

**Configuration files** (relative to your installation root or `/etc/pangrova/`):

| File | Role | Purpose |
|------|------|---------|
| `coordinator.yml` | Coordinator | Coordinator behavior, HA, query, metadata, AI |
| `worker.yml` | Worker | Worker behavior, memory, shuffle, native engine |
| `security.yml` | Coordinator | Authentication, authorization, TLS, audit |
| `jvm-coordinator.config` | Coordinator | JVM heap, GC, and runtime flags |
| `jvm-worker.config` | Worker | JVM heap, GC, and runtime flags |
| `logback.xml` | All | Logging configuration |
| `pangrova-env.sh` | All | Environment variables and OS settings |
| `catalog/<name>.yml` | Coordinator | Per-catalog connection configuration |

## Configuration Syntax

All YAML files support environment variable substitution:

```yaml
# Required variable — startup fails if not set
host: "${MY_ENV_VAR}"

# Optional variable with fallback default
timeout: "${MY_TIMEOUT:-30s}"
```

Duration values use human-readable format: `30s`, `5m`, `1h`, `2h30m`.

Size values use byte suffixes: `512MB`, `4GB`, `100GB`. Or raw bytes: `536870912`.

### Configuration Tiers

This document distinguishes two tiers of configuration:

- **Core ConfigOption** — backed by typed `ConfigOption<T>` definitions in `pangrova-config` module. These are validated at startup, support environment variable override (`PANGROVA_*`), and have programmatic defaults.
- **Module YAML** — consumed directly by downstream modules from the YAML flat map. These are loaded but not validated by the typed config system. Marked with † in tables below.

> **Tip**: If a configuration key is not marked with †, it is guaranteed to be read and validated by the bootstrap code.

---

## Coordinator Configuration (`coordinator.yml`)

### coordinator

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `coordinator.id` | string | `"coordinator-1"` | Yes | Unique stable node identifier. Used in HA leader election and distributed tracing. Convention: `coordinator-<dc>-<index>` |
| `coordinator.host` | string | `"0.0.0.0"` | No | Bind address. Use `0.0.0.0` to listen on all interfaces |
| `coordinator.port` | int | `8080` | No | HTTP REST API port (client-facing) |
| `coordinator.role` | string | `"COMBINED"` | No | Deployment role: `QUERY` (serve queries only), `MAINTENANCE` (background tasks only), or `COMBINED` (both) |

### coordinator.ha

Controls High Availability leader election via a JDBC lease store.

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `coordinator.ha.enabled` | bool | `false` | No | Enable HA mode. Required for multi-coordinator deployments |
| `coordinator.ha.jdbc-url` | string | — | HA only | JDBC URL of the HA state database (PostgreSQL) |
| `coordinator.ha.username` † | string | — | HA only | Database user with read/write access to the HA schema |
| `coordinator.ha.password` † | string | — | HA only | Database password. Use `${ENV_VAR}` to inject from environment |
| `coordinator.ha.pool-size` † | int | `5` | No | JDBC connection pool size per coordinator node |
| `coordinator.ha.lease-duration` † | duration | `"30s"` | No | How long a leader lease remains valid without renewal. Must be > 3× `heartbeat-interval` |
| `coordinator.ha.heartbeat-interval` † | duration | `"10s"` | No | How often the leader renews its lease |
| `coordinator.ha.election-timeout` † | duration | `"10s"` | No | How long a follower waits after lease expiry before starting an election |

**Distributed lease backend (A-HA-01 — removes the single-JDBC coordination SPOF).** By default the leader-election lease is stored in the HA JDBC database above, which is a single point of failure. To run leader election on a fault-tolerant **ZooKeeper quorum** instead, set these JVM system properties (`-D...`) on every coordinator:

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.coordination.lease.backend` | `jdbc` | Lease backend: `jdbc` (single DB) or `zookeeper` (quorum, no SPOF) |
| `pangrova.coordination.lease.zookeeper.connectString` | — | Required when backend=`zookeeper`. ZK ensemble, e.g. `zk1:2181,zk2:2181,zk3:2181` |
| `pangrova.coordination.lease.zookeeper.basePath` | `/pangrova/leases` | Parent znode under which lease nodes are stored |

With `backend=zookeeper`, leader election survives loss of a minority of ZooKeeper nodes. Requires `curator-recipes` + `zookeeper` on the coordinator classpath (bundled as optional dependencies). Lease semantics are identical to the JDBC backend (epoch fencing, TTL renewal, authoritative server clock).

### coordinator.query

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `coordinator.query.timeout` | duration | `"5m"` | No | Wall-clock timeout per query. Queries exceeding this are cancelled |
| `coordinator.query.max-concurrent` | int | `100` | No | Maximum number of concurrently executing queries |
| `coordinator.query.max-queued` † | int | `500` | No | Maximum number of queries queued when `max-concurrent` is reached. Requests beyond this receive `HTTP 429` |
| `coordinator.query.result-cache.enabled` † | bool | `false` | No | Cache query results by fingerprint for repeated identical queries |
| `coordinator.query.result-cache.max-size` † | int | `500` | No | Maximum number of cached result entries |
| `coordinator.query.result-cache.ttl` † | duration | `"5m"` | No | Cache entry time-to-live. Invalidated on table writes regardless of TTL |
| `coordinator.query.result-cache.max-entry-size` † | size | `"32MB"` | No | Maximum size of a single cacheable result |
| `coordinator.query.planner.stats-collection` † | bool | `true` | No | Collect runtime statistics for adaptive re-planning |
| `coordinator.query.planner.replan-threshold` † | double | `3.0` | No | Trigger re-planning when actual row count deviates from estimate by this factor |

### coordinator.worker

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `coordinator.worker.heartbeat-timeout` | duration | `"30s"` | No | Mark a worker DEAD if no heartbeat received within this window |
| `coordinator.worker.max-retry` | int | `3` | No | Number of times to reassign tasks from a dead worker before failing the query |
| `coordinator.worker.min-healthy-workers` † | int | `1` | No | Minimum healthy workers before accepting new queries |
| `coordinator.worker.recovery-cooldown` † | duration | `"60s"` | No | Cooldown after a worker recovery event before it receives high-priority queries |

---

## State Store Configuration (`state-store`)

The state store persists distributed state (HA leases, task journals, DDL locks). `IN_MEMORY` loses all state on restart and must not be used in production.

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `state-store.type` | enum | `IN_MEMORY` | No | Backend type: `IN_MEMORY` (dev only) or `JDBC` (production) |
| `state-store.jdbc-url` † | string | — | JDBC mode | PostgreSQL JDBC URL |
| `state-store.username` † | string | — | JDBC mode | Database user |
| `state-store.password` † | string | — | JDBC mode | Database password. Inject via `${ENV_VAR}` |
| `state-store.pool-size` † | int | `10` | No | JDBC connection pool size |

---

## Metadata Configuration (`metadata`)

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `metadata.enabled` | bool | `true` | No | Enable the metadata module |
| `metadata.jdbc-url` | string | — | Yes | PostgreSQL JDBC URL for metadata storage |
| `metadata.username` † | string | — | Yes | Database user |
| `metadata.password` † | string | — | Yes | Database password. Inject via `${ENV_VAR}` |
| `metadata.pool.min-idle` † | int | `5` | No | Minimum idle JDBC connections |
| `metadata.pool.max-size` † | int | `20` | No | Maximum JDBC connections |
| `metadata.pool.connection-timeout` † | duration | `"5s"` | No | Timeout for acquiring a connection from the pool |
| `metadata.pool.idle-timeout` † | duration | `"10m"` | No | Maximum time a connection may sit idle in the pool |
| `metadata.pool.max-lifetime` † | duration | `"30m"` | No | Maximum lifetime of a pool connection |
| `metadata.cache.l1-max-size` | int | `10000` | No | L1 (on-heap) metadata cache maximum number of entries |
| `metadata.cache.l1-ttl` | duration | `"60s"` | No | L1 cache time-to-live |
| `metadata.cache.l2-enabled` † | bool | `false` | No | Enable distributed L2 metadata cache across coordinators |

---

## Feature Flags

Feature flags control which subsystem modules are active. These are **Core ConfigOptions** shared between `coordinator.yml` and `worker.yml`. All default to `true` except `security.enabled`.

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `governance.enabled` | bool | `true` | No | Enable the governance module (lineage, data quality) |
| `resilience.enabled` | bool | `true` | No | Enable the resilience module (circuit breaker, retry) |
| `streaming.enabled` | bool | `true` | No | Enable the streaming module (CDC checkpoints) |
| `tenant.enabled` | bool | `true` | No | Enable the multi-tenant module |
| `indexing.enabled` | bool | `true` | No | Enable the indexing module |
| `operations.enabled` | bool | `true` | No | Enable the operations module (compaction, maintenance) |
| `observability.enabled` | bool | `true` | No | Enable the observability module |
| `security.enabled` | bool | `false` | No | Enable the security module. When `true`, `security.auth.type` must be configured |
| `security.auth.type` | string | — | Security only | Authenticator type: `OAUTH2`, `LDAP`, or `NOOP` (dev only) |

---

## Worker Configuration (`worker.yml`)

### worker (root)

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.host` | string | `"0.0.0.0"` | No | Bind address |
| `worker.port` | int | `8081` | No | HTTP management port (health check, metrics) |
| `worker.advertised-host` | string | — | Yes | Hostname or IP reachable by Coordinator and other Workers. Auto-detection fails behind NAT — always set explicitly |
| `worker.id` | string | `"worker-1"` | Yes | Unique stable node identifier |
| `worker.coordinator-url` | string | — | Yes | URL of the Coordinator REST API (or load balancer VIP in HA mode) |
| `worker.heartbeat-interval` | duration | `"10s"` | No | How often this Worker sends a heartbeat. Must be < `coordinator.worker.heartbeat-timeout` |

### worker.transport

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.transport.port` | int | `0` | No | Arrow Flight shuffle port. `0` = auto-assign. Set a fixed port in production for firewall rules |
| `worker.transport.ticket-signing-secret` † | string | — | Yes (prod) | HMAC-SHA256 secret for shuffle ticket authorization. Must match on all Workers and the Coordinator |
| `worker.transport.ticket-ttl` † | duration | `"5m"` | No | Shuffle ticket validity window |
| `worker.transport.tls.enabled` † | bool | `false` | No | Enable mTLS for Worker↔Worker and Coordinator→Worker transport |
| `worker.transport.tls.cert-file` † | string | — | TLS only | Path to PEM certificate file |
| `worker.transport.tls.key-file` † | string | — | TLS only | Path to PEM private key file (mode 600) |
| `worker.transport.tls.ca-file` † | string | — | TLS only | Path to CA certificate bundle |
| `worker.transport.tls.min-protocol` † | string | `"TLSv1.2"` | No | Minimum TLS protocol version |

### worker.memory

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.memory.budget-bytes` | bytes | `536870912` | Yes | Total Java-layer query execution memory budget. Does NOT include Arrow off-heap or Velox native cache |
| `worker.memory.node-bytes` | bytes | `1073741824` | Yes | Total node physical memory reported to Coordinator for scheduling. Typically 85% of actual RAM |
| `worker.memory.system-reserved-bytes` | bytes | `67108864` | No | Reserved for JVM internals (metaspace, code cache). Increase if native OOM occurs outside heap |
| `worker.memory.pressure-threshold-low` | double | `50.0` | No | Memory usage percentage that triggers LOW pressure level |
| `worker.memory.pressure-threshold-medium` | double | `70.0` | No | Memory usage percentage that triggers MEDIUM pressure level |
| `worker.memory.pressure-threshold-high` | double | `85.0` | No | Memory usage percentage that triggers HIGH pressure level |
| `worker.memory.pressure-threshold-critical` | double | `95.0` | No | Memory usage percentage that triggers CRITICAL pressure level |

### worker.threads

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.threads.query` | int | `4` | No | Parallel query execution threads. Set to physical CPU core count |
| `worker.threads.io` | int | `2` | No | Async I/O threads for remote storage reads |
| `worker.threads.scheduler` | int | `1` | No | Internal scheduler threads (heartbeat, spill monitor, cleanup) |

### worker.task

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.task.result-retention-seconds` | int | `300` | No | Seconds to retain completed query results before eviction. Clients must fetch results before this window expires |

### worker.shuffle

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.shuffle.buffer-capacity` | int | `100` | No | Maximum number of open shuffle demuxer slots |
| `worker.shuffle.max-memory-bytes` | bytes | `536870912` | No | Total memory budget for in-flight shuffle data. Recommended: ~25% of JVM heap |
| `worker.shuffle.spill-threshold` | double | `0.8` | No | Fraction of `max-memory-bytes` at which shuffle data spills to disk |
| `worker.shuffle.lingering-ttl` † | long (ms) | `0` | No | Evict uncollected demuxers after this many milliseconds. `0` = disabled. Recommended production value: `300000` (5 min) |

### worker.spill

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.spill.directory` | string | — | No | Directory for spill files. Use a fast NVMe mount separate from data and root partitions |
| `worker.spill.max-size-bytes` † | bytes | — | No | Maximum total size of all spill files. Queries are rejected when limit is reached |
| `worker.spill.write-threads` † | int | `4` | No | I/O concurrency for spill writes |

### worker.native

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.native.path` | string | — | No | Explicit path to the native library file. If unset, extracted from classpath |
| `worker.native.workdir` | string | — | No | Working directory for native library extraction and temporaries |
| `worker.native.delete-after-load` | bool | `true` | No | Delete the extracted native library temp file after `System.load()` |

### worker.native.cache

The Velox async data cache absorbs repeated scans of hot data. RAM cache is in-process; SSD cache persists across restarts.

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.native.cache.enabled` | bool | `false` | No | Enable the Velox async data cache |
| `worker.native.cache.memory-bytes` | bytes | `8589934592` | No | In-process RAM cache budget. Keep below `(total RAM − Xmx − MaxDirectMemorySize − OS headroom)` |
| `worker.native.cache.ssd-path` | string | — | No | Directory for the SSD-backed persistent cache |
| `worker.native.cache.ssd-bytes` | bytes | `107374182400` | No | Total SSD cache capacity (default 100 GB) |
| `worker.native.cache.ssd-shards` | int | `8` | No | Number of SSD cache shards for parallel I/O. Match to NVMe drive count or queue depth |
| `worker.native.cache.checkpoint-interval-bytes` | bytes | `1073741824` | No | SSD cache checkpoint interval. The native engine persists its SSD cache index every N bytes so the cache survives restart. Set to `0` to disable |

### worker.registration

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.registration.max-attempts` | int | `5` | No | Maximum registration retry attempts if Coordinator is temporarily unavailable |
| `worker.registration.initial-backoff` | duration | `"1s"` | No | Initial retry backoff |
| `worker.registration.max-backoff` | duration | `"30s"` | No | Maximum retry backoff |

### worker.cdc

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.cdc.enabled` | bool | `true` | No | Enable CDC (Change Data Capture) processing on this worker |

### worker.internal-auth-secret

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `worker.internal-auth-secret` | string | — | Yes (prod) | Shared HMAC-SHA256 secret for X-Internal-Auth header between Coordinator and Worker. Must be identical on all nodes |

---

## Observability Configuration (`observability`)

Applies to both `coordinator.yml` and `worker.yml`.

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `observability.enabled` | bool | `true` | No | Enable the observability module |
| `observability.service-name` | string | — | No | Service name in traces and metrics. Use `"pangrova-coordinator"` or `"pangrova-worker"` |
| `observability.otlp-endpoint` | string | — | No | OTLP gRPC endpoint for OpenTelemetry Collector (e.g. `http://otel-collector:4317`) |
| `observability.trace-sampling-rate` † | double | `1.0` | No | Fraction of traces to export. `1.0` = 100%, `0.1` = 10%. Reduce in high-traffic production |
| `observability.metrics-export-interval` † | duration | `"30s"` | No | How often metrics are pushed to the OTLP endpoint |
| `observability.slow-query-threshold` † | duration | `"5s"` | No | Emit slow query plan as a trace event when execution exceeds this threshold |

---

## Security Configuration (`security.yml`)

### auth

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `auth.type` | enum | — | Yes (security enabled) | Authentication type: `OAUTH2` or `LDAP` |
| `auth.oauth2.issuer-uri` | string | — | OAuth2 | OIDC issuer URI. Must match `"iss"` claim in tokens |
| `auth.oauth2.audience` | string | — | OAuth2 | Expected token audience. Must match `"aud"` claim |
| `auth.oauth2.jwks-uri` | string | — | No | JWKS endpoint. Auto-discovered from `issuer-uri` if omitted |
| `auth.oauth2.jwks.cache-ttl` | duration | `"1h"` | No | How long JWKS keys are cached before re-fetching |
| `auth.oauth2.claims.principal` | string | `"sub"` | No | JWT claim containing the user identity |
| `auth.oauth2.claims.groups` | string | `"groups"` | No | JWT claim containing group memberships for RBAC |
| `auth.oauth2.claims.tenant` | string | `"tenant_id"` | No | JWT claim containing the tenant identifier for multi-tenant isolation |

### internal

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `internal.auth-secret` | string | — | Yes (prod) | HMAC-SHA256 secret shared among all Coordinators and Workers. Generate: `openssl rand -base64 48` |
| `internal.token-ttl` | duration | `"1h"` | No | Internal authentication token lifetime |

### tls

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `tls.external.enabled` | bool | `false` | No | Enable HTTPS for the REST API |
| `tls.external.cert-file` | string | — | TLS only | PEM certificate chain file path |
| `tls.external.key-file` | string | — | TLS only | PEM private key file path (mode 600) |
| `tls.external.ca-file` | string | — | TLS only | CA bundle file path |
| `tls.external.min-protocol` | string | `"TLSv1.2"` | No | Minimum TLS protocol version |
| `tls.external.ciphers` | list | (platform defaults) | No | Explicit cipher suite list. When set, only listed ciphers are accepted |

### rbac

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `rbac.default-deny` | bool | `true` | No | Deny requests that do not match an explicit grant |
| `rbac.admin-role` | string | `"pangrova-admin"` | No | Role that grants superuser access, bypassing all object-level checks |

### audit

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `audit.enabled` | bool | `false` | No | Enable audit logging |
| `audit.log-ddl` | bool | `true` | No | Log all DDL operations |
| `audit.log-dml` | bool | `false` | No | Log all DML operations. High volume — consider disabling and relying on query logs |
| `audit.log-auth` | bool | `true` | No | Log all authentication attempts (success and failure) |
| `audit.log-authz-denials` | bool | `true` | No | Log all authorization denials |
| `audit.retention-days` | int | `365` | No | Audit log retention in days |
| `audit.destination` | enum | `LOGFILE` | No | Audit log destination: `LOGFILE` or `SIEM` |

---

## AI Configuration (`ai`)

AI configuration lives under `ai:` in `coordinator.yml`.

### ai.embedding

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.embedding.enabled` | bool | `false` | No | Enable the embedding pipeline (required for vector columns and semantic cache) |
| `ai.embedding.queue-capacity` | int | `100000` | No | Pending embedding request queue depth |
| `ai.embedding.batch-size` | int | `64` | No | Rows sent to the inference provider per batch call |
| `ai.embedding.batch-timeout-ms` | long (ms) | `100` | No | Maximum time to wait before flushing a partial batch |
| `ai.embedding.provider.base-url` | string | — | Embedding | OpenAI-compatible inference endpoint base URL |
| `ai.embedding.provider.model` | string | — | Embedding | Embedding model name as registered with the provider |
| `ai.embedding.provider.timeout` | duration | `"10s"` | No | Per-call HTTP timeout for inference requests |
| `ai.embedding.provider.max-connections` | int | `8` | No | Concurrent HTTP connections to the inference provider |
| `ai.embedding.provider.drainer-threads` | int | `4` | No | Number of parallel embedding drainer threads |

### ai.semantic-cache

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.semantic-cache.enabled` | bool | `false` | No | Enable the HNSW-backed semantic query cache |
| `ai.semantic-cache.capacity` | int | `1000` | No | Maximum number of cached query embeddings |
| `ai.semantic-cache.similarity-threshold` | double | `0.92` | No | Cosine similarity threshold for a cache hit. Higher = stricter matching |
| `ai.semantic-cache.ttl` | duration | `"10m"` | No | Cache entry TTL. Invalidated on table write regardless of TTL |

### ai.conversation

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.conversation.max-turns` | int | `50` | No | Maximum conversation turns retained per session for multi-turn SQL rewriting |
| `ai.conversation.session-ttl` | duration | `"30m"` | No | Session idle timeout. Context is purged after inactivity |

### ai.layout-advisor

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.layout-advisor.pattern-capacity` | int | `10000` | No | Ring buffer size for recent query patterns used to score layout recommendations |
| `ai.layout-advisor.recommendation-interval-minutes` | int | `60` | No | How often layout recommendations are re-evaluated |

### ai.index-lifecycle

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.index-lifecycle.health-check-interval-minutes` | int | `30` | No | How often to assess index health (staleness, compaction needed) |
| `ai.index-lifecycle.recommendation-interval-minutes` | int | `60` | No | How often to emit index rebuild recommendations |

### ai.reranker

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `ai.reranker.enabled` | bool | `false` | No | Enable reranking for hybrid vector + BM25 search results |
| `ai.reranker.model` | string | — | Reranker | Reranker model name as registered with the inference provider |

---

## Storage Backend Configuration

Pangrova uses object storage (S3, OSS, MinIO, GCS, ADLS) as the primary data lake layer. Storage credentials are configured at two levels:

1. **Environment variables** in `pangrova-env.sh` — global defaults for all catalogs
2. **Per-catalog properties** in `catalog/<name>.yml` — override per data source

### AWS S3

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_REGION` | string | Yes | AWS region (e.g. `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | string | Static auth | S3 access key ID |
| `AWS_SECRET_ACCESS_KEY` | string | Static auth | S3 secret access key |
| `AWS_ROLE_ARN` | string | IAM auth | IAM role ARN for AssumeRole (preferred over static keys) |
| `AWS_SESSION_TOKEN` | string | No | Temporary session token (for STS-based auth) |
| `AWS_ENDPOINT_URL` | string | No | Custom S3 endpoint (for MinIO, Ceph, R2, LocalStack) |
| `s3.endpoint` | string | No | Per-catalog S3 endpoint override |
| `s3.access-key-id` | string | No | Per-catalog access key override |
| `s3.secret-access-key` | string | No | Per-catalog secret key override |
| `s3.region` | string | No | Per-catalog region override |
| `s3.path-style-access` | bool | No | Enable path-style access (required for MinIO/Ceph). Default: `false` |

**Authentication priority** (highest → lowest):
1. Per-catalog properties (`s3.*` in catalog YAML)
2. Environment variables (`AWS_*`)
3. Instance profile / IAM role (EC2, ECS, EKS Pod Identity)
4. Default credential chain (SDK auto-discovery)

### Alibaba Cloud OSS

OSS is S3-compatible. Configure as S3 with a custom endpoint:

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_ACCESS_KEY_ID` or `ALIBABA_CLOUD_ACCESS_KEY_ID` | string | Yes | OSS AccessKey ID |
| `AWS_SECRET_ACCESS_KEY` or `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | string | Yes | OSS AccessKey Secret |
| `AWS_ENDPOINT_URL` | string | Yes | OSS endpoint (e.g. `https://oss-cn-hangzhou.aliyuncs.com`) |
| `s3.endpoint` | string | Yes | Per-catalog OSS endpoint |
| `s3.path-style-access` | bool | Yes | Must be `false` for OSS (virtual-hosted style) |
| `s3.region` | string | Yes | OSS region (e.g. `cn-hangzhou`) |

**OSS endpoint format**: `https://oss-{region}.aliyuncs.com` (public) or `https://oss-{region}-internal.aliyuncs.com` (VPC internal).

### MinIO / S3-Compatible

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | string | Yes | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | string | Yes | MinIO secret key |
| `AWS_ENDPOINT_URL` | string | Yes | MinIO endpoint (e.g. `http://minio:9000`) |
| `s3.path-style-access` | bool | Yes | Must be `true` for MinIO |
| `s3.region` | string | No | Region (default: `us-east-1`; MinIO ignores this) |

### Google Cloud Storage (GCS)

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | string | Yes | Path to GCP service account JSON key file |
| `gcs.project-id` | string | No | GCP project ID (auto-detected from credentials if omitted) |

### Azure ADLS Gen2

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AZURE_STORAGE_ACCOUNT` | string | Yes | Storage account name |
| `AZURE_STORAGE_ACCESS_KEY` | string | Key auth | Storage account access key |
| `AZURE_TENANT_ID` | string | SP auth | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | string | SP auth | Service principal client ID |
| `AZURE_CLIENT_SECRET` | string | SP auth | Service principal client secret |

---

## Catalog Configuration (`catalog/<name>.yml`)

Each catalog has its own YAML file in `${PANGROVA_CONF_DIR}/catalog/`. The file name (without `.yml`) becomes the catalog name in SQL (e.g. `catalog/my_lake.yml` → `USE my_lake`).

### YAML Structure

All catalog files share the same two-level structure:

```yaml
connector: <connector-type>   # "iceberg" or "paimon"
properties:
  <key>: <value>
  # ... connector-specific properties
```

- `connector` — selects the ConnectorFactory implementation (discovered via ServiceLoader)
- `properties` — flat key-value map passed directly to the ConnectorFactory

### Iceberg Connector (`connector: iceberg`)

The Iceberg connector supports **hadoop** (default) and **REST** catalog modes.

#### Mode selection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `catalog-type` | string | `"hadoop"` | Catalog implementation: `hadoop` or `rest` |

#### Hadoop mode properties

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `type` | string | `"hadoop"` | No | Iceberg catalog type passed to `CatalogUtil.buildIcebergCatalog()`. Alternatives: `hive`, `glue`, custom `catalog-impl` |
| `warehouse` | string | `$TMPDIR/iceberg-<name>` | Yes (prod) | Root storage path for table data and metadata (e.g. `s3://bucket/warehouse`) |
| `catalog-impl` | string | — | No | Fully-qualified class name for a custom Iceberg catalog implementation |

All other properties are passed through to Hadoop Configuration (supporting S3, HDFS, GCS, ADLS settings):

| Property | Type | Description |
|----------|------|-------------|
| `s3.endpoint` | string | S3-compatible endpoint URL |
| `s3.region` | string | AWS/OSS region |
| `s3.access-key-id` | string | Static access key |
| `s3.secret-access-key` | string | Static secret key |
| `s3.path-style-access` | bool | `true` for MinIO/Ceph, `false` for AWS/OSS |
| `fs.s3a.*` | string | Any Hadoop S3A filesystem property |

#### REST mode properties (`catalog-type: rest`)

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `uri` | string | — | Yes | Iceberg REST Catalog server endpoint |
| `warehouse` | string | — | No | Warehouse hint passed to REST catalog |
| `credential` | string | — | No | OAuth2 client credential for REST catalog |
| `oauth2-server-uri` | string | — | No | OAuth2 token endpoint URI |
| `scope` | string | — | No | OAuth2 scope |
| `token` | string | — | No | Pre-existing bearer token |

#### Encryption (both modes)

| Property | Type | Description |
|----------|------|-------------|
| `encryption.accessible-keys` | string | Comma-separated list of encryption key IDs accessible by this catalog |

### Paimon Connector (`connector: paimon`)

The Paimon connector supports **filesystem** (default) and **REST** catalog modes.

#### Mode selection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `metastore` | string | `"filesystem"` | Catalog implementation: `filesystem` or `rest` |

#### Filesystem mode properties

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `warehouse` | string | `$TMPDIR/paimon-warehouse` | Yes (prod) | Root storage path for Paimon table data |
| `temp-dir` | string | `$TMPDIR/paimon-temp` | No | Temporary directory for Paimon I/O operations |

All other properties are passed through to Paimon Options (supporting S3, HDFS, OSS, etc.).

#### REST mode properties (`metastore: rest`)

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `uri` | string | — | Yes | Paimon REST Catalog server endpoint |

### Example: Iceberg on S3 (hadoop mode)

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://my-bucket/warehouse"
  s3.region: "us-east-1"
  s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```

### Example: Iceberg on OSS

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://my-oss-bucket/warehouse"
  s3.endpoint: "https://oss-cn-hangzhou-internal.aliyuncs.com"
  s3.region: "cn-hangzhou"
  s3.path-style-access: "false"
  s3.access-key-id: "${ALIBABA_CLOUD_ACCESS_KEY_ID}"
  s3.secret-access-key: "${ALIBABA_CLOUD_ACCESS_KEY_SECRET}"
```

### Example: Iceberg REST Catalog

```yaml
connector: iceberg
properties:
  catalog-type: rest
  uri: "https://iceberg-rest-catalog.example.com"
  warehouse: "s3://my-bucket/warehouse"
  credential: "${ICEBERG_REST_CREDENTIAL}"
  oauth2-server-uri: "https://auth.example.com/oauth/token"
```

### Example: Paimon on S3

```yaml
connector: paimon
properties:
  warehouse: "s3://my-bucket/paimon-warehouse"
  s3.endpoint: "https://s3.us-east-1.amazonaws.com"
  s3.region: "us-east-1"
  s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```

---

## JVM Configuration

JVM flags are set per-role in `jvm-coordinator.config` and `jvm-worker.config` (one flag per line).

### Coordinator JVM Sizing Guide

| Node RAM | Recommended `-Xmx` | `MaxDirectMemorySize` |
|----------|--------------------|----------------------|
| 32 GB | `-Xmx12g` | `2g` |
| 64 GB | `-Xmx24g` | `4g` |
| 128 GB | `-Xmx48g` | `8g` |

### Worker JVM Sizing Guide

Workers allocate memory across three pools:

| Component | Setting | Notes |
|-----------|---------|-------|
| JVM heap | `-Xmx` in `jvm-worker.config` | Task metadata, result buffers, sort tables |
| Arrow off-heap | `-XX:MaxDirectMemorySize` in `jvm-worker.config` | Arrow columnar buffers |
| Velox RAM cache | `worker.native.cache.memory-bytes` in `worker.yml` | Velox data cache |

Rule of thumb: `Xmx + MaxDirectMemorySize + native.cache.memory-bytes ≤ 0.85 × total node RAM`

| Node RAM | `-Xmx` | `MaxDirectMemorySize` | Velox RAM cache |
|----------|--------|-----------------------|-----------------|
| 64 GB | `24g` | `8g` | `16g` |
| 128 GB | `32g` | `16g` | `32g` |
| 256 GB | `48g` | `32g` | `128g` |

### Key JVM Flags

| Flag | Value | Purpose |
|------|-------|---------|
| `-XX:+UseG1GC` | — | Use G1 garbage collector (recommended for Java 17+) |
| `-XX:G1HeapRegionSize` | `32m` | G1 region size; `32m` fits mixed Coordinator/Worker workload |
| `-XX:MaxGCPauseMillis` | `200` (coord) / `300` (worker) | Target GC pause time |
| `-XX:InitiatingHeapOccupancyPercent` | `40–45` | Start concurrent GC earlier than G1 default to avoid burst collections |
| `-XX:+ExitOnOutOfMemoryError` | — | Immediately exit on OOM; process manager restarts the node |
| `-XX:+HeapDumpOnOutOfMemoryError` | — | Write heap dump on OOM for post-mortem analysis |
| `-XX:HeapDumpPath` | `/var/log/pangrova/heapdump-*.hprof` | Heap dump destination |
| `-XX:NativeMemoryTracking` | `summary` | Enable `jcmd VM.native_memory` for off-heap diagnostics |
| `-XX:+PreserveFramePointer` | — | Required for CPU flame graphs with async-profiler |
| `-Dfile.encoding` | `UTF-8` | Force UTF-8 for all string I/O |
| `-Duser.timezone` | `UTC` | Force UTC for all timestamp operations |
| `-Djava.net.preferIPv4Stack` | `true` | Prefer IPv4 to avoid dual-stack issues |

---

## Environment Variables (`pangrova-env.sh`)

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `JAVA_HOME` | (system) | Yes | Path to JDK 17+ installation |
| `PANGROVA_HOME` | (auto-detected) | No | Pangrova installation root |
| `PANGROVA_CONF_DIR` | `$PANGROVA_HOME/etc` | No | Configuration directory. Do NOT set in `pangrova-env.sh` (circular dependency) |
| `PANGROVA_LOG_DIR` | `$PANGROVA_HOME/logs` | No | Log and PID file directory |
| `PANGROVA_JAVA_OPTS` | — | No | Override ALL JVM options. Prefer editing `jvm-*.config` for per-role tuning |
| `PANGROVA_DB_HOST` | — | Yes (prod) | PostgreSQL host |
| `PANGROVA_DB_PORT` | `5432` | No | PostgreSQL port |
| `PANGROVA_DB_NAME` | `pangrova_meta` | No | Metadata database name |
| `PANGROVA_DB_USER` | `pangrova` | No | Database username |
| `PANGROVA_DB_PASSWORD` | — | Yes (prod) | Database password. Inject from secrets manager |
| `PANGROVA_OAUTH2_ISSUER_URI` | — | Yes (security) | OIDC issuer URI |
| `PANGROVA_OAUTH2_AUDIENCE` | `pangrova-api` | No | OAuth2 audience |
| `PANGROVA_INTERNAL_AUTH_SECRET` | — | Yes (prod) | Cluster HMAC secret (≥ 48 bytes base64) |
| `PANGROVA_TLS_CERT_FILE` | `/etc/pangrova/tls/server.crt` | No | TLS certificate path |
| `PANGROVA_TLS_KEY_FILE` | `/etc/pangrova/tls/server.key` | No | TLS private key path |
| `PANGROVA_TLS_CA_FILE` | `/etc/pangrova/tls/ca-bundle.crt` | No | CA bundle path |
| `PANGROVA_ADVERTISED_HOST` | (auto-detect) | Yes (prod) | Hostname/IP reachable by other cluster members |
| `PANGROVA_TRANSPORT_PORT` | `9090` | No | Fixed Worker transport port |
| `PANGROVA_COORDINATOR_URL` | — | Yes (Worker) | Coordinator URL for Worker registration |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | No | OTLP gRPC endpoint |
| `OTEL_SERVICE_NAME` | — | No | Service name in telemetry |
| `PANGROVA_INFERENCE_BASE_URL` | — | No | OpenAI-compatible inference endpoint |
| `PANGROVA_EMBEDDING_MODEL` | — | No | Embedding model name |
| `PANGROVA_RERANKER_MODEL` | — | No | Reranker model name |
| `PANGROVA_INFERENCE_API_KEY` | — | No | Inference provider API key |
| `AWS_REGION` | — | S3 | AWS region |
| `AWS_ACCESS_KEY_ID` | — | S3 (static) | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | — | S3 (static) | S3 secret key |
| `AWS_ROLE_ARN` | — | S3 (IAM) | IAM role ARN for S3 access |

---

## See Also

- `docs/develop/deployment/PRODUCTION-CHECKLIST.md` — step-by-step pre-go-live checklist
- `docs/develop/deployment/README.md` — deployment guide (bare-metal, Docker, Helm)
- `docs/develop/configuration/security.yml` — full security configuration reference with comments
- `docs/develop/configuration/pangrova-env.sh` — environment variable template with OS tuning guidance
- `docs/develop/configuration/logback.xml` — logging configuration with async appenders
- `docs/architecture/15-security.md` — security architecture and design rationale
- `docs/architecture/14-observability.md` — observability architecture
