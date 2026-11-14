---
title: "Configuration Key Reference"
description: "Condensed reference of all verified configuration keys, system properties, and environment variables"
---

# Configuration Key Reference

Role and shared options are declared in `org.pangrova.config.constants`. Domain-specific options such as OTLP sampling are validated by their owners. Boolean configuration options accept `true` and `false` case-insensitively. An invalid value fails with the option name.

## YAML Keys — Coordinator (`coordinator.yml`)

| Key | Default | Description |
|-----|---------|-------------|
| `coordinator.id` | `"coordinator-1"` | Unique coordinator identifier |
| `coordinator.host` | `"0.0.0.0"` | HTTP bind address |
| `coordinator.port` | `8080` | HTTP listen port |
| `coordinator.role` | `"COMBINED"` | `QUERY` / `MAINTENANCE` / `COMBINED` |
| `coordinator.ha.enabled` | `false` | Declared in `CoordinatorOptions`; not read by the current bootstrap |
| `coordinator.ha.ddl-leader-routing` | `false` | Route DDL only to the leader |
| `coordinator.query.timeout` | `"5m"` | Query execution timeout |
| `coordinator.query.placement-provider` | `load-balanced` | Installed worker placement strategy ID |
| `coordinator.query.max-concurrent` | `100` | Max concurrent queries |
| `coordinator.task.spill.max-bytes` | `8589934592` | Per-task spill byte ceiling; requires `worker.spill.directory`; zero disables task spilling |
| `coordinator.shuffle.mode` | `NATIVE` | `REMOTE` uses the external shuffle service |
| `coordinator.shuffle.endpoints` | `""` | Comma-separated `host:port`; required for `REMOTE` |
| `coordinator.worker.heartbeat-timeout` | `"30s"` | Worker liveness timeout |
| `coordinator.worker.max-retry` | `3` | Task dispatch retries per worker |
| `metadata.enabled` | `true` | Enable the metadata module |
| `metadata.jdbc-url` | — | Metadata catalog JDBC store |
| `metadata.cache.l1-max-size` | `10000` | L1 metadata cache entries |
| `metadata.cache.l1-ttl` | `"60s"` | L1 metadata cache TTL |

## YAML Keys — Worker (`worker.yml`)

| Key | Default | Description |
|-----|---------|-------------|
| `worker.host` | `"0.0.0.0"` | HTTP bind address |
| `worker.port` | `8081` | HTTP listen port |
| `worker.advertised-host` | — | Hostname advertised to the coordinator |
| `worker.id` | `"worker-1"` | Unique worker identifier |
| `worker.coordinator-url` | — | Coordinator URL to register with |
| `worker.heartbeat-interval` | `"10s"` | Heartbeat interval |
| `worker.transport.port` | `0` | Arrow Flight port (`0` = auto-assign) |
| `worker.transport.tls.enabled` | `false` | Require mutual TLS for Native data transport; required in distributed production |
| `worker.transport.tls.certificate-chain-path` | `""` | Worker PEM certificate chain |
| `worker.transport.tls.private-key-path` | `""` | Worker PEM private key |
| `worker.transport.tls.root-ca-path` | `""` | Peer-verification PEM root CA |
| `worker.transport.tls.identity` | `""` | Advertised certificate DNS identity |
| `worker.copy.task-tls-port` | `0` | Dedicated mTLS COPY task-control port; `0` disables it, and a fixed port enables it for distributed deployment |
| `worker.task.result-retention-seconds` | `300` | Terminal task retention; earlier result authority expiry or resource drain still applies |
| `worker.shuffle.buffer-capacity` | `100` | Batches per shuffle partition buffer |
| `worker.shuffle.max-memory-bytes` | 512 MB | Total shuffle memory budget |
| `worker.shuffle.spill-threshold` | `0.8` | Shuffle spill trigger ratio |
| `worker.memory.budget-bytes` | 512 MB | Native engine memory budget |
| `worker.memory.system-reserved-bytes` | 64 MB | System overhead reserve |
| `worker.memory.node-bytes` | 1 GB | Node memory pool capacity |
| `worker.memory.pressure-threshold-{low,medium,high,critical}` | 50 / 70 / 85 / 95 | Memory pressure levels (%) |
| `worker.threads.query` / `io` / `scheduler` | 4 / 2 / 1 | Thread pool sizes |
| `worker.spill.directory` | `data/worker-spill` | Spill directory; empty disables spill |
| `worker.shuffle.mode` | `NATIVE` | `NATIVE` or `REMOTE`; must match the Coordinator |
| `worker.native.path` | — | Explicit native library path |
| `worker.native.workdir` | — | Native library extraction directory |
| `worker.native.delete-after-load` | `true` | Delete extracted library after load |
| `worker.native.cache.enabled` | `false` | Enable Velox async data cache |
| `worker.native.cache.memory-bytes` | 8 GB | RAM cache budget |
| `worker.native.cache.ssd-path` | `""` | SSD cache directory |
| `worker.native.cache.ssd-bytes` | 100 GB | SSD cache capacity |
| `worker.native.cache.ssd-shards` | `8` | SSD cache shards |
| `worker.native.cache.checkpoint-interval-bytes` | 1 GB | SSD cache index checkpoint interval |
| `worker.registration.max-attempts` | `5` | Registration attempts |
| `worker.registration.initial-backoff` / `max-backoff` | `"1s"` / `"30s"` | Registration retry backoff |
| `worker.cdc.enabled` | `true` | Enable CDC processing |
| `worker.internal-auth-secret` | — | Shared `X-Internal-Auth` secret |
| `worker.ai.authorization.*` | fail closed | AI model/capability/permission allowlists and invocation ceilings; see [Worker configuration](worker.md#workerai) |
| `worker.ai.models.<id>.*` | — | Exact versioned provider/model descriptor; no implicit model or credential fallback |
| `worker.ai.file.*` | — | Governed FILE S3 backend, external capability secret, limits and HTTPS-import policy; required when AI functions are registered |

## Feature Flags (shared)

| Key | Default | Description |
|-----|---------|-------------|
| `governance.enabled` | `true` | Governance module |
| `resilience.enabled` | `true` | Resilience module |
| `streaming.enabled` | `true` | Streaming module |
| `tenant.enabled` | `false` | Multi-tenant module; requires authenticated security (not NOOP) |
| `indexing.enabled` | `true` | Indexing module |
| `operations.enabled` | `true` | Operations module |
| `observability.enabled` | `true` | Observability module |
| `security.enabled` | `false` | Security module (requires `security.auth.type`) |
| `security.auth.type` | — | `OAUTH2` / `LDAP` / `NOOP` (dev) |

## JVM System Properties

Coordinator:

| Property | Default | Description |
|----------|---------|-------------|
| `pangrova.coordination.lease.backend` | `jdbc` | Lease backend: `jdbc` / `zookeeper` |
| `pangrova.coordination.lease.zookeeper.connectString` | — | ZK ensemble (zookeeper backend) |
| `pangrova.coordination.lease.zookeeper.basePath` | `/pangrova/leases` | ZK lease parent znode |
| `pangrova.http.max-requests-per-minute` | `100` | HTTP request rate limit |
| `pangrova.index.basePath` | — (required when indexing is enabled) | Index artifact directory; no default |

## Environment Variables

Launch scripts (`bin/`):

| Variable | Description |
|----------|-------------|
| `PANGROVA_HOME` | Installation root |
| `PANGROVA_CONF_DIR` | Config directory (default: `$PANGROVA_HOME/etc`) |
| `PANGROVA_LOG_DIR` | Log directory |
| `PANGROVA_JAVA_OPTS` | Extra JVM options |
| `PANGROVA_ROLE` | Logback log role and filename override; it does not select the process role |
| `PANGROVA_COORDINATOR_URL` | Coordinator URL for worker scripts |

Config overrides: any YAML key can be overridden by a `PANGROVA_*` environment
variable (e.g. `PANGROVA_COORDINATOR_PORT=9090` → `coordinator.port`). See
[Configuration model](index.md) for the full mapping rules and priority order.

**See Also:** [Configuration model](index.md), [Coordinator](coordinator.md),
[Worker](worker.md), [Connector](connectors.md)
