---
title: Configuration Reference
sidebar_label: Configuration Reference
---

# Pangrova — Configuration Reference

> **Summary.** The complete configuration-key reference for coordinators, workers, and connectors.

## 1. Configuration Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Configuration Overview                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Configuration categories:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Category          │ Hot reload │ Config file           │ Description      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Coordinator       │ Partial │ coordinator.yml       │ Coordinator config  │   │
│  │  Worker            │ Partial │ worker.yml            │ Worker config       │   │
│  │  Connector         │ ✗       │ catalog/*.yml         │ Data source config  │   │
│  │  Resource          │ ✓       │ resource-groups.yml   │ Resource queue cfg   │   │
│  │  Security          │ ✓       │ security.yml          │ Security config      │   │
│  │  Observability     │ ✓       │ observability.yml     │ Observability config │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Coordinator Configuration

> **Protocol mapping.** The Coordinator exposes an HTTP (REST) interface (control plane). For the full protocol mapping, see [System Overview §4.6](./01-overview.md#46-canonical-protocol-mapping).

Example configuration (can be placed in `pangrova.conf` or in environment variables):

```yaml
# Control-plane protocol - fixed to rest
pangrova.control.protocol: "rest"
# Data-plane protocol - Arrow Flight for high throughput
pangrova.data.protocol: "arrow-flight"
# Points to the proto version persisted in the pangrova-protocol module (built-in / upgrade use only)
pangrova.internal.proto.version: "1"
```

### 2.1 Core Configuration

```yaml
# coordinator.yaml

coordinator:
  # Basic configuration
  id: coordinator-1
  environment: production
  
  # Network configuration — Control Plane: HTTP (REST)
  http:
    port: 8080
    max-threads: 200
    idle-timeout: 30s
  
  # Cluster configuration
  cluster:
    discovery:
      type: zookeeper  # zookeeper | static | kubernetes
      zookeeper:
        connect-string: zk1:2181,zk2:2181,zk3:2181
        session-timeout: 30s
        base-path: /pangrova
    
    # High-availability configuration
    ha:
      enabled: true
      leader-election-timeout: 10s
  
  # Query configuration
  query:
    max-concurrent-queries: 1000
    max-queued-queries: 5000
    max-execution-time: 30m
    max-planning-time: 5m
    
  # Scheduler configuration
  scheduler:
    split-batch-size: 1000
    max-splits-per-task: 1000
    min-drivers-per-task: 1
    max-drivers-per-task: 16
```

### 2.2 Optimizer Configuration

```yaml
  # Optimizer configuration
  optimizer:
    # Calcite configuration
    calcite:
      default-null-collation: LOW
      case-sensitive: false
    
    # Rule configuration
    rules:
      filter-pushdown: true
      projection-pushdown: true
      join-reorder: true
      predicate-pushdown: true
    
    # Cost model
    cost-model:
      cpu-cost-weight: 1.0
      io-cost-weight: 10.0
      network-cost-weight: 100.0
    
    # Join optimization
    join:
      broadcast-threshold: 10MB
      hash-partition-count: 100
      prefer-streaming-join: false
```

---

## 3. Worker Configuration

> **Protocol mapping.** The Worker receives control commands from the Coordinator over HTTP (REST) (control plane) and exchanges shuffle data / streams results over Arrow Flight (gRPC) (data plane). For the full protocol mapping, see [System Overview §4.6](./01-overview.md#46-canonical-protocol-mapping).

### 3.1 Core Configuration

```yaml
# worker.yaml

worker:
  # Basic configuration
  id: worker-1
  coordinator-url: http://coordinator:8080
  
  # Network configuration — Control Plane: HTTP (REST)
  # The Worker receives task dispatch and status queries from the Coordinator over HTTP
  http:
    port: 8081
    max-threads: 100
  
  # Task configuration
  task:
    max-concurrent-tasks: 100
    task-timeout: 30m
    heartbeat-interval: 10s
    
  # Native configuration
  native:
    library-path: /opt/pangrova/lib/libpangrova_native.so
    thread-pool-size: 16
    
    # Velox configuration
    velox:
      cache-enabled: true
      cache-size: 10GB
      spill-enabled: true
      spill-path: /data/spill
  
  # Data Plane configuration — Arrow Flight (gRPC)
  # Used for Worker ↔ Worker shuffle data exchange and Worker → Client result streaming
  arrow-flight:
    port: 8082
    max-concurrent-streams: 100
    max-message-size: 16MB
    flow-control:
      initial-window-size: 1MB
      max-window-size: 16MB
```

### 3.2 Memory Configuration

```yaml
  # Memory configuration
  memory:
    # JVM heap memory
    heap-headroom: 2GB
    
    # Native memory
    native:
      max-memory: 64GB
      reserved-memory: 4GB
      
    # Memory pool configuration
    pools:
      general:
        max-size: 40GB
      reserved:
        max-size: 8GB
      system:
        max-size: 4GB
    
    # Spill configuration
    spill:
      enabled: true
      path: /data/spill
      max-size: 500GB
      threshold: 0.8
      compression: zstd
```

### 3.3 I/O Configuration

```yaml
  # I/O configuration
  io:
    # Read configuration
    read:
      buffer-size: 8MB
      prefetch-enabled: true
      prefetch-size: 64MB
      
    # Write configuration
    write:
      buffer-size: 4MB
      flush-interval: 1s
      
    # File-system configuration
    filesystem:
      s3:
        max-connections: 500
        connect-timeout: 10s
        socket-timeout: 30s
        max-retries: 3
      hdfs:
        replication: 3
        block-size: 128MB
```

---

## 4. Connector Configuration

### 4.1 Iceberg Connector

```yaml
# catalog/iceberg.yaml

connector:
  name: iceberg

  # Catalog configuration
  iceberg:
    catalog-type: hive  # hive | rest | jdbc | glue
    catalog-impl: org.apache.iceberg.hive.HiveCatalog

    # Hive Metastore
    hive:
      metastore:
        uri: thrift://hive-metastore:9083
        warehouse-dir: s3://warehouse/iceberg

    # Default table properties
    file-format: PARQUET
    compression-codec: ZSTD
    target-max-file-size: 128MB

    # Read optimization
    split-size: 128MB
    max-splits-per-partition: 100

    # Write configuration
    commit-retries: 4
    commit-retry-wait-ms: 100
```

### 4.2 Paimon Connector

```yaml
# catalog/paimon.yaml

connector:
  name: paimon

  # Catalog configuration
  paimon:
    catalog-type: filesystem
    warehouse: s3://warehouse/paimon

    # Default table properties
    file-format: parquet
    bucket: 16
    changelog-producer: input

    # Read configuration
    scan:
      mode: latest
      snapshot-id:

    # Write configuration
    write-buffer-size: 256MB
    page-size: 64KB
```

### 4.3 S3 File System

```yaml
# catalog/s3.yaml

s3:
  # Authentication configuration — plaintext credentials are forbidden; a Secret Reference must be used
  access-key: ${ENV:AWS_ACCESS_KEY_ID}
  secret-key: ${VAULT:secret/pangrova/s3#secret-key}
  region: us-east-1

  # Or use an IAM Role (recommended, no key management needed)
  iam-role: arn:aws:iam::123456789:role/pangrova-s3-role

  # Endpoint configuration (private deployment)
  endpoint: https://s3.example.com
  path-style-access: true

  # Performance configuration
  max-connections: 500
  connect-timeout: 10s
  socket-timeout: 30s
  max-error-retries: 3

  # Transfer configuration
  multipart-upload-threshold: 100MB
  multipart-upload-part-size: 64MB
```

---

## 5. Resource Management Configuration

### 5.1 Resource Queues

```yaml
# resource-groups.yaml

resource-groups:
  # Root queue
  root:
    max-queued: 1000
    scheduling-policy: weighted
    
    # Sub-queues
    sub-groups:
      # Interactive query queue
      interactive:
        max-queued: 100
        max-running: 50
        scheduling-weight: 100
        soft-memory-limit: 20GB
        hard-memory-limit: 30GB
        soft-cpu-limit: 10m
        hard-cpu-limit: 30m
        
      # Batch queue
      batch:
        max-queued: 500
        max-running: 100
        scheduling-weight: 50
        soft-memory-limit: 100GB
        hard-memory-limit: 200GB
        soft-cpu-limit: 2h
        hard-cpu-limit: 6h
        
      # ETL queue
      etl:
        max-queued: 200
        max-running: 20
        scheduling-weight: 30
        soft-memory-limit: 50GB
        hard-memory-limit: 100GB

# Selector rules
selectors:
  - user: "etl_.*"
    group: etl
    
  - user: ".*"
    group: batch
```

### 5.2 Rate-Limiting Configuration

```yaml
# rate-limiting.yaml

rate-limiting:
  # Global rate limiting
  global:
    max-concurrent-queries: 500
    max-qps: 1000
    
  # Per-user rate limiting
  user:
    default:
      max-concurrent: 10
      max-qps: 100
      max-daily-queries: 10000
      max-daily-data-scanned: 1TB
      
    # Specific user overrides
    overrides:
      admin:
        max-concurrent: 50
        max-qps: 500
      analyst:
        max-concurrent: 20
        max-qps: 200
        
  # Per-tenant rate limiting
  tenant:
    default:
      max-concurrent: 100
      max-qps: 500
```

---

## 6. Security Configuration

### 6.1 Authentication Configuration

```yaml
# security.yaml

security:
  # Authentication configuration
  authentication:
    type: ldap  # password | ldap | oauth2 | kerberos
    
    ldap:
      url: ldap://ldap.example.com:389
      user-base-dn: ou=users,dc=example,dc=com
      user-search-filter: (uid={0})
      bind-dn: cn=admin,dc=example,dc=com
      bind-password: ${VAULT:secret/pangrova/ldap#bind-password}
      
    oauth2:
      issuer-uri: https://auth.example.com
      client-id: pangrova
      client-secret: ${FILE:/run/secrets/oauth2-client-secret}
      
  # Authorization configuration
  authorization:
    type: file  # file | ranger | opa
    
    file:
      rules-file: /etc/pangrova/access-control.json
      refresh-interval: 60s
```

### 6.2 Encryption Configuration

```yaml
  # Encryption configuration
  encryption:
    # Transport encryption
    tls:
      enabled: true
      keystore-path: /etc/pangrova/keystore.jks
      keystore-password: ${KMS:alias/pangrova-tls-keystore}
      truststore-path: /etc/pangrova/truststore.jks
      truststore-password: ${KMS:alias/pangrova-tls-truststore}
      
    # Data encryption
    data:
      enabled: false
      key-provider: kms  # kms | vault
      kms:
        key-id: alias/pangrova-data-key
```

---

## 7. Observability Configuration

### 7.1 Metrics Configuration

```yaml
# observability.yaml

observability:
  # Prometheus metrics
  metrics:
    enabled: true
    port: 9090
    path: /metrics
    
    # Label configuration
    labels:
      cluster: production
      region: us-east-1
      
    # Histogram bucket configuration
    histograms:
      query-latency:
        buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60, 120]
      scan-rows:
        buckets: [1000, 10000, 100000, 1000000, 10000000]
```

### 7.2 Tracing Configuration

```yaml
  # OpenTelemetry tracing
  tracing:
    enabled: true
    exporter: otlp  # otlp | jaeger | zipkin
    
    otlp:
      endpoint: http://otel-collector:4317
      
    # Sampling configuration
    sampling:
      type: probability  # always | never | probability | rate-limiting
      probability: 0.1
      rate-limit: 100  # traces per second
```

### 7.3 Logging Configuration

```yaml
  # Logging configuration
  logging:
    level: INFO
    
    # Per-module levels
    levels:
      org.pangrova.coordinator: INFO
      org.pangrova.worker: INFO
      org.pangrova.connector: DEBUG
      org.pangrova.native: WARN
      
    # Audit log
    audit:
      enabled: true
      path: /var/log/pangrova/audit.log
      max-size: 100MB
      max-files: 10
```

---

## 8. Operations Configuration

### 8.1 Autoscaling

```yaml
# auto-scaling.yaml

auto-scaling:
  enabled: true
  
  # Scaling boundaries
  min-workers: 3
  max-workers: 100
  
  # Scaling thresholds
  scale-out:
    cpu-threshold: 0.8
    memory-threshold: 0.85
    queue-depth-threshold: 100
    
  scale-in:
    cpu-threshold: 0.3
    memory-threshold: 0.4
    queue-depth-threshold: 10
    
  # Cooldown periods
  cooldown:
    scale-out: 3m
    scale-in: 10m
    
  # Predictive autoscaling
  predictive:
    enabled: true
    lookahead: 15m
    min-confidence: 0.8
```

### 8.2 Maintenance Configuration

```yaml
# maintenance.yaml

maintenance:
  # Small-file compaction
  compaction:
    enabled: true
    interval: 1h
    target-file-size: 128MB
    min-files-to-compact: 5
    max-concurrent-tasks: 4
    
  # Orphan-file cleanup
  orphan-cleanup:
    enabled: true
    interval: 24h
    retention-period: 7d
    max-files-per-run: 10000
    
  # Snapshot expiration
  snapshot-expire:
    enabled: true
    interval: 6h
    retention-period: 7d
    retain-count: 100
```

---

## 9. Environment Variable Reference

| Environment variable | Description | Default value |
|----------|------|--------|
| `PANGROVA_HOME` | Installation directory | `/opt/pangrova` |
| `PANGROVA_CONF_DIR` | Configuration directory | `$PANGROVA_HOME/etc` |
| `PANGROVA_LOG_DIR` | Log directory | `/var/log/pangrova` |
| `PANGROVA_DATA_DIR` | Data directory | `/data/pangrova` |
| `JAVA_HOME` | JDK path | - |
| `JAVA_OPTS` | JVM arguments | `-Xmx16g` |
| `PANGROVA_NODE_ID` | Node ID | Hostname |
| `PANGROVA_COORDINATOR_URL` | Coordinator address | - |

---

## 10. Configuration Priority

<!-- Resolves: DEF-201 — Added conflict resolution rules for ZK unavailability, SET CONFIG storage layer, and RESET CONFIG semantics. -->

```
1. Command-line arguments (highest priority)
2. Environment variables
3. Dynamic configuration center (ZooKeeper)
4. Configuration files (*.yaml)
5. Default values (lowest priority)
```

**Conflict Resolution Rules**:
- **ZK unavailable**: Fall back to last-known ZK value (local cache). If no cached value exists, fall back to config file (level 4).
- **`SET CONFIG 'key' = 'value'`**: Writes the override to ZooKeeper (level 3). Takes effect immediately on all nodes via ZK watch.
- **`RESET CONFIG 'key'`**: Deletes the ZK override for that key, causing the effective value to fall back to config file (level 4) or default (level 5).

---

## 11. Configuration Validation

### 11.1 General Validation

```bash
# Validate configuration file syntax
pangrova-config validate --config /etc/pangrova/coordinator.yaml

# Show effective configuration
pangrova-config show --effective

# Check configuration compatibility
pangrova-config check-compatibility --from 1.0.0 --to 2.0.0
```

### 11.2 Credential Validation (Startup Credential Check)

At startup, Pangrova runs a credential security scan over all configuration files and **rejects configurations that contain plaintext credentials**.

**Validation rules**:
- Scan all configuration keys whose field name contains `password`, `secret`, or `key` (excluding `public-key`)
- If the field value is non-empty and does not match the `${...}` Secret Reference pattern, reject startup
- Emit a clear error message identifying the offending field path

**Error example**:
```
ERROR: Plaintext credential detected in configuration.
  Field: s3.secret-key
  Value: ****
  Expected: Secret reference syntax, e.g. ${ENV:VAR_NAME}, ${VAULT:path}, ${KMS:key-id}, ${FILE:path}
  Action: Replace plaintext value with a secret reference. See docs/architecture/21-configuration-reference.md Section 12.

Pangrova will not start with plaintext credentials in configuration.
```

**Supported Secret Reference formats**:

| Format | Description | Example |
|------|------|------|
| `${ENV:VAR_NAME}` | Environment variable reference | `${ENV:AWS_ACCESS_KEY_ID}` |
| `${VAULT:path#field}` | HashiCorp Vault reference | `${VAULT:secret/pangrova/s3#key}` |
| `${KMS:key-id}` | AWS KMS / cloud KMS reference | `${KMS:alias/pangrova-ldap}` |
| `${FILE:path}` | File reference (K8s Secret mount) | `${FILE:/run/secrets/oauth2}` |

**Validation pseudocode**:
```
for each config field where name matches /(password|secret|key)/i
    and name does NOT match /public[_-]?key/i:
  if value is non-empty AND value does NOT match /^\$\{(ENV|VAULT|KMS|FILE):.*\}$/:
    reject startup with error message
```

---

## 12. Configuration Parameter Quick Reference

All configuration parameters with defaults, ranges, units, and required status. Organized by category for quick lookup.

### 12.1 Coordinator

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `coordinator.id` | hostname | — | — | Yes | No | Unique node identifier |
| `coordinator.http.port` | `8080` | 1024–65535 | — | No | No | HTTP listen port |
| `coordinator.http.max-threads` | `200` | 10–2000 | — | No | No | HTTP thread pool size |
| `coordinator.http.idle-timeout` | `30s` | 5s–300s | duration | No | Yes | Idle connection timeout |
| `coordinator.cluster.discovery.type` | `zookeeper` | zookeeper / static / kubernetes | — | Yes | No | Service discovery type |
| `coordinator.cluster.ha.enabled` | `true` | true / false | — | No | No | Enable HA leader election |
| `coordinator.cluster.ha.leader-election-timeout` | `10s` | 5s–60s | duration | No | No | Leader election timeout |
| `coordinator.query.max-concurrent-queries` | `1000` | 1–100000 | — | No | Yes | Max concurrent queries |
| `coordinator.query.max-queued-queries` | `5000` | 0–1000000 | — | No | Yes | Max queued queries |
| `coordinator.query.max-execution-time` | `30m` | 1m–24h | duration | No | Yes | Query execution timeout |
| `coordinator.query.max-planning-time` | `5m` | 10s–30m | duration | No | Yes | Query planning timeout |
| `coordinator.scheduler.split-batch-size` | `1000` | 1–100000 | — | No | Yes | Splits per scheduling batch |
| `coordinator.scheduler.max-splits-per-task` | `1000` | 1–100000 | — | No | Yes | Max splits assigned per task |
| `coordinator.scheduler.min-drivers-per-task` | `1` | 1–64 | — | No | No | Min pipeline drivers per task |
| `coordinator.scheduler.max-drivers-per-task` | `16` | 1–256 | — | No | No | Max pipeline drivers per task |
| `coordinator.scheduler.output-worker.scheduling-timeout` | `5s` | 1s–60s | duration | No | Yes | Output Worker scheduling timeout |
| `coordinator.scheduler.retry.max-retries-external` | `3` | 0–10 | — | No | Yes | Max retries for external errors |
| `coordinator.scheduler.retry.max-retries-resource` | `1` | 0–5 | — | No | Yes | Max retries for resource errors |
| `coordinator.scheduler.retry.retry-backoff-initial` | `1s` | 100ms–30s | duration | No | Yes | Initial retry backoff |
| `coordinator.scheduler.retry.retry-backoff-max` | `30s` | 1s–300s | duration | No | Yes | Max retry backoff |
| `coordinator.scheduler.retry.retry-backoff-multiplier` | `2.0` | 1.0–10.0 | — | No | Yes | Retry backoff multiplier |
| `coordinator.scheduler.stage.max-concurrent-stages` | `16` | 1–256 | — | No | Yes | Max concurrent stages per query |
| `coordinator.scheduler.stage.task-submit-timeout` | `10s` | 1s–120s | duration | No | Yes | Task submission timeout |

<!-- Resolves: DEF-046 — Added scheduler.output-worker.*, scheduler.retry.*, scheduler.stage.* params; warm-pool keys removed with G-SCHED-08 -->

### 12.2 Optimizer

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `optimizer.calcite.default-null-collation` | `LOW` | LOW / HIGH / FIRST / LAST | — | No | No | NULL sort order |
| `optimizer.calcite.case-sensitive` | `false` | true / false | — | No | No | Identifier case sensitivity |
| `optimizer.rules.filter-pushdown` | `true` | true / false | — | No | Yes | Enable filter pushdown |
| `optimizer.rules.projection-pushdown` | `true` | true / false | — | No | Yes | Enable projection pushdown |
| `optimizer.rules.join-reorder` | `true` | true / false | — | No | Yes | Enable join reorder |
| `optimizer.rules.predicate-pushdown` | `true` | true / false | — | No | Yes | Enable predicate pushdown |
| `optimizer.cost-model.cpu-cost-weight` | `1.0` | 0.0–1000.0 | — | No | Yes | CPU cost weight |
| `optimizer.cost-model.io-cost-weight` | `10.0` | 0.0–1000.0 | — | No | Yes | I/O cost weight |
| `optimizer.cost-model.network-cost-weight` | `100.0` | 0.0–1000.0 | — | No | Yes | Network cost weight |
| `optimizer.join.broadcast-threshold` | `10MB` | 0–1GB | bytes | No | Yes | Max table size for broadcast join. Set to `0` to disable broadcast join entirely. Set to `-1` for automatic (CBO decides based on available Worker memory × 10%). |
| `optimizer.join.broadcast-join-max-rows` | `1000000` | 0–100000000 | rows | No | Yes | Safety guard: even if size < threshold, do not broadcast if row count exceeds this value |
| `optimizer.join.hash-partition-count` | `100` | 1–10000 | — | No | Yes | Hash partition count for shuffle join |

<!-- Resolves: OPT-020 — Broadcast Join Threshold Configuration -->

> **Broadcast Join Tuning Guide**:
> - **Memory-rich clusters** (Workers with 128GB+): increase `broadcast-threshold` to `100MB` for more broadcast join opportunities, reducing shuffle overhead.
> - **Memory-constrained clusters**: keep default `10MB` or lower. Broadcast join replicates the build side to all Workers — oversized broadcasts cause OOM.
> - **High-cardinality dimension tables**: use `broadcast-join-max-rows` as a safety guard. A 10MB table with 10M rows may cause excessive hash table probe overhead even though it fits in memory.
> - **Automatic mode** (`-1`): the optimizer uses `workerAvgMemory × 0.1` as the threshold, adapting to cluster size. Recommended for heterogeneous clusters.
> - **Cross-reference**: The `BroadcastJoinRule` in the VolcanoPlanner uses these thresholds. AQE (OPT-001 in [02 §16](./03-query-execution.md)) can dynamically convert shuffle joins to broadcast joins at runtime when actual build-side size is below threshold.

### 12.3 Worker

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `worker.id` | hostname | — | — | Yes | No | Unique node identifier |
| `worker.coordinator-url` | — | — | URL | Yes | No | Coordinator HTTP endpoint |
| `worker.http.port` | `8081` | 1024–65535 | — | No | No | HTTP listen port |
| `worker.http.max-threads` | `100` | 10–2000 | — | No | No | HTTP thread pool size |
| `worker.task.max-concurrent-tasks` | `100` | 1–10000 | — | No | Yes | Max concurrent tasks |
| `worker.task.task-timeout` | `30m` | 1m–24h | duration | No | Yes | Task execution timeout |
| `worker.task.heartbeat-interval` | `10s` | 1s–60s | duration | No | No | Heartbeat interval to Coordinator |
| `worker.native.library-path` | `/opt/pangrova/lib/libpangrova_native.so` | — | path | Yes | No | Native library path |
| `worker.native.thread-pool-size` | `16` | 1–256 | — | No | No | Native execution thread pool |
| `worker.arrow-flight.port` | `8082` | 1024–65535 | — | No | No | Arrow Flight listen port |
| `worker.arrow-flight.max-concurrent-streams` | `100` | 1–10000 | — | No | Yes | Max concurrent Flight streams |
| `worker.arrow-flight.max-message-size` | `16MB` | 1MB–256MB | bytes | No | No | Max Flight message size |
| `worker.task.exchange.max-buffer-size` | `64MB` | 8MB–512MB | bytes | No | Yes | Exchange client buffer size |
| `worker.task.exchange.max-concurrent-requests` | `8` | 1–64 | — | No | Yes | Max concurrent exchange requests |
| `worker.task.exchange.request-timeout` | `30s` | 5s–300s | duration | No | Yes | Exchange request timeout |
| `worker.task.exchange.idle-timeout` | `60s` | 10s–600s | duration | No | Yes | Exchange idle connection timeout |
| `worker.task.output.max-buffered-bytes` | `32MB` | 4MB–256MB | bytes | No | Yes | Output buffer max bytes |
| `worker.task.output.max-buffered-batches` | `64` | 4–1024 | — | No | Yes | Output buffer max batches |
| `worker.task.output.client-idle-timeout` | `5m` | 30s–30m | duration | No | Yes | Output client idle timeout |
| `worker.task.shuffle.max-buffer-size` | `128MB` | 16MB–1GB | bytes | No | Yes | Shuffle buffer max size |
| `worker.task.shuffle.partition-buffer-size` | `16MB` | 1MB–128MB | bytes | No | Yes | Per-partition shuffle buffer |
| `worker.flight.port` | `8082` | 1024–65535 | — | No | No | Flight server port |
| `worker.flight.max-message-size` | `16MB` | 1MB–256MB | bytes | No | No | Flight max message size |
| `worker.flight.flow-control-window` | `4MB` | 256KB–64MB | bytes | No | No | Flight flow control window |
| `worker.native.velox.cache-enabled` | `true` | true / false | — | No | No | Enable Velox data cache |
| `worker.native.velox.cache-size` | `10GB` | 1GB–256GB | bytes | No | No | Velox data cache size |
| `worker.native.velox.spill-enabled` | `true` | true / false | — | No | Yes | Enable Native-layer spill (controls Velox SpillManager; Java-layer spill is controlled by `memory.spill.enabled` in §12.4) |
| `worker.native.velox.spill-path` | `/data/spill` | — | path | Conditional | No | Velox spill directory (required if spill enabled) |
| `worker.arrow-flight.flow-control.initial-window-size` | `1MB` | 64KB–16MB | bytes | No | No | Arrow Flight flow-control initial window size |
| `worker.arrow-flight.flow-control.max-window-size` | `16MB` | 1MB–256MB | bytes | No | No | Arrow Flight flow-control max window size |

<!-- Resolves: DEF-200 — Added Velox engine and Arrow Flight flow-control params to Worker table -->
<!-- Resolves: DEF-046 — Added task.exchange.*, task.output.*, task.shuffle.*, flight.* params; slot reservation keys removed with G-SCHED-08 -->

### 12.4 Memory

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `memory.heap-headroom` | `2GB` | 512MB–16GB | bytes | No | No | JVM heap headroom |
| `memory.native.max-memory` | `64GB` | 1GB–1TB | bytes | No | No | Native max memory |
| `memory.native.reserved-memory` | `4GB` | 256MB–64GB | bytes | No | No | Native reserved memory |
| `memory.pools.general.max-size` | `40GB` | 1GB–512GB | bytes | No | No | General pool max size |
| `memory.pools.reserved.max-size` | `8GB` | 512MB–128GB | bytes | No | No | Reserved pool max size |
| `memory.pools.system.max-size` | `4GB` | 256MB–64GB | bytes | No | No | System pool max size |
| `memory.spill.enabled` | `true` | true / false | — | No | Yes | Enable spill to disk |
| `memory.spill.path` | `/data/spill` | — | path | Conditional | No | Spill directory (required if spill enabled) |
| `memory.spill.max-size` | `500GB` | 10GB–10TB | bytes | No | No | Max spill disk usage |
| `memory.spill.threshold` | `0.8` | 0.5–0.95 | ratio | No | Yes | Memory usage ratio to trigger spill |
| `memory.spill.compression` | `zstd` | zstd / lz4 / snappy / none | — | No | No | Spill compression codec |

### 12.5 I/O

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `io.read.buffer-size` | `8MB` | 64KB–64MB | bytes | No | No | Read buffer size |
| `io.read.prefetch-enabled` | `true` | true / false | — | No | Yes | Enable read prefetch |
| `io.read.prefetch-size` | `64MB` | 1MB–256MB | bytes | No | Yes | Prefetch window size |
| `io.write.buffer-size` | `4MB` | 64KB–64MB | bytes | No | No | Write buffer size |
| `io.write.flush-interval` | `1s` | 100ms–60s | duration | No | Yes | Write flush interval |
| `io.filesystem.s3.max-connections` | `500` | 10–5000 | — | No | No | S3 max connections |
| `io.filesystem.s3.connect-timeout` | `10s` | 1s–60s | duration | No | No | S3 connect timeout |
| `io.filesystem.s3.socket-timeout` | `30s` | 5s–300s | duration | No | No | S3 socket timeout |
| `io.filesystem.s3.max-retries` | `3` | 0–10 | — | No | No | S3 max retries |

### 12.6 Connector (Iceberg)

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `iceberg.catalog-type` | `hive` | hive / rest / jdbc / glue | — | Yes | No | Catalog backend type |
| `iceberg.file-format` | `PARQUET` | PARQUET / ORC / AVRO | — | No | No | Default file format |
| `iceberg.compression-codec` | `ZSTD` | ZSTD / SNAPPY / GZIP / LZ4 / NONE | — | No | No | Compression codec |
| `iceberg.target-max-file-size` | `128MB` | 8MB–1GB | bytes | No | Yes | Target max data file size |
| `iceberg.split-size` | `128MB` | 16MB–1GB | bytes | No | Yes | Split size for reading |
| `iceberg.max-splits-per-partition` | `100` | 1–10000 | — | No | Yes | Max splits per partition |
| `iceberg.commit-retries` | `4` | 1–20 | — | No | No | Commit retry count |
| `iceberg.commit-retry-wait-ms` | `100` | 10–10000 | ms | No | No | Commit retry wait time |

<!-- Resolves: DEF-194 — Added Paimon Connector and S3 FileSystem Quick Reference tables -->

### 12.6.2 Connector (Paimon)

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `paimon.catalog-type` | `filesystem` | filesystem / hive / jdbc | — | Yes | No | Catalog backend type |
| `paimon.warehouse` | — | — | path | Yes | No | Warehouse root path (S3/HDFS) |
| `paimon.file-format` | `orc` | orc / parquet / avro | — | No | No | Default file format |
| `paimon.bucket` | `16` | 1–4096 | — | No | No | Default bucket count |
| `paimon.changelog-producer` | `none` | none / input / lookup / full-compaction | — | No | No | Changelog producer mode |
| `paimon.scan.mode` | `latest-full` | latest-full / latest / from-timestamp / from-snapshot | — | No | Yes | Scan mode for reads |
| `paimon.write-buffer-size` | `256MB` | 16MB–2GB | bytes | No | Yes | Write buffer size per bucket |
| `paimon.page-size` | `64KB` | 4KB–1MB | bytes | No | No | Page size for columnar storage |

### 12.6.3 S3 FileSystem

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `s3.endpoint` | — | — | URL | Conditional | No | S3-compatible endpoint (required for non-AWS) |
| `s3.region` | `us-east-1` | — | — | No | No | AWS region |
| `s3.access-key` | — | — | — | Conditional | No | Access key (if not using IAM role) |
| `s3.secret-key` | — | — | — | Conditional | No | Secret key (if not using IAM role) |
| `s3.path-style-access` | `false` | true / false | — | No | No | Use path-style access (required for MinIO) |
| `s3.max-connections` | `500` | 10–5000 | — | No | No | Max HTTP connections |
| `s3.multipart-upload.part-size` | `64MB` | 5MB–5GB | bytes | No | No | Multipart upload part size |
| `s3.multipart-upload.threshold` | `128MB` | 16MB–5GB | bytes | No | No | Threshold to trigger multipart upload |
| `s3.transfer.max-bandwidth` | `unlimited` | — | bytes/s | No | Yes | Max transfer bandwidth per node |
| `s3.ssl.enabled` | `true` | true / false | — | No | No | Enable SSL for S3 connections |

### 12.7 Resource Queue

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `resource-groups.root.max-queued` | `1000` | 0–1000000 | — | No | Yes | Root queue max queued |
| `resource-groups.root.scheduling-policy` | `weighted` | weighted / fair / fifo | — | No | Yes | Scheduling policy |
| `resource-groups.*.max-running` | — | 1–100000 | — | Yes | Yes | Max running queries per queue |
| `resource-groups.*.scheduling-weight` | `100` | 1–10000 | — | No | Yes | Queue scheduling weight |
| `resource-groups.*.soft-memory-limit` | — | 1GB–1TB | bytes | No | Yes | Soft memory limit |
| `resource-groups.*.hard-memory-limit` | — | 1GB–1TB | bytes | Yes | Yes | Hard memory limit |
| `resource-groups.*.soft-cpu-limit` | — | 1s–24h | duration | No | Yes | Soft CPU time limit |
| `resource-groups.*.hard-cpu-limit` | — | 1s–24h | duration | No | Yes | Hard CPU time limit |

### 12.8 Security

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `security.authentication.type` | `password` | password / ldap / oauth2 / kerberos | — | Yes | No | Authentication type |
| `security.authorization.type` | `file` | file / ranger / opa | — | No | No | Authorization type |
| `security.authorization.file.refresh-interval` | `60s` | 10s–3600s | duration | No | Yes | ACL file refresh interval |
| `security.encryption.tls.enabled` | `true` | true / false | — | No | No | Enable TLS |
| `security.rls.enabled` | `false` | true / false | — | No | Yes | Enable Row-Level Security |
| `security.rls.cache-ttl` | `5m` | 30s–1h | duration | No | Yes | RLS policy cache TTL |
| `security.column-masking.enabled` | `false` | true / false | — | No | Yes | Enable column masking |
| `security.column-masking.cache-ttl` | `5m` | 30s–1h | duration | No | Yes | Column mask cache TTL |
| `security.audit.enabled` | `true` | true / false | — | No | Yes | Enable security audit logging |
| `security.audit.async` | `true` | true / false | — | No | No | Async audit event emission |
| `security.audit.batch-size` | `100` | 1–10000 | — | No | Yes | Audit event batch size |
| `security.secret.provider` | `env` | env / vault / kms | — | Yes | No | Secret management provider |
| `security.secret.rotation-interval` | `24h` | 1h–720h | duration | No | Yes | Key rotation interval |

<!-- Resolves: DEF-163 — Added RLS, Column Masking, Audit, Secret Management params -->

### 12.8.1 Multi-Tenancy

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `multi-tenancy.enabled` | `false` | true / false | — | No | No | Enable multi-tenancy |
| `multi-tenancy.default-tenant` | `default` | — | — | No | No | Default tenant ID |
| `multi-tenancy.isolation.memory` | `hard` | hard / soft | — | No | Yes | Memory isolation mode |
| `multi-tenancy.isolation.cpu` | `soft` | hard / soft | — | No | Yes | CPU isolation mode |
| `multi-tenancy.quota.enforcement` | `strict` | strict / permissive | — | No | Yes | Quota enforcement mode |
| `multi-tenancy.catalog-access.default-policy` | `deny` | allow / deny | — | No | Yes | Default catalog access policy |

<!-- Resolves: DEF-174 — Added multi-tenancy config params -->

### 12.9 Observability

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `observability.metrics.enabled` | `true` | true / false | — | No | Yes | Enable Prometheus metrics |
| `observability.metrics.port` | `9090` | 1024–65535 | — | No | No | Metrics endpoint port |
| `observability.tracing.enabled` | `true` | true / false | — | No | Yes | Enable distributed tracing |
| `observability.tracing.sampling.type` | `probability` | always / never / probability / rate-limiting | — | No | Yes | Sampling strategy |
| `observability.tracing.sampling.probability` | `0.01` | 0.0–1.0 | ratio | No | Yes | Sampling probability (production default) |
| `observability.tracing.sampling.rate-limit` | `100` | 1–100000 | traces/s | No | Yes | Rate limit sampling |
| `observability.logging.level` | `INFO` | TRACE / DEBUG / INFO / WARN / ERROR | — | No | Yes | Root log level |
| `observability.logging.audit.enabled` | `true` | true / false | — | No | Yes | Enable audit logging |
| `observability.logging.audit.max-size` | `100MB` | 10MB–10GB | bytes | No | No | Audit log max file size |
| `observability.logging.audit.max-files` | `10` | 1–100 | — | No | No | Audit log max file count |
| `observability.native.metrics.pull-interval` | `5s` | 1s–60s | duration | No | Yes | Native metrics pull interval |
| `observability.native.metrics.batch-size` | `100` | 10–10000 | — | No | Yes | Native metrics batch size |
| `observability.native.metrics.enabled` | `true` | true / false | — | No | Yes | Enable native metrics collection |

<!-- Resolves: DEF-182 — Added native metrics pull config params -->
<!-- Resolves: DEF-184 — Fixed tracing.sampling.probability default to 0.01 -->

### 12.9.1 Operations

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `operations.rolling-upgrade.enabled` | `false` | true / false | — | No | No | Enable rolling upgrade mode |
| `operations.rolling-upgrade.drain-timeout` | `5m` | 1m–30m | duration | No | Yes | Worker drain timeout |
| `operations.rolling-upgrade.protocol-version-check` | `true` | true / false | — | No | No | Check protocol version compatibility |
| `operations.online-ddl.enabled` | `true` | true / false | — | No | Yes | Enable online DDL |
| `operations.online-ddl.lock-timeout` | `30s` | 5s–300s | duration | No | Yes | DDL lock acquisition timeout |
| `operations.disaster-recovery.enabled` | `false` | true / false | — | No | No | Enable DR replication |
| `operations.disaster-recovery.replication-mode` | `sync-with-degradation` | sync / async / sync-with-degradation | — | No | No | Replication mode |
| `operations.disaster-recovery.sync-timeout` | `5s` | 1s–30s | duration | No | Yes | Sync replication timeout |
| `operations.config.hot-reload-interval` | `30s` | 5s–300s | duration | No | No | Config hot-reload check interval |

<!-- Resolves: DEF-193 — Added operations config params -->

### 12.10 Auto-Scaling

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `auto-scaling.enabled` | `true` | true / false | — | No | Yes | Enable auto-scaling |
| `auto-scaling.min-workers` | `3` | 1–1000 | — | No | Yes | Min worker count |
| `auto-scaling.max-workers` | `100` | 1–10000 | — | No | Yes | Max worker count |
| `auto-scaling.scale-out.cpu-threshold` | `0.8` | 0.1–1.0 | ratio | No | Yes | CPU scale-out threshold |
| `auto-scaling.scale-out.memory-threshold` | `0.85` | 0.1–1.0 | ratio | No | Yes | Memory scale-out threshold |
| `auto-scaling.scale-in.cpu-threshold` | `0.3` | 0.0–0.9 | ratio | No | Yes | CPU scale-in threshold |
| `auto-scaling.scale-in.memory-threshold` | `0.4` | 0.0–0.9 | ratio | No | Yes | Memory scale-in threshold |
| `auto-scaling.cooldown.scale-out` | `3m` | 1m–30m | duration | No | Yes | Scale-out cooldown |
| `auto-scaling.cooldown.scale-in` | `10m` | 1m–60m | duration | No | Yes | Scale-in cooldown |

### 12.11 Maintenance

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `maintenance.compaction.enabled` | `true` | true / false | — | No | Yes | Enable auto compaction |
| `maintenance.compaction.interval` | `1h` | 10m–24h | duration | No | Yes | Compaction check interval |
| `maintenance.compaction.target-file-size` | `128MB` | 8MB–1GB | bytes | No | Yes | Target file size after compaction |
| `maintenance.compaction.min-files-to-compact` | `5` | 2–100 | — | No | Yes | Min files to trigger compaction |
| `maintenance.orphan-cleanup.enabled` | `true` | true / false | — | No | Yes | Enable orphan file cleanup |
| `maintenance.orphan-cleanup.interval` | `6h` | 1h–168h | duration | No | Yes | Cleanup check interval (aligned with [10 §7](./09-streaming.md) `streaming.maintenance.orphan-cleanup-interval`) | <!-- Resolves: DEF-141 — was 24h, aligned to 10 §2.10 default 360m=6h -->
| `maintenance.orphan-cleanup.retention-period` | `3d` | 1d–90d | duration | No | Yes | Orphan file retention period (aligned with [10 §7](./09-streaming.md)) | <!-- Resolves: DEF-141 — was 7d, aligned to 10 §2.10 default 3d -->
| `maintenance.snapshot-expire.enabled` | `true` | true / false | — | No | Yes | Enable snapshot expiration |
| `maintenance.snapshot-expire.interval` | `12h` | 1h–48h | duration | No | Yes | Expiration check interval (aligned with [10 §7](./09-streaming.md) `streaming.maintenance.snapshot-expiration-interval`) | <!-- Resolves: DEF-141 — was 6h, aligned to 10 §2.10 default 720m=12h -->
| `maintenance.snapshot-expire.retention-period` | `7d` | 1d–365d | duration | No | Yes | Snapshot retention period |
| `maintenance.snapshot-expire.retain-count` | `100` | 1–10000 | — | No | Yes | Min snapshots to retain |

<!-- Resolves: DEF-117 — Resilience config params added to configuration reference -->
### 12.12 Resilience

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `resilience.circuit-breaker.worker.failure-rate-threshold` | `0.5` | 0.0–1.0 | ratio | No | Yes | Worker circuit breaker failure rate threshold |
| `resilience.circuit-breaker.worker.minimum-calls` | `10` | 1–1000 | — | No | Yes | Min calls before evaluating failure rate |
| `resilience.circuit-breaker.worker.wait-duration-open` | `30s` | 5s–300s | duration | No | Yes | Wait duration in OPEN state before HALF_OPEN |
| `resilience.circuit-breaker.worker.permitted-calls-half-open` | `3` | 1–20 | — | No | Yes | Probe calls allowed in HALF_OPEN state |
| `resilience.circuit-breaker.connector.failure-rate-threshold` | `0.3` | 0.0–1.0 | ratio | No | Yes | Connector circuit breaker failure rate threshold |
| `resilience.circuit-breaker.connector.wait-duration-open` | `60s` | 5s–600s | duration | No | Yes | Connector OPEN state wait duration |
| `resilience.degradation.enabled` | `true` | true / false | — | No | Yes | Enable degradation manager |
| `resilience.degradation.thresholds.light.worker-ratio` | `0.7` | 0.0–1.0 | ratio | No | Yes | Worker ratio threshold for LIGHT degradation |
| `resilience.degradation.thresholds.light.memory-pressure` | `0.7` | 0.0–1.0 | ratio | No | Yes | Memory pressure threshold for LIGHT degradation |
| `resilience.degradation.thresholds.moderate.memory-pressure` | `0.9` | 0.0–1.0 | ratio | No | Yes | Memory pressure threshold for MODERATE degradation |
| `resilience.degradation.thresholds.severe.worker-ratio` | `0.3` | 0.0–1.0 | ratio | No | Yes | Worker ratio threshold for SEVERE degradation |
| `resilience.degradation.thresholds.critical.worker-ratio` | `0.1` | 0.0–1.0 | ratio | No | Yes | Worker ratio threshold for CRITICAL degradation |
| `resilience.degradation.thresholds.critical.memory-pressure` | `0.95` | 0.0–1.0 | ratio | No | Yes | Memory pressure threshold for CRITICAL degradation |
| `resilience.backpressure.max-pending-requests` | `1000` | 100–100000 | — | No | Yes | Max pending requests before rejection |
| `resilience.backpressure.high-water-mark` | `800` | 50–99999 | — | No | Yes | High water mark for backpressure slowdown |
| `resilience.backpressure.low-water-mark` | `500` | 10–99998 | — | No | Yes | Low water mark for backpressure recovery |
| `resilience.retry.max-retries` | `3` | 0–10 | — | No | Yes | Max retry attempts |
| `resilience.retry.initial-backoff` | `100ms` | 10ms–10s | duration | No | Yes | Initial retry backoff |
| `resilience.retry.max-backoff` | `10s` | 1s–300s | duration | No | Yes | Max retry backoff |
| `resilience.retry.backoff-multiplier` | `2.0` | 1.0–10.0 | — | No | Yes | Exponential backoff multiplier |

<!-- Resolves: DEF-128 — Cache config params added to configuration reference -->
### 12.13 Cache

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `cache.ram.enabled` | `true` | true / false | — | No | No | Enable RAM cache (L1) |
| `cache.ram.max-size` | `8GB` | 1GB–128GB | bytes | No | No | RAM cache max size |
| `cache.ram.eviction-policy` | `LRU` | LRU / LFU / FIFO / TTL / Adaptive | — | No | No | RAM cache eviction policy |
| `cache.ram.eviction-threshold` | `0.9` | 0.5–0.99 | ratio | No | Yes | Eviction trigger threshold (currentSize/maxSize) |
| `cache.ssd.enabled` | `true` | true / false | — | No | No | Enable SSD cache (L2) |
| `cache.ssd.directory` | `/data/cache` | — | path | Yes (if enabled) | No | SSD cache directory |
| `cache.ssd.max-size` | `100GB` | 10GB–10TB | bytes | No | No | SSD cache max size |
| `cache.ssd.num-shards` | `16` | 1–256 | — | No | No | Number of SSD cache shards |
| `cache.ssd.use-direct-io` | `true` | true / false | — | No | No | Use Direct I/O (bypass OS page cache) |
| `cache.ssd.checksum-enabled` | `true` | true / false | — | No | No | Enable data integrity checksum |
| `cache.prefetch.enabled` | `true` | true / false | — | No | Yes | Enable adaptive prefetch |
| `cache.prefetch.threads` | `4` | 1–32 | — | No | No | Prefetch worker thread count |
| `cache.prefetch.window-size` | `8` | 1–32 | row groups | No | Yes | Prefetch window size |
| `cache.prefetch.max-bytes` | `256MB` | 16MB–2GB | bytes | No | Yes | Max prefetch bytes per query |
| `cache.prefetch.coalesce-threshold` | `64KB` | 4KB–1MB | bytes | No | Yes | Gap threshold for request coalescing |
| `cache.prefetch.adaptive` | `true` | true / false | — | No | Yes | Enable adaptive prefetch adjustment |
| `cache.metrics.enabled` | `true` | true / false | — | No | Yes | Enable cache metrics reporting |
| `cache.metrics.report-interval` | `10s` | 1s–300s | duration | No | Yes | Metrics report interval |

<!-- Resolves: DEF-140 — Streaming config params added to configuration reference -->
### 12.14 Streaming

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `streaming.cdc.enabled` | `true` | true / false | — | No | No | Enable CDC ingestion |
| `streaming.cdc.deployment-mode` | `embedded` | embedded / standalone | — | No | No | CDC consumer deployment mode |
| `streaming.cdc.consumer.bootstrap-servers` | — | — | — | Yes (if CDC enabled) | No | Kafka bootstrap servers |
| `streaming.cdc.consumer.group-id` | `pangrova-cdc-consumer` | — | — | No | No | Kafka consumer group ID |
| `streaming.cdc.consumer.auto-offset-reset` | `earliest` | earliest / latest | — | No | No | Kafka offset reset policy |
| `streaming.writer.batch-size` | `1000` | 100–100000 | — | No | Yes | CDC write batch size |
| `streaming.writer.delete-strategy` | `hybrid` | hybrid / position / equality | — | No | No | Delete strategy for upserts |
| `streaming.maintenance.compaction-interval` | `60m` | 10m–24h | duration | No | Yes | Compaction check interval |
| `streaming.maintenance.orphan-cleanup-interval` | `360m` | 1h–168h | duration | No | Yes | Orphan cleanup interval |
| `streaming.maintenance.snapshot-expiration-interval` | `720m` | 1h–48h | duration | No | Yes | Snapshot expiration interval |
| `streaming.maintenance.max-equality-delete-files` | `10` | 1–1000 | — | No | Yes | Threshold trigger for immediate compaction |
| `streaming.maintenance.min-files-to-compact` | `5` | 2–100 | — | No | Yes | Min files for scan-triggered compaction |
| `streaming.maintenance.max-concurrent-tasks` | `4` | 1–32 | — | No | Yes | Global max concurrent maintenance tasks |
| `streaming.maintenance.task-queue-capacity` | `16` | 4–256 | — | No | Yes | Maintenance task queue capacity |
| `streaming.maintenance.task-timeout` | `30m` | 5m–4h | duration | No | Yes | Per-task timeout |
| `streaming.maintenance.threads` | `4` | 1–32 | — | No | No | Task execution thread count |
| `streaming.maintenance.orphan-file-age` | `3d` | 1d–90d | duration | No | Yes | Orphan file retention period |
| `streaming.maintenance.snapshot-retention` | `7d` | 1d–365d | duration | No | Yes | Snapshot retention period |
| `streaming.ingestion.buffer.max-bytes` | `128MB` | 1MB–1GB | bytes | No | Yes | Streaming buffer max size |
| `streaming.ingestion.buffer.max-rows` | `1000000` | 1000–100000000 | — | No | Yes | Streaming buffer max rows |
| `streaming.ingestion.buffer.max-time` | `1m` | 1s–30m | duration | No | Yes | Streaming buffer max flush interval |
| `streaming.ingestion.commit.interval` | `1m` | 1s–30m | duration | No | Yes | Commit interval |
| `streaming.ingestion.commit.retry-times` | `3` | 0–10 | — | No | Yes | Commit retry attempts |
| `streaming.incremental.default-lookback` | `24h` | 1h–720h | duration | No | Yes | Default incremental query lookback |
| `streaming.incremental.max-snapshots-per-query` | `100` | 1–10000 | — | No | Yes | Max snapshots per incremental query |
| `streaming.watermark.default-delay` | `5m` | 0s–1h | duration | No | Yes | Default watermark delay tolerance |
| `streaming.watermark.update-interval` | `10s` | 1s–300s | duration | No | Yes | Watermark update interval |
| `streaming.upsert.default-mode` | `merge-on-read` | copy-on-write / merge-on-read | — | No | No | Default upsert mode |
| `streaming.upsert.equality-delete-enabled` | `true` | true / false | — | No | Yes | Enable equality delete for upserts |

<!-- Resolves: DEF-153 — Governance config params added to configuration reference -->
### 12.15 Governance

| Parameter | Default | Range | Unit | Required | Hot Reload | Description |
|-----------|---------|-------|------|----------|------------|-------------|
| `governance.schema-evolution.compatibility-check` | `true` | true / false | — | No | Yes | Enable schema compatibility check |
| `governance.schema-evolution.allow-incompatible-changes` | `false` | true / false | — | No | Yes | Allow incompatible schema changes |
| `governance.lineage.enabled` | `true` | true / false | — | No | No | Enable OpenLineage integration |
| `governance.lineage.backend` | `marquez` | marquez / datahub / custom | — | No | No | Lineage backend type |
| `governance.lineage.marquez.url` | — | — | URL | Yes (if marquez) | No | Marquez server URL |
| `governance.lineage.emit-column-lineage` | `true` | true / false | — | No | Yes | Emit column-level lineage |
| `governance.data-quality.enabled` | `true` | true / false | — | No | Yes | Enable data quality checks |
| `governance.data-quality.default-schedule` | `0 0 * * *` | — | cron | No | Yes | Default quality check schedule |
| `governance.classification.auto-classify` | `true` | true / false | — | No | Yes | Enable auto PII classification |


<!-- Resolves: DEF-AP-014 — Observability integration for configuration reference -->
## 13. Observability Integration

This section defines the metrics emitted by the configuration subsystem. For the full observability design, see [14-observability.md](./14-observability.md).

### 13.1 Metrics

| Metric | Type | Source Component | Description |
|--------|------|-----------------|-------------|
| `pangrova.config.reload_total` | counter | ConfigManager | Configuration reload operations |
| `pangrova.config.reload_errors_total` | counter | ConfigManager | Configuration reload failures |
| `pangrova.config.validation_errors_total` | counter | ConfigValidator | Validation errors on reload |
| `pangrova.config.override_active_count` | gauge | ConfigManager | Active dynamic overrides |

### 13.2 Tracing

Configuration operations emit the following spans:

- `config.reload` — Configuration reload span (includes validation)
- `config.validate` — Configuration validation span
