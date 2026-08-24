---
title: Distributed Deployment
sidebar_position: 2
---

# Pangrova — Distributed Deployment Guide

This guide walks you through deploying an Pangrova cluster on bare-metal servers or VMs with object storage (S3, OSS, MinIO) as the data lake backend. After completing this guide, you will have a production-ready cluster with High Availability.

---

## Architecture Overview

```
                    ┌─────────────────────────────┐
                    │     Load Balancer (LB)       │
                    │   (HAProxy / ALB / SLB)      │
                    └─────────────┬───────────────┘
                                  │ :8080 (HTTPS)
                    ┌─────────────┴───────────────┐
              ┌─────┴─────┐               ┌───────┴───┐
              │Coordinator│               │Coordinator│
              │  (Leader)  │               │ (Follower) │
              └─────┬─────┘               └───────┬───┘
                    │         PostgreSQL           │
                    └──────────┬───────────────────┘
                               │ HA lease + metadata
              ┌────────────────┼────────────────────┐
        ┌─────┴─────┐   ┌─────┴─────┐   ┌──────────┴──┐
        │  Worker-0  │   │  Worker-1  │   │   Worker-2   │
        └─────┬─────┘   └─────┬─────┘   └──────┬───────┘
              │                │                  │
              └────────────────┼──────────────────┘
                               │ S3/OSS/MinIO
                    ┌──────────┴───────────┐
                    │    Object Storage     │
                    │  (Data Lake Files)    │
                    └──────────────────────┘
```

**Minimum production cluster**: 2 Coordinators + 3 Workers + 1 PostgreSQL + Object Storage

---

## Prerequisites

| Component | Requirement |
|-----------|-------------|
| OS | Linux x86_64 (kernel ≥ 5.15, recommended 6.1 LTS) |
| JDK | 17 or later (OpenJDK, Temurin, Corretto all supported) |
| PostgreSQL | ≥ 14 (for HA state store + metadata) |
| Object Storage | S3 / OSS / MinIO with read/write access |
| Network | All nodes can reach each other on ports 8080, 8081, 9090 |
| NTP | Clock drift < 100ms across all nodes |

### Hardware Sizing

| Role | CPU | RAM | Disk | Count |
|------|-----|-----|------|-------|
| Coordinator | 8+ cores | 32–64 GB | 100 GB SSD (OS + logs) | 2–3 |
| Worker | 16–64 cores | 64–256 GB | 500 GB NVMe (spill + cache) | 3+ |
| PostgreSQL | 4+ cores | 16 GB | 200 GB SSD | 1 (HA pair recommended) |

---

## Step 1 — Prepare All Nodes

Run on **every** Coordinator and Worker node:

### 1.1 Create Service User

```bash
groupadd -r pangrova
useradd -r -g pangrova -s /sbin/nologin -d /opt/pangrova pangrova
```

### 1.2 Install JDK 17

```bash
# Amazon Corretto example (all platforms)
curl -LO https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.tar.gz
tar -xzf amazon-corretto-17-x64-linux-jdk.tar.gz -C /usr/lib/jvm/
ln -sf /usr/lib/jvm/amazon-corretto-17.* /usr/lib/jvm/java-17

# Verify
/usr/lib/jvm/java-17/bin/java -version
```

### 1.3 OS Tuning

```bash
cat > /etc/sysctl.d/99-pangrova.conf << 'EOF'
vm.swappiness=1
net.core.rmem_max=134217728
net.core.wmem_max=134217728
net.ipv4.tcp_rmem=4096 87380 134217728
net.ipv4.tcp_wmem=4096 65536 134217728
net.core.somaxconn=65535
net.ipv4.tcp_tw_reuse=1
net.core.netdev_max_backlog=250000
EOF
sysctl --system

cat > /etc/security/limits.d/99-pangrova.conf << 'EOF'
pangrova soft nofile 1048576
pangrova hard nofile 1048576
pangrova soft nproc 65536
pangrova hard nproc 65536
EOF
```

### 1.4 Install Pangrova Distribution

```bash
# Download or build the distribution
tar -xzf pangrova-<version>.tar.gz -C /opt/
ln -sf /opt/pangrova-<version> /opt/pangrova
chown -R pangrova:pangrova /opt/pangrova

# Create directories
mkdir -p /var/log/pangrova /mnt/spill/pangrova /etc/pangrova/tls /etc/pangrova/catalog
chown -R pangrova:pangrova /var/log/pangrova /mnt/spill/pangrova /etc/pangrova
```

---

## Step 2 — Set Up PostgreSQL

Pangrova requires PostgreSQL for HA leader election and metadata storage.

### 2.1 Create Databases

```sql
-- Connect as superuser
CREATE DATABASE pangrova_ha;
CREATE DATABASE pangrova_meta;

CREATE USER pangrova WITH PASSWORD '<strong-password>';
GRANT CONNECT ON DATABASE pangrova_ha TO pangrova;
GRANT CONNECT ON DATABASE pangrova_meta TO pangrova;
GRANT CREATE ON DATABASE pangrova_ha TO pangrova;
GRANT CREATE ON DATABASE pangrova_meta TO pangrova;
```

### 2.2 Verify Connectivity

From each Coordinator node:

```bash
psql "postgresql://pangrova:<password>@<postgres-host>:5432/pangrova_meta" -c "SELECT 1;"
```

---

## Step 3 — Configure Object Storage

Choose your storage backend and configure accordingly.

### Option A: AWS S3

```bash
# /etc/pangrova/pangrova-env.sh (on all nodes)
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="<your-secret-key>"
```

Create the catalog config:

```bash
cp /opt/pangrova/config/catalog/s3-iceberg.yml /etc/pangrova/catalog/lakehouse.yml
```

Edit `/etc/pangrova/catalog/lakehouse.yml`:

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://my-lakehouse-bucket/warehouse"
  s3.region: "us-east-1"
  s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```

**IAM Role alternative** (recommended for AWS deployments):

```bash
# No static credentials needed. Attach an IAM role to your EC2 instances:
# {
#   "Effect": "Allow",
#   "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
#   "Resource": [
#     "arn:aws:s3:::my-lakehouse-bucket",
#     "arn:aws:s3:::my-lakehouse-bucket/*"
#   ]
# }
```

### Option B: Alibaba Cloud OSS

```bash
# /etc/pangrova/pangrova-env.sh (on all nodes)
export ALIBABA_CLOUD_ACCESS_KEY_ID="<your-access-key-id>"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<your-access-key-secret>"
export PANGROVA_OSS_ENDPOINT="https://oss-cn-hangzhou-internal.aliyuncs.com"
export PANGROVA_OSS_REGION="cn-hangzhou"
```

Create the catalog config:

```bash
cp /opt/pangrova/config/catalog/oss-iceberg.yml /etc/pangrova/catalog/lakehouse.yml
```

Edit `/etc/pangrova/catalog/lakehouse.yml`:

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

**OSS Endpoint selection**:
- Same region VPC: `https://oss-{region}-internal.aliyuncs.com` (no bandwidth fee)
- Cross-region or public: `https://oss-{region}.aliyuncs.com`

**RAM policy** (minimum permissions):

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "oss:GetObject", "oss:PutObject", "oss:DeleteObject",
        "oss:ListObjects", "oss:ListObjectsV2", "oss:GetBucketLocation"
      ],
      "Resource": [
        "acs:oss:*:*:my-oss-bucket",
        "acs:oss:*:*:my-oss-bucket/*"
      ]
    }
  ]
}
```

### Option C: MinIO (Self-Hosted)

```bash
# /etc/pangrova/pangrova-env.sh (on all nodes)
export MINIO_ACCESS_KEY="pangrova-user"
export MINIO_SECRET_KEY="<your-secret-key>"
export PANGROVA_MINIO_ENDPOINT="http://minio.internal:9000"
```

Create the catalog config:

```bash
cp /opt/pangrova/config/catalog/minio-iceberg.yml /etc/pangrova/catalog/lakehouse.yml
```

Edit `/etc/pangrova/catalog/lakehouse.yml`:

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://pangrova-data/warehouse"
  s3.endpoint: "http://minio.internal:9000"
  s3.path-style-access: "true"
  s3.region: "us-east-1"
  s3.access-key-id: "${MINIO_ACCESS_KEY}"
  s3.secret-access-key: "${MINIO_SECRET_KEY}"
```

**Pre-create the MinIO bucket**:

```bash
mc alias set myminio http://minio.internal:9000 admin-key admin-secret
mc mb myminio/pangrova-data
mc admin user add myminio pangrova-user <password>
mc admin policy attach myminio readwrite --user pangrova-user
```

### Verify Storage Access

From any node, test connectivity:

```bash
# AWS S3
aws s3 ls s3://my-lakehouse-bucket/warehouse/ --region us-east-1

# OSS (using ossutil)
ossutil ls oss://my-oss-bucket/warehouse/ --endpoint https://oss-cn-hangzhou.aliyuncs.com

# MinIO
mc ls myminio/pangrova-data/warehouse/
```

---

## Step 4 — Configure Coordinators

On **each Coordinator node**, create the configuration files.

### 4.1 Environment File

```bash
cat > /etc/pangrova/pangrova-env.sh << 'EOF'
#!/usr/bin/env bash
export JAVA_HOME="/usr/lib/jvm/java-17"
export PANGROVA_LOG_DIR="/var/log/pangrova"
export PANGROVA_CONF_DIR="/etc/pangrova"

# Database
export PANGROVA_DB_HOST="postgres.internal"
export PANGROVA_DB_PORT="5432"
export PANGROVA_DB_USER="pangrova"
export PANGROVA_DB_PASSWORD="<inject-from-secrets-manager>"

# Security
export PANGROVA_INTERNAL_AUTH_SECRET="<openssl rand -base64 48>"
# export PANGROVA_OAUTH2_ISSUER_URI="https://idp.internal/realms/pangrova"

# Storage (choose one — see Step 3)
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="<your-key>"
export AWS_SECRET_ACCESS_KEY="<your-secret>"
EOF

chmod 640 /etc/pangrova/pangrova-env.sh
chown root:pangrova /etc/pangrova/pangrova-env.sh
```

### 4.2 Coordinator Configuration

```bash
cp /opt/pangrova/config/coordinator.yml /etc/pangrova/coordinator.yml
```

Key settings to customize in `/etc/pangrova/coordinator.yml`:

```yaml
coordinator:
  id: "coordinator-0"           # Unique per node: coordinator-0, coordinator-1
  host: "0.0.0.0"
  port: 8080

  ha:
    enabled: true
    jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST}:${PANGROVA_DB_PORT:-5432}/pangrova_ha"
    username: "${PANGROVA_DB_USER}"
    password: "${PANGROVA_DB_PASSWORD}"
    lease-duration: "30s"
    heartbeat-interval: "10s"

  query:
    timeout: "10m"
    max-concurrent: 200

  worker:
    heartbeat-timeout: "30s"
    min-healthy-workers: 2

state-store:
  type: JDBC
  jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST}:${PANGROVA_DB_PORT:-5432}/pangrova_meta"
  username: "${PANGROVA_DB_USER}"
  password: "${PANGROVA_DB_PASSWORD}"

metadata:
  enabled: true
  jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST}:${PANGROVA_DB_PORT:-5432}/pangrova_meta"
  username: "${PANGROVA_DB_USER}"
  password: "${PANGROVA_DB_PASSWORD}"

security:
  enabled: false    # Set to true and configure OAuth2 for production
  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"

observability:
  enabled: true
  service-name: "pangrova-coordinator"
```

### 4.3 JVM Configuration

```bash
cp /opt/pangrova/config/jvm-coordinator.config /etc/pangrova/jvm-coordinator.config
```

Adjust heap based on node RAM (see CONFIG.md JVM Sizing Guide).

### 4.4 Systemd Service

```bash
cat > /etc/systemd/system/pangrova-coordinator.service << 'EOF'
[Unit]
Description=Pangrova Coordinator
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pangrova
Group=pangrova
Environment="PANGROVA_HOME=/opt/pangrova"
Environment="PANGROVA_CONF_DIR=/etc/pangrova"
Environment="PANGROVA_LOG_DIR=/var/log/pangrova"
ExecStart=/opt/pangrova/bin/pangrova-coordinator.sh --config /etc/pangrova/coordinator.yml
ExecStop=/opt/pangrova/bin/pangrova-stop.sh --coordinator
Restart=on-failure
RestartSec=10
LimitNOFILE=1048576
LimitNPROC=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable pangrova-coordinator
```

---

## Step 5 — Configure Workers

On **each Worker node**, create the configuration files.

### 5.1 Environment File

Same as Coordinator (Step 4.1), but on Worker nodes you can skip OAuth2 settings.

### 5.2 Worker Configuration

```bash
cp /opt/pangrova/config/worker.yml /etc/pangrova/worker.yml
```

Key settings to customize in `/etc/pangrova/worker.yml`:

```yaml
worker:
  host: "0.0.0.0"
  port: 8081
  advertised-host: "<this-node-ip-or-hostname>"     # REQUIRED: must be reachable
  id: "worker-0"                                     # Unique per node

  coordinator-url: "http://coordinator-lb.internal:8080"   # LB or primary coordinator
  heartbeat-interval: "10s"

  transport:
    port: 9090
    ticket-signing-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"

  memory:
    budget-bytes: 17179869184        # 16 GB (adjust per node RAM)
    node-bytes: 109951162777         # 85% of total node RAM

  threads:
    query: 32                        # = physical CPU cores
    io: 16                           # = half of query threads

  shuffle:
    buffer-capacity: 500
    max-memory-bytes: 4294967296     # 4 GB
    spill-threshold: 0.75

  spill:
    directory: "/mnt/spill/pangrova"
    max-size-bytes: 107374182400     # 100 GB

  native:
    cache:
      enabled: true
      memory-bytes: 34359738368      # 32 GB
      ssd-path: "/mnt/nvme0/velox-cache"
      ssd-bytes: 214748364800        # 200 GB

  registration:
    max-attempts: 20
    initial-backoff: "1s"
    max-backoff: "60s"

security:
  enabled: false    # Match coordinator setting
  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"

observability:
  enabled: true
  service-name: "pangrova-worker"
```

### 5.3 JVM Configuration

```bash
cp /opt/pangrova/config/jvm-worker.config /etc/pangrova/jvm-worker.config
```

### 5.4 Systemd Service

```bash
cat > /etc/systemd/system/pangrova-worker.service << 'EOF'
[Unit]
Description=Pangrova Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pangrova
Group=pangrova
Environment="PANGROVA_HOME=/opt/pangrova"
Environment="PANGROVA_CONF_DIR=/etc/pangrova"
Environment="PANGROVA_LOG_DIR=/var/log/pangrova"
ExecStart=/opt/pangrova/bin/pangrova-worker.sh --config /etc/pangrova/worker.yml
ExecStop=/opt/pangrova/bin/pangrova-stop.sh --worker
Restart=on-failure
RestartSec=10
LimitNOFILE=1048576
LimitNPROC=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable pangrova-worker
```

---

## Step 6 — Start the Cluster

**Order matters**: Coordinators first, then Workers.

### 6.1 Start Coordinators

```bash
# On coordinator-0 (first):
systemctl start pangrova-coordinator
journalctl -u pangrova-coordinator -f | grep -m1 "role="
# Wait for: "coordinator=coordinator-0 role=LEADER"

# On coordinator-1 (second):
systemctl start pangrova-coordinator
journalctl -u pangrova-coordinator -f | grep -m1 "role="
# Wait for: "coordinator=coordinator-1 role=FOLLOWER"
```

### 6.2 Verify Leader Election

```bash
/opt/pangrova/bin/pangrova-health-check.sh coordinator-0:8080
# Output: healthy

curl -s http://coordinator-0:8080/v1/cluster/status | python3 -m json.tool
# Verify: exactly one LEADER, others are FOLLOWER
```

### 6.3 Start Workers

```bash
# On all worker nodes (can start in parallel):
systemctl start pangrova-worker
```

### 6.4 Verify Cluster Health

```bash
# Check all workers registered
/opt/pangrova/bin/pangrova-cluster-status.sh --coordinator http://coordinator-0:8080

# Expected output:
#   Coordinator coordinator-0  LEADER   healthy  v1.0.0
#   Coordinator coordinator-1  FOLLOWER healthy  v1.0.0
#   Worker      worker-0       HEALTHY           v1.0.0
#   Worker      worker-1       HEALTHY           v1.0.0
#   Worker      worker-2       HEALTHY           v1.0.0
```

---

## Step 7 — Validate End-to-End

### 7.1 Run a Test Query

```bash
/opt/pangrova/bin/pangrova-cli.sh --host coordinator-0 --port 8080
```

```sql
-- Basic connectivity test
SELECT 1 + 1 AS result;

-- List available catalogs
SHOW CATALOGS;

-- Create a test table on object storage
CREATE SCHEMA lakehouse.test;
CREATE TABLE lakehouse.test.hello (id BIGINT, name VARCHAR);
INSERT INTO lakehouse.test.hello VALUES (1, 'Pangrova');
SELECT * FROM lakehouse.test.hello;

-- Cleanup
DROP TABLE lakehouse.test.hello;
DROP SCHEMA lakehouse.test;
```

### 7.2 Verify Object Storage Writes

```bash
# Check that files were created in the object store
# AWS S3:
aws s3 ls s3://my-lakehouse-bucket/warehouse/test/hello/ --recursive

# OSS:
ossutil ls oss://my-oss-bucket/warehouse/test/hello/ -s

# MinIO:
mc ls myminio/pangrova-data/warehouse/test/hello/
```

---

## Step 8 — Load Balancer Setup

Place a load balancer in front of Coordinator nodes for client access.

### HAProxy Configuration Example

```
frontend pangrova_frontend
    bind *:443 ssl crt /etc/haproxy/certs/pangrova.pem
    default_backend pangrova_coordinators

backend pangrova_coordinators
    option httpchk GET /v1/health
    http-check expect status 200
    balance roundrobin
    server coordinator-0 10.0.1.10:8080 check inter 5s fall 3 rise 2
    server coordinator-1 10.0.1.11:8080 check inter 5s fall 3 rise 2
```

### AWS ALB / Alibaba Cloud SLB

- Target group: Coordinator nodes on port 8080
- Health check path: `/v1/health`
- Health check interval: 10s
- Unhealthy threshold: 3
- Healthy threshold: 2

---

## Step 9 — Operations

### Start/Stop Commands

```bash
# Unified commands
/opt/pangrova/bin/pangrova.sh start coordinator
/opt/pangrova/bin/pangrova.sh start worker
/opt/pangrova/bin/pangrova.sh stop --all
/opt/pangrova/bin/pangrova.sh status

# Individual node health check
/opt/pangrova/bin/pangrova-health-check.sh <host>:<port>

# Cluster status
/opt/pangrova/bin/pangrova-cluster-status.sh --coordinator http://coordinator-0:8080
```

### Rolling Upgrade

```bash
/opt/pangrova/bin/pangrova-rolling-upgrade.sh \
  --new-version-dir /opt/pangrova-1.1.0 \
  --workers worker-0:8081,worker-1:8081,worker-2:8081 \
  --coordinators coordinator-0:8080,coordinator-1:8080 \
  --coordinator-url http://coordinator-0:8080
```

### Add a New Worker

1. Install Pangrova on the new node (Step 1)
2. Copy Worker config from an existing Worker
3. Change `worker.id` and `worker.advertised-host`
4. Start the Worker — it auto-registers with the Coordinator

### Remove a Worker

```bash
# Drain queries from the worker (graceful shutdown waits for in-flight queries)
systemctl stop pangrova-worker
# Worker is automatically removed from the cluster after heartbeat-timeout
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Worker fails to register | Wrong `coordinator-url` | Verify URL and network connectivity |
| `S3Exception: Access Denied` | Bad credentials or missing permissions | Verify IAM policy and credentials |
| `ConnectException: Connection refused` | Coordinator not running or wrong port | Check systemd status and port |
| `No Leader elected` | DB unreachable from both coordinators | Verify PostgreSQL connectivity |
| `Worker marked DEAD` | Network partition or GC pause > heartbeat-timeout | Check GC logs, increase timeout if needed |
| `OutOfMemoryError: Direct buffer` | Arrow off-heap exhausted | Increase `-XX:MaxDirectMemorySize` in jvm-worker.config |
| `OSS: SignatureDoesNotMatch` | Wrong region or endpoint mismatch | Verify endpoint matches bucket region |
| `MinIO: NoSuchBucket` | Bucket not pre-created | Run `mc mb` to create the bucket |
| Slow queries on S3/OSS | Too few I/O threads | Increase `worker.threads.io` |

---

## Configuration File Reference

| File | Location | Purpose |
|------|----------|---------|
| `pangrova-env.sh` | `/etc/pangrova/` | Environment variables (secrets, paths, storage credentials) |
| `coordinator.yml` | `/etc/pangrova/` | Coordinator behavior, HA, query, metadata |
| `worker.yml` | `/etc/pangrova/` | Worker behavior, memory, shuffle, native engine |
| `jvm-coordinator.config` | `/etc/pangrova/` | Coordinator JVM heap, GC, flags |
| `jvm-worker.config` | `/etc/pangrova/` | Worker JVM heap, GC, flags |
| `logback.xml` | `/etc/pangrova/` | Logging configuration |
| `catalog/<name>.yml` | `/etc/pangrova/catalog/` | Per-catalog storage configuration |

See `CONFIG.md` for the complete configuration parameter reference.

---

## Security Hardening (Production)

For production deployments, also complete:

1. **Enable TLS** — Set `security.tls.enabled: true` on all nodes
2. **Enable OAuth2** — Configure `security.auth.type: OAUTH2` on Coordinators
3. **Enable internal auth** — Set `PANGROVA_INTERNAL_AUTH_SECRET` on all nodes
4. **Firewall rules** — Block all ports except 8080 (clients), 8081 (internal), 9090 (shuffle)
5. **Secrets manager** — Never store credentials in plaintext config files

See `PRODUCTION-CHECKLIST.md` for the full pre-go-live validation checklist.
