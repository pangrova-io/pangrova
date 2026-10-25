---
title: "Production Checklist"
sidebar_position: 3
---

# Pangrova Production Deployment Checklist

Work through each section in order. Check off each item before proceeding to the next section. A failed item blocks go-live until resolved.

---

## Phase 1 — Infrastructure Preparation

### 1.1 OS and Kernel

- [ ] Linux kernel ≥ 5.15 on all nodes (recommended: 6.1 LTS)
- [ ] Swap disabled or set to absolute minimum (`vm.swappiness=1`)
  ```bash
  sysctl -w vm.swappiness=1
  echo "vm.swappiness=1" >> /etc/sysctl.d/99-pangrova.conf
  ```
- [ ] Open file descriptor limit set to ≥ 1,048,576
  ```bash
  ulimit -n 1048576
  # Persist in /etc/security/limits.d/99-pangrova.conf:
  # pangrova soft nofile 1048576
  # pangrova hard nofile 1048576
  ```
- [ ] Max user processes set to ≥ 65,536
  ```bash
  ulimit -u 65536
  ```
- [ ] Network buffers tuned for high-throughput shuffle:
  ```bash
  sysctl -w net.core.rmem_max=134217728
  sysctl -w net.core.wmem_max=134217728
  sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
  sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
  sysctl -w net.core.somaxconn=65535
  sysctl -w net.ipv4.tcp_tw_reuse=1
  ```
- [ ] Coordinator nodes: TCP keepalive tuned to detect dead workers fast:
  ```bash
  sysctl -w net.ipv4.tcp_keepalive_time=60
  sysctl -w net.ipv4.tcp_keepalive_intvl=10
  sysctl -w net.ipv4.tcp_keepalive_probes=6
  ```
- [ ] NTP / Chrony configured; clock drift < 100 ms across all nodes
  ```bash
  chronyc tracking | grep "System time"
  ```
- [ ] `pangrova` OS user and group created with no login shell
  ```bash
  groupadd -r pangrova && useradd -r -g pangrova -s /sbin/nologin pangrova
  ```

### 1.2 Storage Mounts

- [ ] Spill directory mounted on a dedicated fast disk (NVMe preferred), separate from root and data partitions
  - Mount point: `/mnt/spill` with ≥ 200 GB free
  - `noatime` mount option enabled
- [ ] Velox SSD cache directory mounted on NVMe
  - Mount point: `/mnt/nvme0/velox-cache` with ≥ 250 GB free
  - Filesystem: `ext4` or `xfs`; `noatime`, `nodiratime`
- [ ] Log directory created and owned by `pangrova`
  ```bash
  mkdir -p /var/log/pangrova
  chown pangrova:pangrova /var/log/pangrova
  ```
- [ ] Verify no shared mounts between Coordinator and Worker data paths

### 1.3 JDK

- [ ] JDK 17 installed on all nodes (exact version, not 11 or 21)
  ```bash
  java -version   # must show 17.x.x
  ```
- [ ] `JAVA_HOME` set correctly in `/etc/pangrova/pangrova-env.sh`
- [ ] JDK verified with a simple sanity test:
  ```bash
  $JAVA_HOME/bin/java -version && echo OK
  ```

---

## Phase 2 — Database (State Store and Metadata)

- [ ] PostgreSQL ≥ 14 running and accessible from all Coordinator nodes
- [ ] Dedicated databases created:
  ```sql
  CREATE DATABASE pangrova_ha;
  CREATE DATABASE pangrova_meta;
  ```
- [ ] `pangrova` database user created with limited privileges:
  ```sql
  CREATE USER pangrova WITH PASSWORD '...';
  GRANT CONNECT ON DATABASE pangrova_ha TO pangrova;
  GRANT CONNECT ON DATABASE pangrova_meta TO pangrova;
  -- Grant DDL rights for schema creation on first startup:
  GRANT CREATE ON DATABASE pangrova_ha TO pangrova;
  GRANT CREATE ON DATABASE pangrova_meta TO pangrova;
  ```
- [ ] Database connection string verified from each Coordinator:
  ```bash
  psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_meta -c "SELECT 1;"
  ```
- [ ] PostgreSQL `max_connections` set to accommodate the JDBC connections opened by each Coordinator (state store + metadata store) with OS headroom
- [ ] Point-in-time recovery (PITR) enabled on the metadata database
- [ ] Database backup and restore procedure tested (restore from backup to verify integrity)

---

## Phase 3 — TLS at the Load Balancer

Pangrova nodes do not terminate TLS themselves — TLS is terminated at the load
balancer / ingress in front of the Coordinators. Coordinator and Worker HTTP
endpoints serve plain HTTP on `coordinator.port` / `worker.port` internally.

- [ ] Server certificate installed on the load balancer / ingress
  - Subject Alternative Names (SANs) include the public FQDN
  - Validity period: ≥ 90 days (use automation for renewal, e.g., Let's Encrypt + cert-manager)
- [ ] Certificate chain complete (server cert + intermediate CA + root CA)
- [ ] Private key permissions restricted to the LB process (mode 600)
- [ ] Certificate validity verified:
  ```bash
  openssl s_client -connect pangrova.example.com:443 -servername pangrova.example.com \
      </dev/null | openssl x509 -noout -dates -subject -issuer
  ```
- [ ] LB forwards to the Coordinator HTTP port (`coordinator.port`, default 8080)
- [ ] Certificate rotation procedure documented and tested (zero-downtime reload at the LB)

---

## Phase 4 — Secrets and Credentials

- [ ] All secrets stored in a secrets manager (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager, or Azure Key Vault) — NOT in plaintext files
- [ ] The following secrets generated and stored:
  - [ ] `PANGROVA_INTERNAL_AUTH_SECRET` — inter-node authentication secret shared by Coordinators and Workers (set `worker.internal-auth-secret` / env `PANGROVA_INTERNAL_AUTH_SECRET`)
    ```bash
    openssl rand -base64 48
    ```
  - [ ] IdP client credentials (OAuth2 client ID/secret or LDAP bind DN) if `security.auth.type` is enabled
  - [ ] Object store credentials injected as environment variables (`AWS_*`, `ALIBABA_CLOUD_*`, `MINIO_*`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_*` — per your storage)
- [ ] Secrets injected as environment variables at runtime (not written to `/etc/pangrova/pangrova-env.sh` in cleartext)
- [ ] `pangrova-env.sh` file permissions set to restrict reads:
  ```bash
  chmod 640 /etc/pangrova/pangrova-env.sh
  chown root:pangrova /etc/pangrova/pangrova-env.sh
  ```
- [ ] Secret rotation procedure documented for each secret type above

---

## Phase 5 — Network and Firewall

- [ ] Coordinator nodes reachable from clients on port 8080 (HTTP) or 443 (HTTPS behind load balancer)
- [ ] Coordinator nodes reachable from Worker nodes on port 8080
- [ ] Worker nodes reachable from Coordinator on port 8081 (management) and 9090 (transport/shuffle)
- [ ] If private COPY task transport is configured, set a fixed `worker.copy.task-tls-port`, expose that port only to Coordinator nodes, and verify the Worker certificate SAN covers `worker.advertised-host`
- [ ] Worker-to-Worker ports open: 9090 (Arrow Flight shuffle)
- [ ] All inter-node communication BLOCKED from public internet (firewall rules or security groups)
- [ ] Load balancer health check configured:
  - Path: `GET /v1/health`
  - Expected response: `HTTP 200`
  - Interval: 10s, unhealthy threshold: 3, healthy threshold: 2
- [ ] DNS resolution verified from each node:
  ```bash
  nslookup coordinator.internal
  nslookup worker-0.internal
  ```
- [ ] `PANGROVA_ADVERTISED_HOST` on each Worker resolves to the correct IP from other nodes

---

## Phase 6 — Identity Provider (OAuth2 / LDAP)

- [ ] Pangrova registered as an application/client in the IdP
- [ ] Client ID and Client Secret obtained and stored in secrets manager
- [ ] `security.enabled: true` and `security.auth.type: OAUTH2` (or `LDAP`) set in `coordinator.yml` — startup fails with `CONFIG_MISSING_KEY` if `security.auth.type` is missing
- [ ] Test token obtained and validated:
  ```bash
  # Obtain a test token (example with client_credentials):
  curl -X POST "${ISSUER_URI}/protocol/openid-connect/token" \
    -d "grant_type=client_credentials&client_id=...&client_secret=..."
  # Decode and verify claims:
  echo "<token>" | cut -d. -f2 | base64 -d | python3 -m json.tool
  ```
- [ ] Admin role group (`pangrova-admin`) provisioned in the IdP with at least one member
- [ ] Authenticated request succeeds against the REST API:
  ```bash
  curl -H "Authorization: Bearer <token>" https://pangrova.example.com/v1/namespaces
  ```
- [ ] Unauthenticated request correctly returns `HTTP 401`

---

## Phase 7 — Object Store (S3 / OSS / MinIO / GCS / ADLS)

- [ ] IAM role or service account created for Pangrova with read/write access to the warehouse bucket
- [ ] Bucket/container exists and is accessible:
  ```bash
  # AWS S3:
  aws s3 ls s3://my-lakehouse/warehouse/ || echo "FAILED"

  # Alibaba Cloud OSS:
  ossutil ls oss://my-oss-bucket/warehouse/ \
    --endpoint https://oss-cn-hangzhou.aliyuncs.com || echo "FAILED"

  # MinIO:
  mc ls myminio/pangrova-data/warehouse/ || echo "FAILED"
  ```
- [ ] Bucket versioning and lifecycle policies reviewed (Iceberg time-travel requires version retention)
- [ ] Server-side encryption enabled (SSE-S3/SSE-KMS for AWS; SSE-OSS/SSE-KMS for OSS)
- [ ] Pangrova nodes can reach object store endpoints (no proxy required, or proxy configured in `pangrova-env.sh`)
- [ ] For OSS: verify internal endpoint is used within VPC (`oss-{region}-internal.aliyuncs.com`)
- [ ] For MinIO: verify `path-style-access: true` is set in catalog config
- [ ] Storage credentials are injected via environment variables (not hardcoded in config files)
- [ ] Catalog YAML file placed at `/etc/pangrova/catalog/<name>.yml` on all Coordinator nodes
- [ ] Iceberg external writers conform to the
  [deletion-file compatibility limits](../getting-started/catalogs/storage-backends.md#iceberg-deletion-file-compatibility):
  DV Puffin footers are uncompressed, DV blobs have no compression codec, and
  equality-delete inputs are rejected while their read capability is false.
  Record the actual writer/artifact acceptance results before sign-off; a
  documented format limit is not a passing external-writer qualification.
- [ ] Test write and read from Pangrova CLI:
  ```bash
  # Quick connectivity test (interactive CLI):
  /opt/pangrova/bin/pangrova-cli.sh --host coordinator --port 8080
  # then run: CREATE SCHEMA lakehouse.test; DROP SCHEMA lakehouse.test;
  ```

---

## Phase 8 — First Deployment

- [ ] Configuration files installed on all nodes (under `PANGROVA_CONF_DIR`, e.g. `/etc/pangrova`):
  - `/etc/pangrova/coordinator.yml` (Coordinator nodes)
  - `/etc/pangrova/worker.yml` (Worker nodes — security keys also live here, e.g. `worker.internal-auth-secret`)
  - `/etc/pangrova/pangrova-env.sh` (all nodes)
  - `/etc/pangrova/logback.xml` (all nodes, optional — used automatically when present)
  - `/etc/pangrova/jvm-coordinator.config` or `jvm-worker.config` (role-appropriate, optional)
  - `/etc/pangrova/catalog/` (catalog configuration files)
- [ ] Startup smoke check passes on each Coordinator (no `--validate` mode exists — start, then probe health):
  ```bash
  systemctl start pangrova-coordinator
  curl -sf http://localhost:8080/v1/health && echo OK
  ```
- [ ] Startup smoke check passes on each Worker:
  ```bash
  systemctl start pangrova-worker
  curl -sf http://localhost:8081/v1/health || journalctl -u pangrova-worker --no-pager | tail -20
  ```
- [ ] **Start Coordinators first** (in order; wait for each to reach LEADER/STANDBY state before starting the next):
  ```bash
  systemctl start pangrova-coordinator
  # Wait for log line: "event=ha_started ... role=LEADER" or "role=STANDBY"
  journalctl -u pangrova-coordinator -f | grep -m1 "role="
  ```
- [ ] **Verify exactly one leader elected** (not zero, not two):
  ```bash
  journalctl -u pangrova-coordinator | grep -m1 "role="   # expect role=LEADER on one node, role=STANDBY on others
  ```
- [ ] **Start Workers** (all can start in parallel):
  ```bash
  systemctl start pangrova-worker
  ```
- [ ] All Workers registered and HEALTHY:
  ```bash
  curl -s http://coordinator:8080/api/workers | python3 -m json.tool
  # All workers should show healthState: "HEALTHY"
  ```

---

## Phase 9 — Observability Validation

- [ ] OTel Collector running and reachable from all Workers (Workers export via `observability.otlp-endpoint`, e.g. `http://otel-collector:4317`)
- [ ] Traces appearing in your tracing backend (Grafana Tempo / Jaeger):
  ```bash
  curl -s http://coordinator:8080/v1/health   # generates request activity
  # Verify spans appear in backend within 30s
  ```
- [ ] Metrics appearing in your metrics backend via the OTel Collector (Pangrova exports through OTLP; there is no built-in `/metrics` scrape endpoint):
  ```bash
  # Verify on the collector/Prometheus side instead of the node, e.g.:
  curl -s http://prometheus:9090/api/v1/label/__name__/values | grep -i pangrova
  ```
- [ ] GC logs being written:
  ```bash
  ls -la /var/log/pangrova/gc-coordinator.log
  ls -la /var/log/pangrova/gc-worker.log
  ```
- [ ] Application logs being written to rolling files:
  ```bash
  ls -la /var/log/pangrova/pangrova-coordinator.log
  ls -la /var/log/pangrova/pangrova-worker.log
  ```
- [ ] Error log file is empty on a clean startup (no errors on start):
  ```bash
  wc -l /var/log/pangrova/pangrova-coordinator-error.log   # should be 0
  ```
- [ ] Alert rules configured for: Coordinator leader loss, Worker DEAD, heap OOM, query timeout rate spike

---

## Phase 10 — Security Validation

- [ ] Unauthenticated SQL query rejected (`HTTP 401`):
  ```bash
  curl -i -X POST https://pangrova.example.com/v1/query/submit \
       -H 'Content-Type: application/json' \
       -d '{"query":"SELECT 1"}' | grep -i "401\|unauthorized"
  ```
- [ ] SQL query with valid token succeeds:
  ```bash
  curl -X POST https://pangrova.example.com/v1/query/submit \
       -H "Authorization: Bearer ${TEST_TOKEN}" \
       -H 'Content-Type: application/json' \
       -d '{"query":"SELECT 1"}' | python3 -m json.tool
  ```
- [ ] Authorization scope reviewed: `security.auth.type` enforces authentication (401 for missing/invalid tokens); fine-grained RBAC/RLS policy must be enforced in the IdP or at the data layer — confirm the intended policy exists there
- [ ] TLS verified — no plaintext HTTP accepted from clients (LB terminates TLS):
  ```bash
  curl -v https://pangrova.example.com/v1/health 2>&1 | grep -i "HTTP/2\|HTTP/1.1 200"
  ```
- [ ] Port scan confirms only expected ports open:
  ```bash
  nmap -p 1-65535 coordinator-node-ip | grep "open"
  # Expected: 8080 (Coordinator HTTP; 443 at the LB). JMX (9900 Coordinator / 9901 Worker,
  # local-only) must NOT appear from remote.
  ```

---

## Phase 11 — Performance Baseline

- [ ] Run the included TPC-H benchmark queries and record runtimes (the benchmark module ships `queries/tpch/q01.sql`; run each query through the interactive CLI):
  ```bash
  /opt/pangrova/bin/pangrova-cli.sh --host coordinator --port 8080 \
      --catalog <catalog> --schema <schema>
  # paste each query from pangrova-testing/pangrova-test-benchmark/queries/tpch/ and record runtimes
  ```
- [ ] Coordinator heap usage under sustained load stays below 80% of `-Xmx`
- [ ] Worker heap usage stays below 80% of `-Xmx`; GC pause P99 < 500 ms
- [ ] No `DirectMemory allocation failed` warnings in Worker logs
- [ ] If the native data cache is enabled (`worker.native.cache.enabled`, SSD path configured), repeated scans of the same data show improved latency; monitor cache effectiveness in the tracing/metrics backend
- [ ] Shuffle spill rate is zero under normal load (spill only under memory pressure)
- [ ] Baseline results saved and committed to the ops runbook for future regression comparison

---

## Phase 12 — Runbook and On-Call Handover

- [ ] On-call engineers have read access to `/var/log/pangrova/` on all nodes
- [ ] `systemctl status pangrova-{coordinator,worker}` works for non-root on-call users
- [ ] Ops runbook exists and covers:
  - [ ] Rolling restart procedure (Coordinator + Worker)
  - [ ] Forced leader failover procedure
  - [ ] Worker drain and removal procedure
  - [ ] Emergency config change procedure (hot-reload vs. restart)
  - [ ] Database backup and restore procedure
  - [ ] TLS certificate renewal procedure
  - [ ] What to do when coordinator log shows `OOM`, `LEADER_LOST`, `WORKER_DEAD`
- [ ] Escalation path documented (who to page, with contact info)
- [ ] Monitoring dashboards linked in the runbook (Grafana URLs, alert manager links)

---

## Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Infrastructure Lead | | | |
| Security Reviewer | | | |
| DBA / Data Platform | | | |
| On-Call Lead | | | |

**Deployment approved for production:** ☐ YES  ☐ NO — blocked on items: ___________
