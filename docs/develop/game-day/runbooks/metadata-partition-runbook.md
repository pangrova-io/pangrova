# Runbook: Metadata Store Partition

**Use this runbook when:** The Coordinator cannot reach the PostgreSQL metadata store (pangrova_meta / pangrova_ha), causing DDL failures, metadata cache fallback, or circuit breaker trips.

| Field | Value |
|-------|-------|
| Scenario | [GD-003: Metadata Store Partition](../scenarios/gd-003-metadata-partition.md) |
| Severity | Critical |
| Primary risk | DDL blocked; uncached queries fail; risk of stale metadata serving |
| Expected resolution | Restore network connectivity or restart PostgreSQL; < 120s |

---

## Decision Tree

```
Metadata store unreachable?
  │
  ├─► Can coordinator reach PostgreSQL port at all?
  │     │
  │     ├─► NO (timeout) ──► Network partition or firewall → Step 3A
  │     │
  │     └─► YES (port open) but queries fail ──► Database-level issue → Step 3B
  │
  ├─► Is the metadata store process running?
  │     │
  │     ├─► YES ──► Network/firewall issue between coordinator and DB → Step 3A
  │     │
  │     └─► NO  ──► Database process crashed → Step 3B
  │
  └─► Is the partition limited to one coordinator or all?
        │
        ├─► ONE coordinator ──► Likely local network/firewall issue on that coordinator
        │
        └─► ALL coordinators ──► Database-side issue or upstream network event
```

---

## Step 1: Confirm the Partition

```bash
# Test TCP connectivity from each coordinator to the metadata store
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} → metadata store ==="
  ssh "${coord}" "
    nc -zv ${PANGROVA_DB_HOST:-metadata-store} ${PANGROVA_DB_PORT:-5432} -w 5 && \
      echo 'TCP: OPEN' || echo 'TCP: CLOSED/TIMEOUT'
  "
done

# Check coordinator logs for connection error details
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} recent metadata errors ==="
  ssh "${coord}" "
    grep -E 'metadata.*timeout|connection.*refused|HikariPool|JDBC.*error|circuit.*open' \
      /var/log/pangrova/pangrova-coordinator.log | tail -10
  "
done

# Check the Pangrova health endpoint for metadata component status
curl -s https://coordinator-1:8080/v1/health | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
components = data.get('components', {})
print('Metadata status:', components.get('metadata', {}).get('status', 'unknown'))
print('Circuit breaker:', components.get('metadata', {}).get('circuitBreaker', 'unknown'))
"
```

---

## Step 2: Determine Partition Scope

```bash
# Test from coordinator AND from a worker (different network paths)
for node in coordinator-1 worker-0; do
  REACHABLE=$(ssh "${node}" "
    nc -zv ${PANGROVA_DB_HOST:-metadata-store} 5432 -w 3 2>&1 | grep -c 'open' || echo 0
  ")
  echo "${node} → metadata store: $([ '${REACHABLE}' -gt 0 ] && echo REACHABLE || echo UNREACHABLE)"
done

# Network path investigation
ssh coordinator-1 "
  echo '=== ping ==='
  ping -c 3 ${PANGROVA_DB_HOST:-metadata-store} || echo 'ping failed'

  echo '=== traceroute ==='
  traceroute -w 2 ${PANGROVA_DB_HOST:-metadata-store} 2>/dev/null | head -15 || echo 'traceroute not available'

  echo '=== DNS resolution ==='
  nslookup ${PANGROVA_DB_HOST:-metadata-store}
  dig +short ${PANGROVA_DB_HOST:-metadata-store}
"

# Check if firewall rules are causing the partition (e.g., game-day iptables)
ssh coordinator-1 "
  sudo iptables -L OUTPUT -n | grep DROP || echo 'No OUTPUT DROP rules'
  sudo iptables -L INPUT -n | grep DROP || echo 'No INPUT DROP rules'
"
```

---

## Step 3: Attempt Resolution

### Step 3A: Network / Firewall Issue

```bash
# If game-day iptables rules are causing the partition, remove them
ssh coordinator-1 "
  # List DROP rules with line numbers
  sudo iptables -L OUTPUT -n --line-numbers | grep DROP
  sudo iptables -L INPUT -n --line-numbers | grep DROP

  # Remove specific DROP rules (replace <num> with the actual rule number)
  # sudo iptables -D OUTPUT <num>
  # sudo iptables -D INPUT <num>
"

# After removing rules, re-test connectivity
ssh coordinator-1 "nc -zv ${PANGROVA_DB_HOST:-metadata-store} 5432 -w 5 && echo 'RESTORED'"

# If security group / network ACL (cloud environments)
# AWS: check aws ec2 describe-network-acls --filters "Name=association.subnet-id,Values=<subnet>"
# GCP: gcloud compute firewall-rules list --filter="targetTags:coordinator"
```

### Step 3B: Database Process Down

```bash
# Check PostgreSQL status on the metadata store host
ssh "${PANGROVA_DB_HOST:-metadata-store}" "
  systemctl status postgresql
  pg_isready -h localhost -p 5432 -U pangrova
"

# If PostgreSQL is stopped, start it
ssh "${PANGROVA_DB_HOST:-metadata-store}" "
  sudo systemctl start postgresql
  sleep 5
  pg_isready -h localhost -p 5432 -U pangrova && echo 'PostgreSQL: UP'
"

# Verify pangrova_meta and pangrova_ha databases are accessible
ssh "${PANGROVA_DB_HOST:-metadata-store}" "
  psql -U pangrova -d pangrova_meta -c 'SELECT 1;' && echo 'pangrova_meta: OK'
  psql -U pangrova -d pangrova_ha   -c 'SELECT 1;' && echo 'pangrova_ha: OK'
"
```

### Step 3C: DNS Resolution Issue

```bash
# If DNS is failing but IP works directly
ssh coordinator-1 "
  # Test with direct IP address
  nc -zv ${PANGROVA_DB_HOST_IP:-<db_ip>} 5432 -w 5 && echo 'Direct IP: works'

  # Check /etc/hosts for override
  grep -E 'pangrova|metadata' /etc/hosts

  # Check resolv.conf
  cat /etc/resolv.conf
"
```

---

## Step 4: Verify Recovery

```bash
# Test metadata store connectivity from each coordinator
for coord in coordinator-1 coordinator-2 coordinator-3; do
  STATUS=$(ssh "${coord}" "nc -zv ${PANGROVA_DB_HOST:-metadata-store} 5432 -w 5 2>&1 | grep -c 'open' || echo 0")
  echo "${coord}: $([ '${STATUS}' -gt 0 ] && echo CONNECTED || echo STILL UNREACHABLE)"
done

# Verify Pangrova coordinator has reconnected
sleep 10  # Allow JDBC pool to reconnect (pool validation interval)

curl -s https://coordinator-1:8080/v1/health | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
metadata_status = data.get('components', {}).get('metadata', {}).get('status', 'unknown')
print(f'Metadata component: {metadata_status}')
"

# Test a DDL operation to confirm full recovery
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator-1:8080/v1/sql" \
  -d '{"query": "SHOW SCHEMAS"}')
echo "DDL test (SHOW SCHEMAS): HTTP ${STATUS}"  # Expected: 200

# Force metadata cache invalidation to clear any stale entries served during the partition
curl -s -X POST "https://coordinator-1:8080/v1/admin/cache/invalidate" \
  -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}" | python3 -m json.tool
```

---

## Step 5: Validate Data Consistency

```bash
# Check for stale metadata: verify catalog reflects current database state
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator-1:8080/v1/sql" \
  -d '{"query": "DESCRIBE benchmark.lineitem"}' | python3 -m json.tool | head -20

# Check if any DDL operations that were queued during the partition need to be re-run
ssh coordinator-1 "
  grep -E 'DDL.*failed|CREATE.*failed|DROP.*failed|ALTER.*failed' \
    /var/log/pangrova/pangrova-coordinator.log | tail -20
"

# Run the coordinator-side consistency check
curl -s "https://coordinator-1:8080/v1/admin/metadata/consistency-check" \
  -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}" | python3 -m json.tool 2>/dev/null || \
  echo "(consistency check endpoint not available — manual verification required)"
```

---

## Recovery Verification Checklist

- [ ] Metadata store reachable from all coordinator nodes on TCP port 5432
- [ ] Coordinator health endpoint shows metadata component status: UP
- [ ] DDL operations succeed (`SHOW SCHEMAS` returns HTTP 200)
- [ ] Metadata cache has been invalidated post-recovery
- [ ] No stale table definitions being served (spot-check `DESCRIBE table`)
- [ ] Circuit breaker closed (if it was open during partition)
- [ ] Monitoring alerts cleared in Grafana/Alertmanager

---

## Escalation Criteria

| Condition | Action |
|-----------|--------|
| Partition > 5 minutes with no resolution | Page on-call SRE lead |
| PostgreSQL will not start | Page DBA immediately |
| Inconsistent catalog detected after recovery | Page data platform lead; freeze writes until resolved |
| Data loss in pangrova_meta database | Page data platform lead + engineering manager |
| JDBC connection pool not recovering after network restored | Restart coordinator process (last resort); page on-call SRE |

---

## Post-Incident

1. Review PostgreSQL HA configuration — consider read replicas and automatic failover
2. Review metadata cache TTL: shorter TTL reduces stale-serving window during partitions
3. Evaluate metadata store co-location with coordinator nodes to reduce network hops
4. Confirm `metadata_store_unreachable` alert fired within 30s of partition start
5. Document any DDL operations that failed and need re-execution
