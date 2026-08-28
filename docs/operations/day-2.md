---
title: "Day-2 Operations"
sidebar_label: "Day-2 Operations"
---

# Pangrova — Day-2 Operations

> **Summary.** Rolling upgrades, online DDL, disaster recovery, autoscaling, and failure-recovery SOPs.

## 1. Operations Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Operations Architecture                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Operations capabilities:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Capability         │ Description                 │ Implementation      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Rolling Upgrade    │ Zero-downtime upgrade       │ Rolling restart+cmp │   │
│  │  Online DDL         │ Non-blocking schema change │ Async exec+progress │   │
│  │  Disaster Recovery  │ Cross-region DR             │ Metadata+data repl. │  │
│  │  Auto Scaling       │ Autoscaling                 │ Load-based elastic. │   │
│  │  Config Hot Reload  │ Config hot reload           │ Dynamic config ctr. │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Rolling Upgrade

### 2.1 Upgrade Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Rolling Upgrade Architecture                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Upgrade order:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Worker nodes (upgraded one by one)                                   │   │
│  │     Worker-1 → Worker-2 → Worker-3 → ...                                │   │
│  │                                                                          │   │
│  │  2. Coordinator nodes (upgraded one by one)                              │   │
│  │     Coordinator-1 → Coordinator-2 → Coordinator-3                       │   │
│  │                                                                          │   │
│  │  3. Verification & rollback points                                       │   │
│  │     Verify after each node upgrade; roll back immediately on failure     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Version compatibility requirements:                                            │
│  • An N-version Coordinator must be compatible with N-1 version Workers          │
│  • An N-version Worker must be compatible with N-1 version Coordinators          │
│  • Protocol version is managed independently; incompatible changes bump it       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Upgrade Flow

```java
/**
 * Rolling upgrade controller
 */
public class RollingUpgradeController {

    /**
     * Execute rolling upgrade
     */
    public UpgradeResult upgrade(UpgradeRequest request) {
        String targetVersion = request.getTargetVersion();

        // 1. Pre-check
        preCheck(targetVersion);

        // 2. Upgrade Workers
        List<WorkerInfo> workers = workerManager.getAllWorkers();
        for (WorkerInfo worker : workers) {
            upgradeWorker(worker, targetVersion);

            // Verify
            if (!verifyWorker(worker)) {
                rollbackWorker(worker);
                return UpgradeResult.failed("Worker upgrade failed: " + worker.getId());
            }
        }

        // 3. Upgrade Coordinators
        List<CoordinatorInfo> coordinators = getCoordinators();
        for (CoordinatorInfo coordinator : coordinators) {
            upgradeCoordinator(coordinator, targetVersion);

            if (!verifyCoordinator(coordinator)) {
                rollbackCoordinator(coordinator);
                return UpgradeResult.failed("Coordinator upgrade failed: " + coordinator.getId());
            }
        }

        return UpgradeResult.success();
    }

    /**
     * Upgrade a single Worker
     */
    private void upgradeWorker(WorkerInfo worker, String targetVersion) {
        // 1. Mark as draining; stop accepting new tasks
        workerManager.setDraining(worker.getId(), true);

        // 2. Wait for existing tasks to complete
        waitForTasksComplete(worker.getId(), Duration.ofMinutes(5));

        // 3. Stop the Worker
        workerManager.stopWorker(worker.getId());

        // 4. Update version and restart
        deployNewVersion(worker, targetVersion);
        workerManager.startWorker(worker.getId());

        // 5. Wait for the Worker to be ready
        waitForWorkerReady(worker.getId(), Duration.ofMinutes(2));

        // 6. Cancel draining
        workerManager.setDraining(worker.getId(), false);
    }

    /**
     * upgradeWorker() Error Handling Specification
     *
     * 1. Drain Timeout: If waitForTasksComplete() exceeds drain-timeout (default 5m),
     *    mark Worker as DRAIN_TIMEOUT, force-stop remaining tasks (Coordinator will
     *    reschedule them to other Workers), then proceed with upgrade.
     *
     * 2. Deploy Failure: If deployNewVersion() throws, automatically rollback to
     *    previous version and restart. Mark Worker as UPGRADE_FAILED.
     *
     * 3. Startup Timeout: If waitForWorkerReady() exceeds 2m, rollback to previous
     *    version and restart.
     *
     * 4. Partial Upgrade Failure Strategy (controlled by rolling-upgrade.rollback-on-failure):
     *    - true (default): Rollback ALL already-upgraded Workers to previous version,
     *      restoring the cluster to a homogeneous state.
     *    - false: Keep already-upgraded Workers running on new version (mixed-version
     *      cluster). Relies on protocol version compatibility (§2.3) to ensure N and
     *      N+1 versions can coexist.
     */
}


```

### 2.3 Version Compatibility

```java
/**
 * Version compatibility checker
 */
public class VersionCompatibilityChecker {

    /**
     * Check version compatibility
     */
    public CompatibilityResult check(String currentVersion, String targetVersion) {
        Version current = Version.parse(currentVersion);
        Version target = Version.parse(targetVersion);

        // Check protocol version
        int currentProtocol = getProtocolVersion(current);
        int targetProtocol = getProtocolVersion(target);

        if (Math.abs(targetProtocol - currentProtocol) > 1) {
            return CompatibilityResult.incompatible(
                "Protocol version gap too large: " + currentProtocol + " -> " + targetProtocol);
        }

        // Check Native library version
        String currentNative = getNativeVersion(current);
        String targetNative = getNativeVersion(target);

        if (!isNativeCompatible(currentNative, targetNative)) {
            return CompatibilityResult.incompatible(
                "Native library incompatible: " + currentNative + " -> " + targetNative);
        }

        return CompatibilityResult.compatible();
    }

    /**
     * Protocol version mapping
     *
     * This mapping must stay consistent with overview.md §4.6 Canonical Protocol Mapping.
     * Each protocol version number corresponds to a compatible set of communication protocols:
     *
     * Protocol Version 1: REST v1 + Arrow Flight v1 + Protobuf TaskRequest v1
     * Protocol Version 2: REST v1 + Arrow Flight v1 + Protobuf TaskRequest v2 (adds trace context)
     * Protocol Version 3: REST v2 + Arrow Flight v2 + Protobuf TaskRequest v2
     *
     * Compatibility rules:
     * - Coordinator and Worker within the same Protocol Version are fully compatible
     * - Adjacent Protocol Versions (N and N+1) are guaranteed backward-compatible
     * - Spanning two versions (N and N+2) is not guaranteed compatible; must upgrade in steps
     *
     * When changing the protocol version, the following must be updated together:
     * 1. This mapping table
     * 2. overview.md §4.6 Canonical Protocol Mapping
     * 3. The Protobuf definitions of the pangrova-protocol module
     */
    private static final Map<String, Integer> PROTOCOL_VERSIONS = Map.of(
        "1.0.x", 1,
        "1.1.x", 1,
        "1.2.x", 2,
        "2.0.x", 3
    );
}
```

---

## 3. Online DDL

### 3.1 Online DDL Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Online DDL Architecture                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                  │
│  • DDL operations do not block queries currently executing                      │
│  • Large-table DDL can run asynchronously in the background                      │
│  • DDL progress query and cancellation are supported                             │
│                                                                                  │
│  DDL type classification:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Type              │ Blocking  │ Execution mode  │ Example              │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Metadata Only     │ Non-block │ Synchronous     │ ADD COLUMN, RENAME  │   │
│  │  Data Rewrite      │ Non-block │ Async background │ ALTER COLUMN TYPE   │   │
│  │  Table Lock        │ Brief lock│ Synchronous     │ DROP TABLE          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Asynchronous DDL Execution

```java
/**
 * Asynchronous DDL executor
 */
public class AsyncDdlExecutor {

    private final ExecutorService executor;
    private final DdlJobStore jobStore;

    /**
     * Submit a DDL job
     */
    public DdlJobHandle submit(DdlStatement statement) {
        DdlJob job = DdlJob.builder()
            .jobId(UUID.randomUUID().toString())
            .statement(statement)
            .status(DdlJobStatus.PENDING)
            .createdAt(Instant.now())
            .build();

        jobStore.save(job);

        executor.submit(() -> executeJob(job));

        return new DdlJobHandle(job.getJobId());
    }

    /**
     * Execute a DDL job
     */
    private void executeJob(DdlJob job) {
        try {
            jobStore.updateStatus(job.getJobId(), DdlJobStatus.RUNNING);

            DdlStatement statement = job.getStatement();

            if (statement instanceof AlterColumnType alter) {
                executeAlterColumnType(job, alter);
            } else if (statement instanceof RewriteTable rewrite) {
                executeRewriteTable(job, rewrite);
            }

            jobStore.updateStatus(job.getJobId(), DdlJobStatus.COMPLETED);

        } catch (Exception e) {
            jobStore.updateStatus(job.getJobId(), DdlJobStatus.FAILED, e.getMessage());
        }
    }

    /**
     * Execute a column-type change (requires data rewrite)
     */
    private void executeAlterColumnType(DdlJob job, AlterColumnType alter) {
        Table table = catalog.loadTable(alter.getTableIdentifier());

        // 1. Create new schema
        Schema newSchema = updateColumnType(table.schema(), alter.getColumn(), alter.getNewType());

        // 2. Rewrite data files in the background
        long totalFiles = countDataFiles(table);
        long processedFiles = 0;

        for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
            // Read old file, convert types, write new file
            DataFile newFile = rewriteWithNewSchema(file, newSchema);

            // Atomic replacement
            table.newRewrite()
                .deleteFile(file)
                .addFile(newFile)
                .commit();

            processedFiles++;
            jobStore.updateProgress(job.getJobId(), (double) processedFiles / totalFiles);
        }

        // 3. Update the table schema
        table.updateSchema().unionByNameWith(newSchema).commit();
    }
}

/**
 * DDL job status query
 */
public class DdlJobStatus {
    public static final String PENDING = "PENDING";
    public static final String RUNNING = "RUNNING";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    public static final String CANCELLED = "CANCELLED";
}
```

### 3.3 DDL Management SQL

```sql
-- View DDL jobs in progress
SHOW DDL JOBS;

-- View DDL progress
SHOW DDL JOB 'job-uuid';

-- Cancel DDL
CANCEL DDL JOB 'job-uuid';

-- Execute DDL asynchronously
ALTER TABLE iceberg.db.large_table ALTER COLUMN amount TYPE DECIMAL(20, 4) ASYNC;

-- View DDL history
SELECT * FROM system.ddl_jobs
WHERE table_name = 'iceberg.db.large_table'
ORDER BY created_at DESC;
```

---

## 4. Disaster Recovery

### 4.1 Disaster Recovery Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Disaster Recovery Architecture                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Active-active architecture:                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Region A (Primary)              Region B (Standby)                     │   │
│  │  ┌─────────────────┐            ┌─────────────────┐                    │   │
│  │  │  Coordinator    │            │  Coordinator    │                    │   │
│  │  │  Cluster        │            │  Cluster        │                    │   │
│  │  └────────┬────────┘            └────────┬────────┘                    │   │
│  │           │                              │                              │   │
│  │  ┌────────▼────────┐            ┌────────▼────────┐                    │   │
│  │  │  Worker         │            │  Worker         │                    │   │
│  │  │  Cluster        │            │  Cluster        │                    │   │
│  │  └────────┬────────┘            └────────┬────────┘                    │   │
│  │           │                              │                              │   │
│  │  ┌────────▼────────┐            ┌────────▼────────┐                    │   │
│  │  │  Metadata       │ ─────────► │  Metadata       │                    │   │
│  │  │  (PgSQL/ZK)     │ Sync repl. │  (PgSQL/ZK)     │                    │   │
│  │  └────────┬────────┘            └────────┬────────┘                    │   │
│  │           │                              │                              │   │
│  │  ┌────────▼────────┐            ┌────────▼────────┐                    │   │
│  │  │  Data Lake      │ ─────────► │  Data Lake      │                    │   │
│  │  │  (S3/HDFS)      │ Async repl.│  (S3/HDFS)      │                    │   │
│  │  └─────────────────┘            └─────────────────┘                    │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  RPO/RTO targets:                                                               │
│  • RPO (Recovery Point Objective): < 5 minutes                                  │
│  • RTO (Recovery Time Objective): < 30 minutes                                  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Metadata Replication

```java
/**
 * Metadata replication service
 */
public class MetadataReplicationService {

    private final MetadataStore primaryStore;
    private final MetadataStore standbyStore;

    /**
     * Replicate metadata changes synchronously
     */
    @Transactional
    public void replicate(MetadataChange change) {
        // 1. Write to the primary store
        primaryStore.apply(change);

        // 2. Write to the standby store synchronously (sync replication ensures consistency)
        try {
            standbyStore.apply(change);
        } catch (Exception e) {
            // Standby write failed; record to the replication queue and retry later
            replicationQueue.enqueue(change);
            log.warn("Standby replication failed, queued for retry", e);
        }
    }

    /**
     * Replication queue processing (handles failed replications)
     */
    @Scheduled(fixedRate = 1000)
    public void processReplicationQueue() {
        while (!replicationQueue.isEmpty()) {
            MetadataChange change = replicationQueue.peek();
            try {
                standbyStore.apply(change);
                replicationQueue.remove();
            } catch (Exception e) {
                // Retry failed; wait for next round
                break;
            }
        }
    }
}
```

### 4.3 Failover

```java
/**
 * Failover controller
 */
public class FailoverController {

    /**
     * Execute failover
     */
    public FailoverResult failover(FailoverRequest request) {
        // 1. Verify standby status
        if (!standbyHealthChecker.isHealthy()) {
            return FailoverResult.failed("Standby region is not healthy");
        }

        // 2. Check replication lag
        Duration replicationLag = getReplicationLag();
        if (replicationLag.compareTo(request.getMaxAcceptableLag()) > 0) {
            if (!request.isForceFailover()) {
                return FailoverResult.failed("Replication lag too high: " + replicationLag);
            }
            log.warn("Force failover with replication lag: {}", replicationLag);
        }

        // 3. Stop writes to the primary store
        primaryStore.setReadOnly(true);

        // 4. Wait for replication to catch up
        waitForReplicationCatchUp(Duration.ofMinutes(5));

        // 5. Promote standby to primary
        standbyStore.promoteToMaster();

        // 6. Update DNS/load balancer
        updateDnsToStandby();

        // 7. Notify all clients
        notifyClients();

        return FailoverResult.success();
    }

    /**
     * Failback (after the original primary recovers)
     */
    public void failback() {
        // 1. Demote the original primary to standby
        primaryStore.demoteToStandby();

        // 2. Sync data from the new primary
        syncFromNewMaster();

        // 3. Optional: switch back to the original primary
    }
}
```

### 4.4 Shared State Disaster Recovery

Disaster recovery flow for each shared-state component:

| Component | DR strategy | RPO | RTO | Recovery flow |
|------|----------|-----|-----|----------|
| ZooKeeper | 3-node quorum + cross-AZ deployment | 0 (strong consistency) | < 30s (automatic leader election) | Minority node failure recovers automatically; majority node failure requires restore from snapshot |
| Redis | Primary-replica replication + Sentinel automatic failover | < 1s | < 10s (Sentinel failover) | On primary failure, Sentinel automatically promotes a replica; on total failure, the system runs degraded (cache bypass to source) |
| PostgreSQL | Streaming replication + synchronous commit | < 1s (synchronous commit) | < 5min (manual/automatic failover) | On primary failure, fail over to a replica; promote after verifying replication lag |

**ZooKeeper DR flow**:
1. Deploy a 3- or 5-node ZooKeeper cluster, distributed across availability zones
2. Periodic snapshot backups (every 6 hours) to remote storage
3. Minority node failure: recovers automatically, no manual intervention
4. Majority node failure (cluster unavailable): restore from the latest snapshot; the Pangrova cluster pauses accepting new queries until ZK recovers

**Redis DR flow**:
1. Deploy Redis Sentinel or Redis Cluster mode
2. Primary failure: Sentinel fails over automatically, Pangrova reconnects automatically
3. All Redis unavailable: Pangrova runs degraded, all cache requests bypass to the source (Iceberg Catalog / PostgreSQL); performance degrades but functionality is unaffected
4. After recovery the cache warms up automatically (triggered by queries)

**PostgreSQL DR flow**:
1. Deploy streaming replication, enable synchronous commit (`synchronous_commit = on`)
2. Primary failure: verify the replica's replication position, then promote it to primary
3. After recovery the original primary rejoins as the new replica
4. Critical data (query state, audit logs) is periodically archived to remote storage

---

## 5. Auto Scaling

### 5.1 Scaling Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Auto Scaling Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Scaling decision flow:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Metrics Collector ──► Scaling Policy ──► Scaling Decision ──► Executor │   │
│  │        │                     │                    │                │     │   │
│  │        ▼                     ▼                    ▼                ▼     │   │
│  │  • CPU usage             • Threshold rules   • Scale Out        • K8s API│   │
│  │  • Memory usage          • Prediction model  • Scale In         • Cloud API│ │
│  │  • Queue depth           • Cooldown period   • No Action        • Manual │   │
│  │  • Query latency         • Time window                                       │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Scaling metrics:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Metric              │ Scale Out thr.  │ Scale In thr.  │ Weight       │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  CPU Usage           │ > 80%           │ < 30%          │ 0.3          │   │
│  │  Memory Usage        │ > 85%           │ < 40%          │ 0.2          │   │
│  │  Query Queue Depth   │ > 100           │ < 10           │ 0.3          │   │
│  │  Avg Query Latency   │ > 5s            │ < 1s           │ 0.2          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Scaling Policy

```java
/**
 * Auto-scaling controller
 */
public class AutoScalingController {

    private final MetricsCollector metricsCollector;
    private final ScalingPolicy policy;
    private final ScalingExecutor executor;

    /**
     * Scaling decision (runs on a schedule)
     */
    @Scheduled(fixedRate = 30_000)
    public void evaluate() {
        ClusterMetrics metrics = metricsCollector.collect();
        ScalingDecision decision = policy.evaluate(metrics);

        if (decision.getAction() == ScalingAction.NONE) {
            return;
        }

        // Check the cooldown period
        if (!cooldownManager.canScale(decision.getAction())) {
            log.info("Scaling action {} blocked by cooldown", decision.getAction());
            return;
        }

        // Execute scaling
        executor.execute(decision);
        cooldownManager.recordScaling(decision.getAction());
    }
}

/**
 * Scaling policy
 */
public class ScalingPolicy {

    private final ScalingConfig config;

    /**
     * Evaluate whether scaling is needed
     */
    public ScalingDecision evaluate(ClusterMetrics metrics) {
        double score = calculateScore(metrics);

        if (score > config.getScaleOutThreshold()) {
            int targetCount = calculateScaleOutTarget(metrics);
            return ScalingDecision.scaleOut(targetCount);
        }

        if (score < config.getScaleInThreshold()) {
            int targetCount = calculateScaleInTarget(metrics);
            return ScalingDecision.scaleIn(targetCount);
        }

        return ScalingDecision.none();
    }

    /**
     * Compute the composite score
     */
    private double calculateScore(ClusterMetrics metrics) {
        return config.getCpuWeight() * metrics.getAvgCpuUsage()
             + config.getMemoryWeight() * metrics.getAvgMemoryUsage()
             + config.getQueueWeight() * normalizeQueueDepth(metrics.getQueueDepth())
             + config.getLatencyWeight() * normalizeLatency(metrics.getAvgLatency());
    }

    /**
     * Compute the scale-out target count
     */
    private int calculateScaleOutTarget(ClusterMetrics metrics) {
        int currentCount = metrics.getWorkerCount();
        int maxCount = config.getMaxWorkers();

        // Compute the extra nodes needed based on queue depth
        int queueBasedIncrease = metrics.getQueueDepth() / config.getQueriesPerWorker();

        // Scale out by at least 1, at most 50%
        int increase = Math.max(1, Math.min(queueBasedIncrease, currentCount / 2));

        return Math.min(currentCount + increase, maxCount);
    }

    /**
     * Compute the scale-in target count
     */
    private int calculateScaleInTarget(ClusterMetrics metrics) {
        int currentCount = metrics.getWorkerCount();
        int minCount = config.getMinWorkers();

        // Compute how many nodes can be removed based on utilization
        double utilization = metrics.getAvgCpuUsage();
        int targetCount = (int) Math.ceil(currentCount * utilization / 0.6);

        return Math.max(targetCount, minCount);
    }
}
```

### 5.3 Predictive Scaling

```java
/**
 * Predictive scaling (based on historical patterns)
 */
public class PredictiveScaling {

    private final TimeSeriesPredictor predictor;

    /**
     * Predict future load and scale out proactively
     */
    public ScalingDecision predictAndScale() {
        // Fetch historical data
        List<MetricPoint> history = metricsStore.getHistory(Duration.ofDays(7));

        // Predict load for the next 15 minutes
        PredictionResult prediction = predictor.predict(history, Duration.ofMinutes(15));

        if (prediction.getConfidence() < 0.8) {
            // Insufficient confidence; do not perform predictive scale-out
            return ScalingDecision.none();
        }

        double predictedLoad = prediction.getValue();
        int currentWorkers = workerManager.getWorkerCount();
        int requiredWorkers = (int) Math.ceil(predictedLoad / config.getLoadPerWorker());

        if (requiredWorkers > currentWorkers) {
            return ScalingDecision.scaleOut(requiredWorkers)
                .withReason("Predictive scaling: expected load " + predictedLoad);
        }

        return ScalingDecision.none();
    }
}
```

### 5.4 Kubernetes Integration

```java
/**
 * Kubernetes scaling executor
 */
public class KubernetesScalingExecutor implements ScalingExecutor {

    private final KubernetesClient k8sClient;

    @Override
    public void execute(ScalingDecision decision) {
        String deploymentName = "pangrova-worker";
        String namespace = config.getNamespace();

        // Update the Deployment replica count
        k8sClient.apps().deployments()
            .inNamespace(namespace)
            .withName(deploymentName)
            .scale(decision.getTargetCount());

        // Wait for scaling to complete
        waitForScalingComplete(deploymentName, namespace, decision.getTargetCount());

        log.info("Scaled {} to {} replicas", deploymentName, decision.getTargetCount());
    }

    /**
     * Wait for scaling to complete
     */
    private void waitForScalingComplete(String name, String namespace, int target) {
        k8sClient.apps().deployments()
            .inNamespace(namespace)
            .withName(name)
            .waitUntilCondition(
                d -> d.getStatus().getReadyReplicas() == target,
                5, TimeUnit.MINUTES);
    }
}
```



### 5.5 Scaling & Resource Management Linkage

Auto-scaling changes Worker count, which affects resource scheduling. This section describes the integration between scaling events and the resource management layer ([06 §5](../architecture/resource-management.md), [13 §3](../architecture/multi-tenancy.md)).

| Event | Resource Impact | Mechanism |
|-------|----------------|-----------|
| Scale Out (new Worker joins) | Coordinator scheduler automatically discovers new Worker via ZK ephemeral node ([01 §2.4](../architecture/distributed.md)). New Worker's ResourcePool is initialized with default tenant shares. | Automatic — no manual intervention |
| Scale In (Worker drain) | Coordinator stops scheduling new tasks to draining Worker. ResourcePool shares on draining Worker are NOT redistributed — remaining Workers simply receive proportionally more tasks. | Automatic via drain flag |
| Worker removed after drain | ZK CHILD_REMOVED event triggers Coordinator to remove Worker from scheduling pool. In-flight tasks on removed Worker are rescheduled. | Automatic via ZK watch |

**ResourceQueue `max-running` and Worker count**: The `max-running` parameter in ResourceQueue ([06 §5](../architecture/resource-management.md)) limits concurrent queries at the Coordinator level, independent of Worker count. Scaling Workers does NOT require adjusting `max-running` — more Workers means each query gets more parallelism, not more concurrent queries.



> **Auto-Scaling Decision Model**: The auto-scaling system uses a two-level decision mechanism:
> 1. **Proposed per-metric emergency thresholds:** If any single metric exceeds
> its threshold (for example `cpu-threshold: 0.8` or `memory-threshold: 0.85`),
> scaling is triggered without waiting for the composite score. These keys are not
> part of the current typed configuration surface.
> 2. **Weighted composite score** (§5.1/§5.2): Under normal conditions, the composite score (cpu-weight × CPU + memory-weight × Memory + queue-weight × Queue + latency-weight × Latency) is compared against `scale-out-threshold` (0.8) and `scale-in-threshold` (0.3) for gradual scaling decisions.

---

## 6. Config Hot Reload

### 6.1 Configuration Management Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Config Hot Reload Architecture                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Configuration categories:                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Category          │ Hot reload│ Example                                  │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Runtime Config    │ ✓       │ Concurrency, timeout, log level          │   │
│  │  Resource Limits   │ ✓       │ Memory limits, queue size                │   │
│  │  Feature Flags     │ ✓       │ Feature toggles, canary config           │   │
│  │  Security Config   │ ✓       │ Permission rules, masking rules          │   │
│  │  Connector Config  │ ✗       │ Data source connections, Catalog config  │   │
│  │  Cluster Config    │ ✗       │ Node addresses, ports                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Configuration source priority:                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Command-line arguments (highest priority)                            │   │
│  │  2. Environment variables                                                │   │
│  │  3. Dynamic configuration center (ZooKeeper)                             │   │
│  │  4. Configuration files (*.yaml)                                         │   │
│  │  5. Default values (lowest priority)                                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Dynamic Configuration Service

```java
/**
 * Dynamic configuration service
 */
public class DynamicConfigService {

    private final ConfigStore configStore;
    private final Map<String, List<ConfigChangeListener>> listeners = new ConcurrentHashMap<>();
    private final AtomicReference<ConfigSnapshot> currentConfig = new AtomicReference<>();

    /**
     * Initialize and watch for configuration changes
     */
    @PostConstruct
    public void init() {
        // Load the initial configuration
        currentConfig.set(configStore.loadAll());

        // Watch for configuration changes
        configStore.watch(this::onConfigChange);
    }

    /**
     * Get a configuration value
     */
    public <T> T get(String key, Class<T> type) {
        return currentConfig.get().get(key, type);
    }

    /**
     * Get a configuration value (with a default)
     */
    public <T> T get(String key, Class<T> type, T defaultValue) {
        T value = currentConfig.get().get(key, type);
        return value != null ? value : defaultValue;
    }

    /**
     * Register a configuration change listener
     */
    public void addListener(String keyPattern, ConfigChangeListener listener) {
        listeners.computeIfAbsent(keyPattern, k -> new CopyOnWriteArrayList<>())
            .add(listener);
    }

    /**
     * Configuration change callback
     */
    private void onConfigChange(ConfigChangeEvent event) {
        // Update the local cache
        ConfigSnapshot newConfig = currentConfig.get().with(event.getKey(), event.getNewValue());
        currentConfig.set(newConfig);

        // Notify listeners
        notifyListeners(event);

        log.info("Config updated: {} = {}", event.getKey(), event.getNewValue());
    }

    /**
     * Notify listeners
     */
    private void notifyListeners(ConfigChangeEvent event) {
        for (Map.Entry<String, List<ConfigChangeListener>> entry : listeners.entrySet()) {
            if (matchPattern(entry.getKey(), event.getKey())) {
                for (ConfigChangeListener listener : entry.getValue()) {
                    try {
                        listener.onConfigChange(event);
                    } catch (Exception e) {
                        log.error("Config listener failed", e);
                    }
                }
            }
        }
    }
}
```

### 6.3 Config Hot Reload Example

```java
/**
 * Resource manager - supports config hot reload
 */
public class ResourceManager {

    private final DynamicConfigService configService;
    private volatile int maxConcurrency;
    private volatile long queryTimeout;

    @PostConstruct
    public void init() {
        // Load the initial configuration
        reloadConfig();

        // Register a configuration change listener
        configService.addListener("coordinator.query.*", event -> reloadConfig());
    }

    private void reloadConfig() {
        this.maxConcurrency = configService.get("coordinator.query.max-concurrent-queries", Integer.class, 100);
        this.queryTimeout = configService.get("coordinator.query.max-execution-time", Long.class, 300_000L);

        log.info("Resource config reloaded: maxConcurrency={}, queryTimeout={}ms",
            maxConcurrency, queryTimeout);
    }
}

/**
 * Dynamic log-level adjustment
 */
public class LogLevelManager {

    @PostConstruct
    public void init() {
        configService.addListener("logging.*", this::onLogLevelChange);
    }

    private void onLogLevelChange(ConfigChangeEvent event) {
        String loggerName = event.getKey().replace("logging.", "");
        String level = event.getNewValue().toString();

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = context.getLogger(loggerName);
        logger.setLevel(Level.valueOf(level));

        log.info("Logger {} level changed to {}", loggerName, level);
    }
}
```

### 6.4 Configuration Management SQL

```sql
-- View the current configuration
SHOW CONFIG;

-- View a specific configuration
SHOW CONFIG LIKE 'resource%';

-- Modify configuration dynamically
SET CONFIG 'resource.max_concurrency' = 200;

-- View configuration change history
SELECT * FROM system.config_history
WHERE key LIKE 'resource%'
ORDER BY changed_at DESC
LIMIT 100;

-- Reset configuration to default value
RESET CONFIG 'resource.max_concurrency';
```

---

## 7. Operations Configuration Summary

### 7.1 Complete Configuration Example

```yaml
# pangrova-config.yaml

# Rolling upgrade configuration
rolling-upgrade:
  drain-timeout: 5m
  health-check-interval: 10s
  max-parallel-upgrades: 1
  rollback-on-failure: true

# Online DDL configuration
online-ddl:
  async-executor-threads: 4
  max-concurrent-jobs: 10
  job-timeout: 24h
  checkpoint-interval: 1000

# Disaster recovery configuration
disaster-recovery:
  mode: active-standby  # active-standby | active-active
  replication:
    type: sync  # sync | async
    max-lag: 5m
  failover:
    auto-failover: false
    health-check-interval: 10s
    failure-threshold: 3

# Auto-scaling configuration
auto-scaling:
  enabled: true
  min-workers: 3
  max-workers: 100
  scale-out-threshold: 0.8
  scale-in-threshold: 0.3
  cooldown:
    scale-out: 3m
    scale-in: 10m
  metrics:
    cpu-weight: 0.3
    memory-weight: 0.2
    queue-weight: 0.3
    latency-weight: 0.2
  predictive:
    enabled: true
    lookahead: 15m
    min-confidence: 0.8

# Config hot reload
config:
  source: zookeeper  # zookeeper (designated) | file (local fallback)
  zookeeper:
    connect-string: zk1:2181,zk2:2181,zk3:2181
    base-path: /pangrova/config
  refresh-interval: 30s

# Proposed hot-reloadable runtime configuration
#
runtime:
  coordinator.query.max-concurrent-queries: 100
  coordinator.query.max-execution-time: 30m
  memory.spill.threshold: 0.8
  observability.logging.level: INFO
```

### 7.2 Operations Command Summary

```sql
-- Rolling upgrade
CALL system.start_rolling_upgrade('2.0.0');
CALL system.check_upgrade_status();
CALL system.rollback_upgrade();

-- Online DDL
SHOW DDL JOBS;
SHOW DDL JOB 'job-uuid';
CANCEL DDL JOB 'job-uuid';

-- Disaster recovery
CALL system.failover('region-b');
CALL system.failback();
SHOW REPLICATION STATUS;

-- Scaling
CALL system.scale_workers(10);
SHOW SCALING STATUS;
SHOW SCALING HISTORY;

-- Configuration management
SHOW CONFIG;
SET CONFIG 'key' = 'value';
RESET CONFIG 'key';
```

---

## 8. System Bootstrap Sequence

This section defines the complete bootstrap sequence for an Pangrova cluster, from cold start to accepting queries, including external dependencies, startup order, health checks, and readiness determination.

### 8.1 Bootstrap Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         System Bootstrap Sequence                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Phase A: Infrastructure (external dependencies)                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  A1. ZooKeeper Quorum (≥3 nodes)    → Election, registration, locks    │   │
│  │  A2. PostgreSQL Cluster (primary/replica) → Query state, Catalog, audit │   │
│  │  A3. Redis Cluster (≥3 primaries)  → Metadata cache, session cache     │   │
│  │  A4. Object Storage (S3/HDFS)       → Data files, shuffle persistence   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                              │ All ready                                       │
│                              ▼                                                   │
│  Phase B: Coordinator cluster (≥2 nodes)                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  B1. haService.start()              → ZK connection                    │   │
│  │  B2. haService.registerAsCandidate()→ Leader Election                  │   │
│  │  B3. metadataService.initialize()   → Catalog load (PostgreSQL → memory)│   │
│  │  B4. workerManager.startDiscovery() → Watch /pangrova/workers            │   │
│  │  B5. httpServer.start()             → REST API ready                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                              │ HTTP port ready                                  │
│                              ▼                                                   │
│  Phase C: Worker cluster (≥1 node)                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  C1. nativeBridge.loadLibrary()     → JNI loads the Native library     │   │
│  │  C2. nativeBridge.initializeVelox() → Velox engine initialization       │   │
│  │  C3. memoryManager.initialize()     → Memory pool initialization        │   │
│  │  C4. connectorManager.loadPlugins() → Connector plugin loading          │   │
│  │  C5. flightServer.start()           → Data Plane ready                 │   │
│  │  C6. httpServer.start()             → Control Plane ready              │   │
│  │  C7. serviceRegistry.register()     → ZK Ephemeral Node               │   │
│  │  C8. serviceRegistry.startHeartbeat()→ First heartbeat                 │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                              │ Coordinator discovers Workers                    │
│                              ▼                                                   │
│  Phase D: System readiness determination                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  D1. Coordinator detects ≥1 Worker ACTIVE                              │   │
│  │  D2. Catalog health check passes (≥1 Catalog ACTIVE)                    │   │
│  │  D3. SELECT 1 executes successfully                                    │   │
│  │  D4. Load Balancer health check passes → start accepting external traffic│  │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Startup Sequence Diagram

```
Timeline →

ZooKeeper    PgSQL    Redis    Coordinator-1    Coordinator-2    Worker-1    Worker-2    LB
    │          │        │           │                │              │           │         │
    │ Ready    │        │           │                │              │           │         │
    ├──────────┤ Ready  │           │                │              │           │         │
    │          ├────────┤ Ready     │                │              │           │         │
    │          │        ├──────────►│                │              │           │         │
    │          │        │           │ Phase B        │              │           │         │
    │          │        │           │ start()        │ start()      │           │         │
    │◄─────────────────────────────┤ ZK connect     │              │           │         │
    │◄──────────────────────────────────────────────┤ ZK connect   │           │         │
    │          │        │           │                │              │           │         │
    │          │◄───────────────────┤ load catalogs  │              │           │         │
    │◄─────────────────────────────┤ watch workers   │              │           │         │
    │          │        │           │ HTTP ready     │ HTTP ready   │           │         │
    │          │        │           │                │              │           │         │
    │          │        │           │                │              │ Phase C   │         │
    │          │        │           │                │              │ start()   │ start() │
    │          │        │           │                │              │ native    │ native  │
    │          │        │           │                │              │ init      │ init    │
    │          │        │           │                │              │ flight    │ flight  │
    │          │        │           │                │              │ http      │ http    │
    │◄──────────────────────────────────────────────────────────────┤ register  │         │
    │◄─────────────────────────────────────────────────────────────────────────┤ register│
    │          │        │           │                │              │           │         │
    │          │        │           │ CHILD_ADDED    │ CHILD_ADDED  │           │         │
    │          │        │           │ Worker-1       │ Worker-1     │           │         │
    │          │        │           │ Worker-2       │ Worker-2     │           │         │
    │          │        │           │                │              │           │         │
    │          │        │           │◄───────────────────────────────┤ heartbeat│         │
    │          │        │           │◄──────────────────────────────────────────┤heartbeat│
    │          │        │           │                │              │           │         │
    │          │        │           │ Phase D        │              │           │         │
    │          │        │           │ ≥1 Worker ACTIVE              │           │         │
    │          │        │           │ SELECT 1 OK    │              │           │         │
    │          │        │           │                │              │           │         │
    │          │        │           │────────────────────────────────────────────────────►│
    │          │        │           │                │              │           │  LB     │
    │          │        │           │                │              │           │  health │
    │          │        │           │                │              │           │  check  │
    │          │        │           │                │              │           │  pass   │
    │          │        │           │                │              │           │         │
    │          │        │           │ ═══ SYSTEM READY ═══          │           │         │
```

### 8.3 Health Check Endpoints

| Component | Endpoint | Readiness condition | Purpose |
|------|------|---------|------|
| Coordinator | `GET /v1/health` | HTTP port ready | LB health check |
| Coordinator | `GET /v1/health/ready` | HTTP ready + ≥1 Worker ACTIVE + ≥1 Catalog ACTIVE | LB readiness check (accepts queries only when ready) |
| Worker | `GET /v1/health` | HTTP + Flight ports ready | Coordinator health check |
| Worker | `GET /v1/health/ready` | HTTP + Flight ready + Native engine initialized | Coordinator readiness check |

### 8.4 Startup Failure Handling

| Failure scenario | Impact | Handling strategy |
|---------|------|---------|
| ZK unavailable | Coordinator/Worker cannot start | fail-fast, process exits, restart after ZK recovers |
| PostgreSQL unavailable | Coordinator cannot load Catalog | fail-fast, process exits |
| Redis unavailable | Metadata cache unavailable | degraded start, skip cache warm-up, log WARN |
| Native library load failure | Worker cannot start | fail-fast, process exits, check LD_LIBRARY_PATH |
| Connector plugin load failure | Corresponding Catalog unavailable | degraded start, mark Catalog as DEGRADED, other Catalogs work |
| No Worker registered | System cannot execute queries | Coordinator starts normally but `/v1/health/ready` returns 503 |

### 8.5 Startup Script Reference

```bash
#!/bin/bash
# bin/start-cluster.sh — full cluster startup script

set -euo pipefail

echo "=== Phase A: Verify Infrastructure ==="
./bin/check-zookeeper.sh   || { echo "ZooKeeper not ready"; exit 1; }
./bin/check-postgresql.sh  || { echo "PostgreSQL not ready"; exit 1; }
./bin/check-redis.sh       || { echo "Redis not ready"; exit 1; }
echo "Infrastructure ready."

echo "=== Phase B: Start Coordinators ==="
for host in $COORDINATOR_HOSTS; do
    ssh $host "nohup java -jar pangrova-coordinator.jar &"
done
# Wait for Coordinators to be ready
for host in $COORDINATOR_HOSTS; do
    ./bin/wait-for-health.sh "http://$host:8080/v1/health" 60
done
echo "Coordinators ready."

echo "=== Phase C: Start Workers ==="
for host in $WORKER_HOSTS; do
    ssh $host "nohup java -jar pangrova-worker.jar &"
done
# Wait for Workers to register
for host in $WORKER_HOSTS; do
    ./bin/wait-for-health.sh "http://$host:8080/v1/health/ready" 120
done
echo "Workers ready."

echo "=== Phase D: System Readiness Check ==="
COORDINATOR=${COORDINATOR_HOSTS%% *}
./bin/wait-for-health.sh "http://$COORDINATOR:8080/v1/health/ready" 30
# Smoke test
curl -s -X POST "http://$COORDINATOR:8080/v1/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT 1","sessionProperties":{}}' | jq .
echo "=== SYSTEM READY ==="
```

> **Cross-Reference**: For the Coordinator startup sequence see [distributed.md §1.2](../architecture/distributed.md); for the Worker startup sequence see [distributed.md §2.4](../architecture/distributed.md); for Catalog initialization see [metadata.md §3.2](../architecture/metadata.md).

---

## 9. Failure Recovery SOP (Standard Operating Procedure)

> **Cross-reference**: The error codes referenced in this section's SOPs come from the PangrovaErrorCode definitions in [resilience.md §2.2](../architecture/resilience.md). When diagnosing, use the error-code classification to quickly locate the failure domain.

### 9.1 Failure Classification and Response

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Incident Response Matrix                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Severity classification:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Level │ Impact scope          │ Response│ Recovery│ Notification       │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  P0    │ Whole cluster down    │ < 5min  │ < 30min │ All + mgmt         │   │
│  │  P1    │ Partial feature outage│ < 15min │ < 1h    │ On-call + owner    │   │
│  │  P2    │ Severe perf. drop     │ < 30min │ < 4h    │ On-call            │   │
│  │  P3    │ Non-critical fault    │ < 2h    │ < 24h   │ Ticket             │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Error code -> SOP quick routing:                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  PangrovaErrorCode          │ Failure domain │ SOP section                │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  COORDINATOR_UNAVAILABLE  │ Coordinator     │ §9.2                   │   │
│  │  (4203)                   │                 │                        │   │
│  │  WORKER_UNAVAILABLE       │ Worker          │ §9.3                   │   │
│  │  (4204)                   │                 │                        │   │
│  │  NATIVE_ERROR (3101)      │ Worker/Native   │ §9.3 (Native crash)    │   │
│  │  JNI_ERROR (3102)         │ Worker/Native   │ §9.3 (Native crash)    │   │
│  │  MEMORY_EXCEEDED (2002)   │ Query/Resource  │ §9.4                   │   │
│  │  QUERY_TIMEOUT (2101)     │ Query/Resource  │ §9.4                   │   │
│  │  RESOURCE_EXHAUSTED (2001)│ Query/Resource  │ §9.4                   │   │
│  │  STORAGE_ERROR (4001)     │ Storage         │ §9.5                   │   │
│  │  NETWORK_ERROR (4101)     │ Network/Storage │ §9.5                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.1.1 Global Error Code -> Module -> Recovery Action Mapping

> This table covers all PangrovaErrorCode values defined in [resilience.md §2.2](../architecture/resilience.md), mapping each to its responsible module, typical trigger scenarios, and recovery actions. Operators can use the error code to directly locate the recovery path.

| Error code | Name | Scope | Responsible module | Typical trigger scenario | Retryable | Recovery action | SOP |
|--------|------|------|---------|------------|--------|---------|-----|
| 1001 | `SYNTAX_ERROR` | USER_ERROR | Coordinator (Parser) | SQL syntax error | ✗ | Fix the SQL statement | — |
| 1101 | `TABLE_NOT_FOUND` | USER_ERROR | Coordinator (Metadata) | Table name misspelled or table does not exist | ✗ | `SHOW TABLES` to confirm the table name | — |
| 1102 | `COLUMN_NOT_FOUND` | USER_ERROR | Coordinator (Metadata) | Column name misspelled | ✗ | `DESCRIBE table` to confirm the column name | — |
| 1201 | `ACCESS_DENIED` | USER_ERROR | Security ([12](../architecture/security.md)) | Insufficient RBAC permissions | ✗ | Contact the administrator for authorization | — |
| 1301 | `SCHEMA_INCOMPATIBLE` | USER_ERROR | Governance ([11](../architecture/data-governance.md)) | Schema Evolution compatibility check failed | ✗ | Check Schema change compatibility rules | — |
| 1302 | `COLUMN_TYPE_MISMATCH` | USER_ERROR | Governance ([11](../architecture/data-governance.md)) | Column type mismatch | ✗ | Fix the ALTER TABLE statement | — |
| 1401 | `CDC_PARSE_ERROR` | USER_ERROR | Streaming ([10](../architecture/streaming.md)) | CDC message format error | ✗ | Check Debezium configuration and message format | — |
| 1402 | `CDC_OFFSET_ERROR` | USER_ERROR | Streaming ([10](../architecture/streaming.md)) | CDC offset commit failed | ✗ | Check Kafka connectivity, reset offset | — |
| 1501 | `CACHE_MISS` | USER_ERROR | Cache ([09](../architecture/caching.md)) | Cache miss (informational) | ✗ | Normal behavior, no recovery needed | — |
| 1502 | `CACHE_INVALIDATION_FAILED` | USER_ERROR | Cache ([09](../architecture/caching.md)) | Cache invalidation operation failed | ✗ | Check Redis connectivity | — |
| 1601 | `TENANT_NOT_FOUND` | USER_ERROR | Multi-tenancy ([13](../architecture/multi-tenancy.md)) | Tenant does not exist | ✗ | `SHOW TENANTS` to confirm the tenant name | — |
| 1602 | `TENANT_QUOTA_EXCEEDED` | USER_ERROR | Multi-tenancy ([13](../architecture/multi-tenancy.md)) | Tenant quota exceeded | ✗ | Wait for the quota to recover or contact the administrator to scale up | §9.4 |
| 1603 | `TENANT_ACCESS_DENIED` | USER_ERROR | Multi-tenancy ([13](../architecture/multi-tenancy.md)) | Tenant not authorized to access the Catalog | ✗ | Contact the administrator to configure allowed_catalogs | — |
| 2001 | `RESOURCE_EXHAUSTED` | RESOURCE_ERROR | Resource ([06](../architecture/resource-management.md)) | Insufficient cluster resources | ✓ | Wait for resources to free up or scale out Workers | §9.4 |
| 2002 | `MEMORY_EXCEEDED` | RESOURCE_ERROR | Resource ([06](../architecture/resource-management.md)) + Native ([03](../architecture/native-execution.md)) | Query memory limit exceeded | ✓ | Enable Spill or increase the memory quota | §9.4 |
| 2101 | `QUERY_TIMEOUT` | RESOURCE_ERROR | Coordinator (Scheduler) | Query execution timeout | ✓ | Optimize the query or increase the timeout | §9.4 |
| 2201 | `QUERY_CANCELLED` | RESOURCE_ERROR | Coordinator (QueryManager) | Query cancelled by user or system | ✓ | Check the cancellation reason | — |
| 3001 | `INTERNAL_ERROR` | INTERNAL_ERROR | Any module | Unexpected internal error | ✓ | Collect logs, file a bug | §9.2/§9.3 |
| 3101 | `NATIVE_ERROR` | INTERNAL_ERROR | Native ([03](../architecture/native-execution.md)) | Velox execution error | ✓ | Check Core Dump, file a bug | §9.3 |
| 3102 | `JNI_ERROR` | INTERNAL_ERROR | Worker (JNI Bridge) | JNI call failed | ✓ | Check Native library version compatibility | §9.3 |
| 4001 | `STORAGE_ERROR` | EXTERNAL_ERROR | Connector ([04](../architecture/connectors.md)) | S3/HDFS access failed | ✓ | Check storage service status and credentials | §9.5 |
| 4101 | `NETWORK_ERROR` | EXTERNAL_ERROR | Network layer | Network connection failed | ✓ | Check network connectivity | §9.5 |
| 4203 | `COORDINATOR_UNAVAILABLE` | EXTERNAL_ERROR | Coordinator ([01](../architecture/distributed.md)) | Coordinator unreachable | ✓ | Check the Coordinator process and ZK | §9.2 |
| 4204 | `WORKER_UNAVAILABLE` | EXTERNAL_ERROR | Worker ([01](../architecture/distributed.md)) | Worker unreachable | ✓ | Check the Worker process and ZK registration | §9.3 |
| 5001 | `UPSTREAM_TASK_FAILED` | EXCHANGE_ERROR | Exchange ([02](../architecture/query-execution.md)) | Upstream Task failure leaves downstream without data | ✓ | Automatic retry, check the upstream Task failure cause | §9.3 |
| 5101 | `EXCHANGE_CONNECTION_LOST` | EXCHANGE_ERROR | Exchange ([02](../architecture/query-execution.md)) | Shuffle connection dropped | ✓ | Automatic reconnect, check the Worker network | §9.3 |
| 5102 | `EXCHANGE_DATA_TIMEOUT` | EXCHANGE_ERROR | Exchange ([02](../architecture/query-execution.md)) | Shuffle data fetch timeout | ✓ | Check upstream Worker load and network | §9.3 |

**Native error code mapping** (supplementary): The Native layer's `NativeError.error_code` is mapped via `NativeErrorMapper` ([resilience.md §3.3](../architecture/resilience.md)) to the Java error codes in the table above:

| Native error_code range | Mapped PangrovaErrorCode | Recovery action |
|------------------------|----------------------|---------|
| 100-199 (MEMORY_ERROR) | `MEMORY_EXCEEDED` (2002) | Enable Spill, increase Native memory quota |
| 200-299 (IO_ERROR) | `STORAGE_ERROR` (4001) | Check S3/HDFS connectivity and credentials |
| 300-699 (COMPUTE_ERROR) | `INTERNAL_ERROR` (3001) | Collect Core Dump, file a bug |
| 700-799 (CANCELLED) | `QUERY_CANCELLED` (2201) | Normal cancellation, no recovery needed |

### 9.2 Coordinator Failure Recovery

> **Related error code**: `COORDINATOR_UNAVAILABLE` (4203) — see [resilience.md §2.2](../architecture/resilience.md)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Coordinator Failure Recovery SOP                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Symptom: Coordinator node is unresponsive or crashed                           │
│                                                                                  │
│  Diagnostic steps:                                                              │
│  1. Check process status: ps aux | grep pangrova-coordinator                      │
│  2. Check port listening: netstat -tlnp | grep 8080                             │
│  3. Check logs: tail -f /var/log/pangrova/coordinator.log                         │
│  4. Check ZK registration: zkCli.sh ls /pangrova/coordinators                     │
│                                                                                  │
│  Recovery steps:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: Confirm the other Coordinators are healthy                     │   │
│  │          curl http://coordinator-2:8080/health                          │   │
│  │                                                                          │   │
│  │  Step 2: Remove the failed node from the LB                             │   │
│  │          haproxy-cli disable server pangrova/coordinator-1                │   │
│  │                                                                          │   │
│  │  Step 3: Attempt to restart the service                                 │   │
│  │          systemctl restart pangrova-coordinator                           │   │
│  │                                                                          │   │
│  │  Step 4: Check startup logs, confirm no anomalies                       │   │
│  │          journalctl -u pangrova-coordinator -f                            │   │
│  │                                                                          │   │
│  │  Step 5: After the health check passes, add back to the LB              │   │
│  │          haproxy-cli enable server pangrova/coordinator-1                 │   │
│  │                                                                          │   │
│  │  Step 6: Verify queries work                                            │   │
│  │          pangrova-cli -e "SELECT 1"                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  If restart fails:                                                              │
│  • Check JVM OOM: dmesg | grep -i oom                                           │
│  • Check disk space: df -h                                                      │
│  • Check ZK connection: zkCli.sh stat                                           │
│  • Consider rolling back the version or replacing the node                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.3 Worker Failure Recovery

> **Related error codes**: `WORKER_UNAVAILABLE` (4204), `NATIVE_ERROR` (3101), `JNI_ERROR` (3102) — see [resilience.md §2.2](../architecture/resilience.md)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       Worker Failure Recovery SOP                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Symptom: Worker node unresponsive, task failures, Native layer crash           │
│                                                                                  │
│  Diagnostic steps:                                                              │
│  1. Check process status: ps aux | grep pangrova-worker                           │
│  2. Check Native crash: ls /var/log/pangrova/core.*                               │
│  3. Check memory usage: free -h && cat /proc/meminfo                            │
│  4. Check Spill directory: df -h /data/spill                                    │
│                                                                                  │
│  Recovery steps:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: Mark the Worker as DRAINING (if still responsive)              │   │
│  │          curl -X POST http://worker-1:8081/admin/drain                  │   │
│  │                                                                          │   │
│  │  Step 2: Wait for existing tasks to finish or time out (up to 5 minutes)│   │
│  │          watch "curl http://worker-1:8081/admin/tasks | jq '.count'"    │   │
│  │                                                                          │   │
│  │  Step 3: Stop the Worker service                                        │   │
│  │          systemctl stop pangrova-worker                                   │   │
│  │                                                                          │   │
│  │  Step 4: Clean up the Spill directory (if the disk is full)             │   │
│  │          rm -rf /data/spill/*                                           │   │
│  │                                                                          │   │
│  │  Step 5: Check Core Dump (if the Native layer crashed)                  │   │
│  │          gdb /opt/pangrova/lib/libpangrova_native.so /var/log/pangrova/core.* │   │
│  │                                                                          │   │
│  │  Step 6: Restart the Worker                                             │   │
│  │          systemctl start pangrova-worker                                  │   │
│  │                                                                          │   │
│  │  Step 7: Verify the Worker registered successfully                      │   │
│  │          curl http://coordinator:8080/admin/workers | jq                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Common causes of Native crashes:                                               │
│  • OOM: increase the memory limit or enable Spill                               │
│  • Segfault: check Native library version compatibility                         │
│  • Assertion failure: possibly a data issue, check the input data               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.4 Query Timeout/OOM Recovery

> **Related error codes**: `MEMORY_EXCEEDED` (2002), `QUERY_TIMEOUT` (2101), `RESOURCE_EXHAUSTED` (2001) — see [resilience.md §2.2](../architecture/resilience.md)
>
> Diagnostic tip: If the error code is `MEMORY_EXCEEDED`, check Native layer memory first (the `pangrova.native.memory.peak` metric), because a Native OOM is mapped to this error code via NativeErrorMapper (see [resilience.md §3.3](../architecture/resilience.md), Native error_code 100-199).

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Query Timeout/OOM Recovery SOP                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Symptom: query timeout, out-of-memory errors, Spill failure                    │
│                                                                                  │
│  Diagnostic steps:                                                              │
│  1. View query details: EXPLAIN ANALYZE <query>                                 │
│  2. Check resource usage: SELECT * FROM system.runtime.queries WHERE state='RUNNING'│
│  3. Check memory allocation: SELECT * FROM system.runtime.memory_pools          │
│                                                                                  │
│  Recovery steps:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: Cancel the problematic query                                   │   │
│  │          CALL system.cancel_query('query-id')                           │   │
│  │                                                                          │   │
│  │  Step 2: Analyze the query plan, identify bottlenecks                   │   │
│  │  • Full scan of a large table -> add filters or partition pruning       │   │
│  │  • Large-table Join -> consider Broadcast or add filters                │   │
│  │  • High-cardinality aggregation -> increase memory or enable Spill      │   │
│  │                                                                          │   │
│  │  Step 3: Optimize the query or adjust resources                         │   │
│  │          SET SESSION query_max_memory = '10GB';                         │   │
│  │          SET SESSION spill_enabled = true;                              │   │
│  │                                                                          │   │
│  │  Step 4: Re-run the query                                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Preventive measures:                                                           │
│  • Set a reasonable query timeout: query.max-execution-time = 30m               │
│  • Enable query complexity limits: query.max-scan-rows = 10000000000            │
│  • Configure resource queues to isolate large queries                           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.5 Storage Failure Recovery

> **Related error codes**: `STORAGE_ERROR` (4001), `NETWORK_ERROR` (4101) — see [resilience.md §2.2](../architecture/resilience.md)
>
> Diagnostic tip: If the error code is `STORAGE_ERROR` and originates from the Native layer (NativeError error_code 200-299, error_type=`IO_ERROR`), it indicates a read failure in the Native I/O layer; check S3/HDFS connectivity and credentials.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       Storage Failure Recovery SOP                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Symptom: S3/HDFS access failure, corrupted data files, metadata inconsistency  │
│                                                                                  │
│  S3 access failure:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Check S3 service status: aws s3 ls s3://bucket/                     │   │
│  │  2. Check credential validity: aws sts get-caller-identity              │   │
│  │  3. Check network connectivity: curl -I https://s3.amazonaws.com        │   │
│  │  4. Check throttling: view CloudWatch S3 metrics                        │   │
│  │  5. Temporary measure: increase retry count and timeout                 │   │
│  │     SET SESSION s3.max-retries = 10;                                    │   │
│  │     SET SESSION s3.connect-timeout = 30s;                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Corrupted data files:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Identify corrupted files: check file paths in the error log         │   │
│  │  2. Check file integrity: parquet-tools meta <file>                     │   │
│  │  3. Restore from backup or regenerate                                   │   │
│  │  4. For an Iceberg table, roll back to the previous snapshot:           │   │
│  │     ALTER TABLE t ROLLBACK TO SNAPSHOT <snapshot-id>                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Metadata inconsistency:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Refresh metadata cache: CALL system.sync_table_metadata('catalog.db.table')│   │
│  │  2. Check Iceberg metadata.json integrity                               │   │
│  │  3. Run orphan-file cleanup: VACUUM TABLE t RETAIN 168 HOURS DRY RUN    │   │
│  │  4. If necessary, restore from metadata history                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.6 Failure Recovery Checklist

```markdown
## Post-recovery checklist

### Basic checks
- [ ] All Coordinator nodes healthy
- [ ] All Worker nodes healthy
- [ ] ZooKeeper cluster healthy
- [ ] Metadata store (PostgreSQL) healthy

### Functional verification
- [ ] Simple query works: SELECT 1
- [ ] Table scan works: SELECT * FROM test_table LIMIT 10
- [ ] Join query works: SELECT * FROM t1 JOIN t2 ON ...
- [ ] Writes work: INSERT INTO test_table VALUES (...)

### Performance verification
- [ ] Query latency back to normal levels
- [ ] No abnormal queuing
- [ ] Resource utilization normal

### Monitoring confirmation
- [ ] Alerts cleared
- [ ] Metric curves normal
- [ ] No persistent errors in logs
```

---



## 10. Observability Integration

This section defines the metrics and tracing integration points for the operations layer. For the full observability design, see [observability.md](../architecture/observability.md).

### 10.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.ops.upgrade.in_progress` | RollingUpgradeManager | Rolling upgrade active flag |
| `pangrova.ops.scaling.events` | AutoScaler | Scale-up/down events |
| `pangrova.ops.scaling.workers.target` | AutoScaler | Target Worker count |
| `pangrova.ops.config.reload.count` | ConfigHotReloader | Config reload count |
| `pangrova.ops.config.reload.failures` | ConfigHotReloader | Config reload failures |
| `pangrova.ops.recovery.duration` | RecoveryManager | Failure recovery duration |

### 10.2 Tracing Spans

- `ops.rolling_upgrade` — Rolling upgrade lifecycle (SERVER span)
- `ops.config_reload` — Configuration hot reload (INTERNAL span)
- `ops.failover` — Coordinator failover (SERVER span)

### 10.3 Log Events

- Rolling upgrade phase transitions (INFO)
- Auto-scaling decisions with reason and target count (INFO)
- Config reload events with changed parameters (INFO)
- Disaster recovery activation events (WARN)
- SOP execution steps (INFO)

See [14 §7](../architecture/observability.md) for structured logging format.



> **Protocol Version Cross-Reference (§2.3)**: The protocol version management here covers the operational upgrade process (rolling upgrade compatibility). The Native layer protocol versioning in [03 §5.7](../architecture/native-execution.md) covers Protobuf wire format compatibility. Both systems use semantic versioning. The operational version gate (§2.3) checks Native protocol version compatibility before allowing a rolling upgrade to proceed.


> **Configuration status:** The rolling-upgrade, online-DDL, disaster-recovery,
> and configuration-management settings in this guide are operational design unless
> they also appear in the [configuration key reference](../configuration/reference.md).


> **Hot reload status:** §6.1 describes the target classification. The current
> typed loader does not expose the former per-key “Hot Reload” registry.


> **MetadataReplicationService (§4.2)**: The replication strategy is **synchronous by default** with **async degradation** under failure. Normal operation: synchronous replication with `replication.sync-timeout = 5s`. If sync replication times out, the system degrades to async mode and logs a warning. The `replication.mode` config (sync | async | sync-with-degradation) controls this behavior. Default: `sync-with-degradation`.
