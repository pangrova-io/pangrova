---
title: "Day-2 Operations"
description: "Rolling upgrade, online DDL, disaster recovery, autoscaling, and operational procedures"
sidebar_label: "Day-2 Operations"
---

# Pangrova — Day-2 Operations

> **Summary.** Epoch-fenced upgrades, online DDL, disaster recovery,
> autoscaling, and failure-recovery SOPs.

## Runtime coverage

| Capability | Entry and authority in this distribution |
| --- | --- |
| Recovery | The Coordinator constructs `RecoveryPlaybook` when `operations.enabled` is true. Explicit incidents with no installed handler are counted as escalations. Worker loss, timeout and OOM have no installed automatic remediation. |
| Scaling | `AutoScalingController` implements scaling decisions for an injected worker-count source, executor and shuffle residency policy. Unknown residency keeps the current worker pool on scale-in. `KubernetesScalingExecutor` requires a host-supplied deployment client. The server has no Kubernetes client, autoscaling loop or predictive model wired. |
| Upgrade | The distribution ships `bin/pangrova-rolling-upgrade.sh`. The experimental Java controller requires a caller-supplied `NodeOperations` implementation. Verification must succeed before a node is reported upgraded. |
| Asynchronous DDL | `AsyncDdlExecutor` requires a job store with atomic state transitions and a callback. There is no shipped production job store or SQL dispatcher for this executor. Cancelling fences completion and skips queued work. A callback already executing can finish its external side effects. |
| JVM diagnostics | `JvmDiagnostics` uses the JDK for thread dumps, heap dumps and Flight Recorder. Trusted hosts supply caller identity for heap dumps and close the diagnostic service to release active recordings and the scavenger. The distribution does not register a diagnostics HTTP endpoint. |
| DR and config | The replication target, failover executor and hot-reload callbacks require host integration. The running Coordinator does not wire them from the examples below. |

The upgrade version parser and version ranges belong to the Java upgrade checker.
They have no external callers and use that checker's version comparison rules.
Shuffle drain decisions consume residency evidence, and JVM diagnostics own JDK
recordings and heap files. These classes stay in their capability packages.
Storage callbacks, node operations, deployment scaling and replication each have
an explicit contract. This distribution has a single Kubernetes scaling delegate
and no production host for it. There is no second installed backend requiring a
factory or provider artifact in Operations.

The examples of APIs, SQL syntax and infrastructure workflows below describe the design where the table identifies an outstanding integration. Connector maintenance procedures and durable Query recovery have separate execution paths.

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
│  │  Epoch Upgrade      │ Blue/green traffic cutover  │ Drain+fence+verify │   │
│  │  Online DDL         │ Non-blocking schema change │ Async exec+progress │   │
│  │  Disaster Recovery  │ Cross-region DR             │ Metadata+data repl. │  │
│  │  Auto Scaling       │ Autoscaling                 │ Load-based elastic. │   │
│  │  Config Hot Reload  │ Config hot reload           │ Dynamic config ctr. │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Protocol-Epoch Upgrade

### 2.1 Upgrade Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       Protocol-Epoch Upgrade Architecture                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Upgrade order:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Create a fenced green Worker pool with the new execution epoch       │   │
│  │     Descriptor + Native library + capability inventory must all match    │   │
│  │                                                                          │   │
│  │  2. Validate green, stop admission to blue, and drain/fence blue          │   │
│  │     No query or background job may span execution epochs                 │   │
│  │                                                                          │   │
│  │  3. Atomically switch admission to green; keep blue fenced for rollback   │   │
│  │     Remove blue only after query/job and publication-fence verification  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Epoch requirements:                                                            │
│  • One admitted descriptor/capability epoch per cluster                          │
│  • Mismatched Workers remain fenced and receive no tasks                         │
│  • No negotiation, v1 fallback, dual parser, or per-task downgrade                │
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
     * 4. Partial Upgrade Failure Strategy:
     *    Keep the green pool fenced, continue serving on the homogeneous blue
     *    epoch, and roll green back or replace it. A mixed execution epoch is
     *    never admitted and there is no rollback-by-protocol-downgrade.
     */
}


```

### 2.3 Epoch Admission

```java
/**
 * Exact execution-epoch admission checker
 */
public class VersionCompatibilityChecker {

    /**
     * Check descriptor, Native library and capability identity before admission.
     */
    public CompatibilityResult check(String currentVersion, String targetVersion) {
        ExecutionEpoch admitted = readAdmittedEpoch();
        ExecutionEpoch candidate = inspectCandidate(targetVersion);
        if (!candidate.protocolDescriptorSha256()
                .equals(admitted.protocolDescriptorSha256())
                || !candidate.nativeLibrarySha256()
                        .equals(admitted.nativeLibrarySha256())
                || !candidate.capabilityEpoch().equals(admitted.capabilityEpoch())) {
            return CompatibilityResult.incompatible(
                    "Worker execution epoch does not match cluster admission");
        }
        return CompatibilityResult.compatible();
    }

    /**
     * Changing the execution epoch updates the canonical v2 proto source,
     * Java and C++ generated descriptors, packaged Native library, capability
     * inventory and admission record atomically. REST API versioning is a
     * separate client contract and never selects a JNI execution protocol.
     */
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

> **Status: experimental/target.** `MetadataReplicationService` is not wired into a
> production coordinator lifecycle; the current implementation is asynchronous with
> a retry queue and no production `ReplicationTarget`. `replication.mode` and
> `replication.sync-timeout` are not current config keys.

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

> The default deployment uses PostgreSQL `distributed_lease` and a JDBC Worker
> registry; ZooKeeper and Redis are optional backends. The rows below describe
> those optional deployments, not prerequisites.

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

### 7.1 Operations configuration

Rolling upgrade, online DDL, disaster recovery, auto-scaling, and ZooKeeper-backed
config hot reload are not implemented as typed runtime configuration. Do not add
`rolling-upgrade`, `online-ddl`, `disaster-recovery`, `auto-scaling`, or
`config.source` blocks to `coordinator.yml` or `worker.yml`. Supported keys are the
typed options in [Configuration](../configuration/index.md) and the bundled
`etc/coordinator.yml` / `etc/worker.yml`. The supported operations interface is the
shipped `bin/pangrova-*.sh` scripts and the runbooks in this section.

### 7.2 Operations command summary

No `system.*` SQL procedures for rolling upgrade, online DDL, disaster recovery,
auto-scaling, or cache maintenance are registered. Use the shipped operations
scripts instead:

```bash
bin/pangrova-rolling-upgrade.sh --help
bin/pangrova-cluster-status.sh
bin/pangrova-health-check.sh --help
bin/pangrova-stop.sh --help
```

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
│  │  C1. WorkerCatalogRuntime.discover() → Load role connector plugins     │   │
│  │  C2. nativeBridge.loadLibrary()      → Load the Native library          │   │
│  │  C3. nativeBridge.initializeVelox()  → Velox engine initialization        │   │
│  │  C4. memoryManager.initialize()      → Memory pool initialization         │   │
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
| Coordinator | `GET /v1/health/live` | HTTP port ready | LB liveness check |
| Coordinator | `GET /v1/health` | HTTP ready + ≥1 Worker ACTIVE + ≥1 Catalog ACTIVE | LB readiness check |
| Worker | `GET /v1/health/live` | HTTP port ready | Liveness check |
| Worker | `GET /v1/health` | HTTP + Flight ready + Native engine initialized | Coordinator readiness check |

### 8.4 Startup Failure Handling

| Failure scenario | Impact | Handling strategy |
|---------|------|---------|
| ZK unavailable | Coordinator/Worker cannot start | fail-fast, process exits, restart after ZK recovers |
| PostgreSQL unavailable | Coordinator cannot load Catalog | fail-fast, process exits |
| Redis unavailable | Metadata cache unavailable | degraded start, skip cache warm-up, log WARN |
| Native library load failure | Worker cannot start | fail-fast, process exits, check the packaged Native library and startup log |
| Connector plugin load failure | Corresponding Catalog unavailable | degraded start, mark Catalog as DEGRADED, other Catalogs work |
| No Worker registered | System cannot execute queries | Coordinator starts normally but `/v1/health` returns 503 |

### 8.5 Startup scripts

Use the shipped scripts rather than ad-hoc `java -jar` commands:

```bash
bin/pangrova-coordinator.sh --daemon
bin/pangrova-worker.sh --daemon
bin/pangrova-health-check.sh <host>:8080
bin/pangrova-cluster-status.sh
curl -s -X POST http://<coordinator>:8080/v1/query/submit \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT 1","session_properties":{}}'
```

Coordinator and Worker scripts accept `--config <path>` and `--daemon`. There is no
`bin/start-cluster.sh`, `check-zookeeper.sh`, `check-postgresql.sh`,
`check-redis.sh`, or `wait-for-health.sh`; check infrastructure readiness with the
actual database, ZooKeeper, and object-store clients.

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
│  │  TRANSPORT-0001           │ Coordinator     │ §9.2                   │   │
│  │  TRANSPORT_CONNECTION_FAILED│                │                        │   │
│  │  WORKER-0002              │ Worker          │ §9.3                   │   │
│  │  WORKER_LOST              │                 │                        │   │
│  │  NATIVE-0001..0007        │ Worker/Native   │ §9.3 (Native crash)    │   │
│  │  NATIVE-0001              │ Worker/Native   │ §9.3 (Native crash)    │   │
│  │  RESOURCE-0001            │ Query/Resource  │ §9.4                   │   │
│  │  RUNTIME-0003             │ Query/Resource  │ §9.4                   │   │
│  │  RUNTIME-0004             │ Query/Resource  │ §9.4                   │   │
│  │  FILE-0005               │ Storage         │ §9.5                   │   │
│  │  TRANSPORT-0001           │ Network/Storage │ §9.5                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.1.1 Error-code routing

`PangrovaErrorCode` values are strings in `CATEGORY-NNNN` form. The REST body is
`ErrorDetail` with `error_code`, `error_type`, and `message`; `error_type` is
`USER_ERROR` for 4xx responses and `INTERNAL_ERROR` otherwise. Use the code string,
not a numeric range.

| Code | Name | Domain | Typical trigger | Recovery | SOP |
|---|---|---|---|---|---|
| `PARSER-0001` | `PARSER_SYNTAX_ERROR` | Coordinator (parser) | SQL syntax error | Fix the SQL statement | — |
| `PLANNER-0001` | `PLANNER_VALIDATION_FAILURE` | Coordinator (planner) | Query validation failure | Fix the query or metadata | — |
| `METADATA-0008` | `METADATA_TABLE_NOT_FOUND` | Coordinator (metadata) | Table missing or misspelled | `SHOW TABLES` | — |
| `SECURITY-*` | Security codes | Security | RBAC, RLS, or masking failure | Check grants and policies | — |
| `RESOURCE-0001` | `RESOURCE_MEMORY_EXCEEDED` | Query/resource + Native | Memory quota exceeded | Enable Spill or raise the quota | §9.4 |
| `RESOURCE-0002` | `RESOURCE_SPILL_FAILED` | Query/resource | Spill write failed | Check disk capacity and spill path | §9.4 |
| `RUNTIME-0003` | `RUNTIME_QUERY_TIMEOUT` | Coordinator/scheduler | Query execution timeout | Optimize the query or raise the timeout | §9.4 |
| `RUNTIME-0004` | `RUNTIME_RESOURCE_EXHAUSTED` | Coordinator/resource | Resource exhausted | Scale out or wait for capacity | §9.4 |
| `COORDINATOR-0001` | `COORDINATOR_NO_AVAILABLE_WORKER` | Coordinator | No Worker for dispatch | Check Worker registration and capacity | §9.3 |
| `COORDINATOR-0007` | `COORDINATOR_MEMORY_EXCEEDED` | Coordinator | Coordinator row allocation limit | Reduce split/row materialization | §9.4 |
| `WORKER-0002` | `WORKER_LOST` | Worker/coordination | Worker died while executing tasks | Check the Worker process and registration | §9.3 |
| `NATIVE-0001..0007` | `NATIVE_*` | Worker/Native | JNI, execution, library, or plan failure | Check the Native library and logs | §9.3 |
| `EXCHANGE-0001..0004` | `EXCHANGE_*` | Exchange | Upstream/shuffle unavailable or corrupt | Check the upstream Worker and network | §9.3 |
| `TRANSPORT-0001..0003` | `TRANSPORT_*` | Transport | Connection, stream, or result-size failure | Check network and result limits | §9.5 |
| `FILE-0005` / `FILE-0006` | `FILE_STORAGE_*` | File plane | Object storage unavailable or not configured | Check storage configuration and credentials | §9.5 |
| `OPS-0001..0009` | `OPS_*` | Operations | Upgrade, failover, scaling, or config reload failure | Follow the matching runbook | §9.2 |

Native errors are not mapped by numeric buckets. A negative JNI return code raises
`NativeExecutionException` with `NATIVE-0001` and retains the raw native code;
canonical task reports carry `ExecutionError` with `error_class`, `code`, `message`,
`retryable`, `failed_component`, and `failed_partition_ids`. See
[resilience.md §3.3](../architecture/resilience.md).

### 9.2 Coordinator Failure Recovery

> **Related error code**: `TRANSPORT-0001` (`TRANSPORT_CONNECTION_FAILED`) — see [resilience.md §2.2](../architecture/resilience.md)

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
│  │          curl http://coordinator-2:8080/v1/health                          │   │
│  │                                                                          │   │
│  │  Step 2: Remove the failed node from the LB                             │   │
│  │          haproxy-cli disable server pangrova/coordinator-1                │   │
│  │                                                                          │   │
│  │  Step 3: Attempt to restart the service                                 │   │
│  │          /opt/pangrova/bin/pangrova-stop.sh --coordinator && /opt/pangrova/bin/pangrova-coordinator.sh --daemon                           │   │
│  │                                                                          │   │
│  │  Step 4: Check startup logs, confirm no anomalies                       │   │
│  │          tail -f /var/log/pangrova/coordinator.log                            │   │
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

> **Related error codes**: `WORKER-0002` (`WORKER_LOST`), `NATIVE-0004` (`NATIVE_INIT_FAILED`), `NATIVE-0001` (`NATIVE_JNI_EXCEPTION`) — see [resilience.md §2.2](../architecture/resilience.md)

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
│  │          ssh worker-1 /opt/pangrova/bin/pangrova-stop.sh --worker                  │   │
│  │                                                                          │   │
│  │  Step 2: Wait for existing tasks to finish or time out (up to 5 minutes)│   │
│  │          watch "bin/pangrova-cluster-status.sh"    │   │
│  │                                                                          │   │
│  │  Step 3: Stop the Worker service                                        │   │
│  │          /opt/pangrova/bin/pangrova-stop.sh --worker                                   │   │
│  │                                                                          │   │
│  │  Step 4: Clean up the Spill directory (if the disk is full)             │   │
│  │          rm -rf /data/spill/*                                           │   │
│  │                                                                          │   │
│  │  Step 5: Check Core Dump (if the Native layer crashed)                  │   │
│  │          gdb /opt/pangrova/lib/libpangrova_native.so /var/log/pangrova/core.* │   │
│  │                                                                          │   │
│  │  Step 6: Restart the Worker                                             │   │
│  │          /opt/pangrova/bin/pangrova-worker.sh --daemon                                  │   │
│  │                                                                          │   │
│  │  Step 7: Verify the Worker registered successfully                      │   │
│  │          curl http://coordinator:8080/api/workers | jq                │   │
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

> **Related error codes**: `RESOURCE-0001` (`RESOURCE_MEMORY_EXCEEDED`), `RUNTIME-0003` (`RUNTIME_QUERY_TIMEOUT`), `RUNTIME-0004` (`RUNTIME_RESOURCE_EXHAUSTED`) — see [resilience.md §2.2](../architecture/resilience.md)
>
> Diagnostic tip: If the error code is `RESOURCE-0001`, check Native layer memory first (the `pangrova.native.memory_used_bytes` metric), because a Native OOM is mapped to this error code by the Native-to-Java error path documented in [resilience.md §3.3](../architecture/resilience.md).

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

> **Related error codes**: `FILE-0005` (`FILE_STORAGE_UNAVAILABLE`), `TRANSPORT-0001` (`TRANSPORT_CONNECTION_FAILED`) — see [resilience.md §2.2](../architecture/resilience.md)
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



> **Execution epoch cross-reference (§2.3):** The operational gate and
> [Native protocol contract](../architecture/native-execution.md) enforce the
> same descriptor, Native-library and capability epoch. Blue and green pools may
> coexist while fenced, but only one homogeneous epoch receives work.


> **Configuration status:** The rolling-upgrade, online-DDL, disaster-recovery,
> and configuration-management settings in this guide are operational design unless
> they also appear in the [configuration key reference](../configuration/reference.md).


> **Hot reload status:** §6.1 describes the target classification. The current
> typed loader does not expose the former per-key “Hot Reload” registry.


> **MetadataReplicationService (§4.2)**: The replication strategy is **synchronous by default** with **async degradation** under failure. Normal operation: synchronous replication with `replication.sync-timeout = 5s`. If sync replication times out, the system degrades to async mode and logs a warning. The `replication.mode` config (sync | async | sync-with-degradation) controls this behavior. Default: `sync-with-degradation`.

## Conditional Iceberg branch TTL cleanup

Tables with `branch.ttl` use conditional branch removal against the observed
snapshot. `main` and tags are never cleanup candidates. A conflict or unknown
commit result defers work until a later fresh scan; it does not replay an
unconditional drop or continue acting on the stale ref listing.

PostgreSQL-backed SDK `JdbcCatalog` instances support this operation through an
atomic SQL comparison of the complete old metadata location. This also protects
against a branch being replaced by a same-name tag at the same snapshot after
the TTL check. Data-file location does not determine catalog commit safety:
JDBC-backed tables are not rejected merely because their table root uses S3.

Configure the Iceberg catalog with `type=jdbc`, a `jdbc:postgresql:` `uri`,
`warehouse`, `jdbc.user`, and `jdbc.password` (credentials supplied through the
deployment's secret handling). The coordinator assembly includes the PostgreSQL
runtime driver; the connector declares it as an optional runtime dependency for
other assemblies. The ordinary connector factory uses SDK `CatalogUtil` wiring.

The currently verified non-JDBC path remains Hadoop catalog metadata on local
files with atomic version publication. REST, Hive, HDFS and Hadoop-catalog object
storage are not enabled by this change and fail explicitly. Supporting them
requires backend-specific atomicity/conditional-commit validation, not adding
URI schemes to an allowlist or falling back to `removeBranch().commit()`.
