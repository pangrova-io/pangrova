package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.resource.tenant.TenantResourceTracker;
import org.pangrova.resource.tenant.TenantResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces per-tenant resource quotas during task execution.
 *
 * <p>Called on the task execution hot path (completion monitor polling loop)
 * to check whether a tenant has exceeded its memory or CPU time budget.
 * When a quota is exceeded, the task is cancelled.</p>
 *
 * <p>Concurrency strategy: all mutable state uses atomic types with CAS,
 * ensuring thread-safe accounting without locks.</p>
 */
@ThreadSafe
public final class TenantExecutionQuotaEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger(TenantExecutionQuotaEnforcer.class);

    private final TenantResourceTracker resourceTracker;

    /** Per-task CPU time tracking: taskId → accumulated CPU time in milliseconds. */
    private final Map<String, AtomicLong> taskCpuTime = new ConcurrentHashMap<>();

    /** Per-task scan bytes tracking: taskId → accumulated scan bytes. */
    private final Map<String, AtomicLong> taskScanBytes = new ConcurrentHashMap<>();

    /** Per-task tenant mapping: taskId → tenantId. */
    private final Map<String, String> taskTenantMap = new ConcurrentHashMap<>();

    public TenantExecutionQuotaEnforcer(TenantResourceTracker resourceTracker) {
        this.resourceTracker = resourceTracker;
    }

    /**
     * Registers a task for quota tracking.
     *
     * @param taskId   the worker-assigned task identifier
     * @param tenantId the tenant owning this task (null means no enforcement)
     */
    public void registerTask(String taskId, String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return;
        }
        taskTenantMap.put(taskId, tenantId);
        taskCpuTime.put(taskId, new AtomicLong(0));
        taskScanBytes.put(taskId, new AtomicLong(0));
    }

    /**
     * Records CPU time consumed by a task and checks against tenant quota.
     *
     * @param taskId         the task identifier
     * @param cpuTimeDeltaMs incremental CPU time in milliseconds
     */
    public void recordCpuTime(String taskId, long cpuTimeDeltaMs) {
        String tenantId = taskTenantMap.get(taskId);
        if (tenantId == null) {
            return;
        }
        AtomicLong counter = taskCpuTime.get(taskId);
        if (counter != null && cpuTimeDeltaMs > 0) {
            counter.addAndGet(cpuTimeDeltaMs);
        }
    }

    /**
     * Records scan bytes consumed by a task.
     *
     * @param taskId         the task identifier
     * @param scanBytesDelta incremental scan bytes
     */
    public void recordScanBytes(String taskId, long scanBytesDelta) {
        String tenantId = taskTenantMap.get(taskId);
        if (tenantId == null) {
            return;
        }
        AtomicLong counter = taskScanBytes.get(taskId);
        if (counter != null && scanBytesDelta > 0) {
            counter.addAndGet(scanBytesDelta);
        }
    }

    /**
     * Checks whether the tenant owning this task has exceeded any runtime quota.
     * Called on the task execution hot path during completion monitor polling.
     *
     * @param taskId the task identifier
     * @return true if the task should be rejected/cancelled due to quota violation
     */
    public boolean isQuotaExceeded(String taskId) {
        String tenantId = taskTenantMap.get(taskId);
        if (tenantId == null) {
            return false;
        }
        return resourceTracker.isOverMemoryQuota(tenantId);
    }

    /**
     * Runtime quota enforcement: checks CPU time and scan bytes against tenant limits.
     * Mirrors the semantics of {@code QuotaChecker.checkDuringExecution} in pangrova-security,
     * operating within the worker module's dependency boundary using
     * {@link TenantResourceTracker} as the quota source.
     *
     * <p>Called at task start and on each completion monitor poll cycle (stage boundary).
     * Throws {@link TenantQuotaViolation} if any runtime quota is exceeded.</p>
     *
     * @param taskId the task identifier
     * @throws TenantQuotaViolation if CPU time or scan bytes exceed tenant quota
     */
    public void checkDuringExecution(String taskId) {
        String tenantId = taskTenantMap.get(taskId);
        if (tenantId == null) {
            return;
        }
        TenantResourceQuota quota = resourceTracker.getQuota(tenantId);

        AtomicLong cpuCounter = taskCpuTime.get(taskId);
        if (cpuCounter != null && quota.hasCpuTimeLimit()) {
            long cpuTimeMs = cpuCounter.get();
            if (cpuTimeMs > quota.maxCpuTimePerQueryMs()) {
                LOG.warn("event=quota_cpu_time_exceeded taskId={} tenantId={} cpuTimeMs={} limitMs={}",
                        taskId, tenantId, cpuTimeMs, quota.maxCpuTimePerQueryMs());
                throw new TenantQuotaViolation(tenantId, "CPU_TIME",
                        "CPU time exceeded: " + cpuTimeMs + "ms > " + quota.maxCpuTimePerQueryMs() + "ms");
            }
        }

        AtomicLong scanCounter = taskScanBytes.get(taskId);
        if (scanCounter != null && quota.hasScanBytesLimit()) {
            long scanBytes = scanCounter.get();
            if (scanBytes > quota.maxScanBytesPerQuery()) {
                LOG.warn("event=quota_scan_bytes_exceeded taskId={} tenantId={} scanBytes={} limitBytes={}",
                        taskId, tenantId, scanBytes, quota.maxScanBytesPerQuery());
                throw new TenantQuotaViolation(tenantId, "SCAN_BYTES",
                        "Scan bytes exceeded: " + scanBytes + " > " + quota.maxScanBytesPerQuery());
            }
        }
    }

    /**
     * Checks whether a new task can be admitted for the given tenant.
     * Verifies both memory and concurrency quotas.
     *
     * @param tenantId the tenant identifier
     * @return true if the task is admitted, false if quota is exceeded
     */
    public boolean canAdmitTask(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return true;
        }
        return resourceTracker.tryAcquireQuery(tenantId);
    }

    /**
     * Releases a query slot when a task completes or is cancelled.
     *
     * @param taskId the task identifier
     */
    public void releaseTask(String taskId) {
        String tenantId = taskTenantMap.remove(taskId);
        if (tenantId != null) {
            resourceTracker.releaseQuery(tenantId);
        }
        taskCpuTime.remove(taskId);
        taskScanBytes.remove(taskId);
    }

    /**
     * Returns the current memory usage for a tenant.
     */
    public long getTenantMemoryUsage(String tenantId) {
        return resourceTracker.getMemoryUsage(tenantId);
    }

    /**
     * Returns the current active query count for a tenant.
     */
    public int getTenantActiveQueryCount(String tenantId) {
        return resourceTracker.getActiveQueryCount(tenantId);
    }

    /**
     * Returns the quota for a tenant.
     */
    public TenantResourceQuota getTenantQuota(String tenantId) {
        return resourceTracker.getQuota(tenantId);
    }

    /**
     * Exception indicating a tenant quota violation during execution.
     */
    public static final class TenantQuotaViolation extends RuntimeException {
        private final String tenantId;
        private final String violationType;

        public TenantQuotaViolation(String tenantId, String violationType, String message) {
            super(message);
            this.tenantId = tenantId;
            this.violationType = violationType;
        }

        public String tenantId() { return tenantId; }
        public String violationType() { return violationType; }
    }
}
