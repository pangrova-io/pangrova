package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.pangrova.worker.resource.tenant.TenantResourceQuota;
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
 * <p>Each pre-registered task owns its slot and counters. Per-owner synchronization
 * covers only in-memory quota accounting, never execution or external I/O.</p>
 */
@ThreadSafe
public final class TenantExecutionQuotaEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger(TenantExecutionQuotaEnforcer.class);

    private final TenantResourceTracker resourceTracker;

    private final Map<String, TaskQuota> taskQuotas = new ConcurrentHashMap<>();

    public TenantExecutionQuotaEnforcer(TenantResourceTracker resourceTracker) {
        this.resourceTracker = resourceTracker;
    }

    /**
     * Atomically reserves and registers a task's tenant concurrency slot.
     *
     * @param taskId   the worker-assigned task identifier
     * @param tenantId the tenant owning this task (null means no enforcement)
     * @return whether this exact task owns a concurrency slot
     */
    public boolean tryReserveTask(String taskId, String tenantId) {
        return tryReserveTask(taskId, tenantId, 0);
    }

    /** Reserves a control operation's known JVM allowance together with its tenant slot. */
    public boolean tryReserveTask(String taskId, String tenantId, long memoryBytes) {
        java.util.Objects.requireNonNull(taskId, "taskId");
        if (memoryBytes < 0) throw new IllegalArgumentException("Tenant memory reservation must be nonnegative");
        if (tenantId == null || tenantId.isBlank()) {
            return true;
        }
        TaskQuota added = new TaskQuota(tenantId, memoryBytes);
        TaskQuota existing = taskQuotas.putIfAbsent(taskId, added);
        TaskQuota quota = existing == null ? added : existing;
        synchronized (quota) {
            if (!quota.tenantId.equals(tenantId)) {
                throw new IllegalArgumentException("Task quota tenant cannot change: " + taskId);
            }
            if (quota.memoryBytes != memoryBytes) {
                throw new IllegalArgumentException("Task quota memory reservation cannot change: " + taskId);
            }
            if (quota.released) return false;
            if (quota.acquired) return true;
            try {
                quota.acquired = resourceTracker.tryAcquireQuery(tenantId);
                if (quota.acquired && !resourceTracker.tryAllocateMemory(tenantId, memoryBytes)) {
                    resourceTracker.releaseQuery(tenantId);
                    quota.acquired = false;
                }
                if (!quota.acquired) taskQuotas.remove(taskId, quota);
                return quota.acquired;
            } catch (RuntimeException | Error failure) {
                taskQuotas.remove(taskId, quota);
                throw failure;
            }
        }
    }

    /**
     * Records CPU time consumed by a task and checks against tenant quota.
     *
     * @param taskId         the task identifier
     * @param cpuTimeDeltaMs incremental CPU time in milliseconds
     */
    public void recordCpuTime(String taskId, long cpuTimeDeltaMs) {
        TaskQuota quota = taskQuotas.get(taskId);
        if (quota != null && cpuTimeDeltaMs > 0) quota.cpuTime.addAndGet(cpuTimeDeltaMs);
    }

    /**
     * Records scan bytes consumed by a task.
     *
     * @param taskId         the task identifier
     * @param scanBytesDelta incremental scan bytes
     */
    public void recordScanBytes(String taskId, long scanBytesDelta) {
        TaskQuota quota = taskQuotas.get(taskId);
        if (quota != null && scanBytesDelta > 0) quota.scanBytes.addAndGet(scanBytesDelta);
    }

    /**
     * Checks whether the tenant owning this task has exceeded any runtime quota.
     * Called on the task execution hot path during completion monitor polling.
     *
     * @param taskId the task identifier
     * @return true if the task should be rejected/cancelled due to quota violation
     */
    public boolean isQuotaExceeded(String taskId) {
        TaskQuota quota = taskQuotas.get(taskId);
        return quota != null && resourceTracker.isOverMemoryQuota(quota.tenantId);
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
        TaskQuota tracked = taskQuotas.get(taskId);
        if (tracked == null) return;
        String tenantId = tracked.tenantId;
        TenantResourceQuota quota = resourceTracker.getQuota(tenantId);

        AtomicLong cpuCounter = tracked.cpuTime;
        if (cpuCounter != null && quota.hasCpuTimeLimit()) {
            long cpuTimeMs = cpuCounter.get();
            if (cpuTimeMs > quota.maxCpuTimePerQueryMs()) {
                LOG.warn("event=quota_cpu_time_exceeded taskId={} tenantId={} cpuTimeMs={} limitMs={}",
                        taskId, tenantId, cpuTimeMs, quota.maxCpuTimePerQueryMs());
                throw new TenantQuotaViolation(tenantId, "CPU_TIME",
                        "CPU time exceeded: " + cpuTimeMs + "ms > " + quota.maxCpuTimePerQueryMs() + "ms");
            }
        }

        AtomicLong scanCounter = tracked.scanBytes;
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
     * Releases a query slot when a task completes or is cancelled.
     *
     * @param taskId the task identifier
     */
    public void releaseTask(String taskId) {
        TaskQuota quota = taskQuotas.get(taskId);
        if (quota == null) return;
        synchronized (quota) {
            if (quota.acquired && !quota.released) {
                resourceTracker.releaseMemory(quota.tenantId, quota.memoryBytes);
                resourceTracker.releaseQuery(quota.tenantId);
            }
            quota.released = true;
            taskQuotas.remove(taskId, quota);
        }
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

    private static final class TaskQuota {
        private final String tenantId;
        private final long memoryBytes;
        private final AtomicLong cpuTime = new AtomicLong();
        private final AtomicLong scanBytes = new AtomicLong();
        private boolean acquired;
        private boolean released;

        private TaskQuota(String tenantId, long memoryBytes) {
            this.tenantId = tenantId;
            this.memoryBytes = memoryBytes;
        }
    }

    /**
     * Exception indicating a tenant quota violation during execution.
     */
    public static final class TenantQuotaViolation extends RuntimeException {
        private static final long serialVersionUID = 1L;
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
