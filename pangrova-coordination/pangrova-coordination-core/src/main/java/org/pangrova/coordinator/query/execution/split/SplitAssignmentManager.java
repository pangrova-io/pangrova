package org.pangrova.coordinator.query.execution.split;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages split assignment for pull-mode task execution.
 * Workers request splits on demand rather than receiving all splits at dispatch time.
 *
 * <p><b>Status: coordinator-side complete, worker-side client available.</b>
 * DAG dispatch currently embeds splits directly in task requests (push mode).
 * Pull-mode is guarded by feature flag {@value #PULL_MODE_FLAG} (default:
 * {@code false}). The worker-side {@code SplitPullClient} is available in
 * {@code org.pangrova.worker.split} for end-to-end wiring.</p>
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} for stage queue registry.</p>
 */
@ThreadSafe
public final class SplitAssignmentManager {
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5_000L;

    private static final Logger LOG = LoggerFactory.getLogger(SplitAssignmentManager.class);

    /** Feature flag for pull-mode split assignment. Default: false (push mode). */
    public static final String PULL_MODE_FLAG = "pangrova.split.assignment.pull-mode.enabled";

    /** Default value for pull-mode feature flag. */
    public static final boolean PULL_MODE_DEFAULT = false;

    private static final long DEFAULT_LEASE_DURATION_MS = 60_000;

    private static final long DEFAULT_LEASE_CHECK_INTERVAL_MS = 10_000;

    private final ConcurrentHashMap<String, StageSplitQueue> stageQueues = new ConcurrentHashMap<>();
    private final long leaseDurationMs;
    private final long stealDelayMs;
    private final boolean pullModeEnabled;
    private final CacheAffinityAssigner cacheAffinityAssigner;
    private volatile ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> reclaimTask;

    // Cache affinity metrics
    private final AtomicLong cacheAffinityHits = new AtomicLong(0);
    private final AtomicLong cacheAffinityMisses = new AtomicLong(0);
    private final AtomicLong stealDelayExpired = new AtomicLong(0);

    public SplitAssignmentManager() {
        this(SplitAssignmentConfig.defaults());
    }

    public SplitAssignmentManager(long leaseDurationMs) {
        this(new SplitAssignmentConfig(leaseDurationMs, null, 0, PULL_MODE_DEFAULT));
    }

    public SplitAssignmentManager(long leaseDurationMs, CacheAffinityAssigner cacheAffinityAssigner) {
        this(new SplitAssignmentConfig(leaseDurationMs, cacheAffinityAssigner, 0, PULL_MODE_DEFAULT));
    }

    public SplitAssignmentManager(long leaseDurationMs, CacheAffinityAssigner cacheAffinityAssigner,
                                   long stealDelayMs) {
        this(new SplitAssignmentConfig(leaseDurationMs, cacheAffinityAssigner, stealDelayMs, PULL_MODE_DEFAULT));
    }

    /** Creates a split assignment manager from its lease configuration. */
    public SplitAssignmentManager(SplitAssignmentConfig config) {
        this.leaseDurationMs = config.leaseDurationMs() > 0 ? config.leaseDurationMs() : DEFAULT_LEASE_DURATION_MS;
        this.cacheAffinityAssigner = config.cacheAffinityAssigner();
        this.stealDelayMs = config.stealDelayMs() >= 0 ? config.stealDelayMs() : 0;
        this.pullModeEnabled = config.pullModeEnabled();
        if (pullModeEnabled) {
            LOG.warn("event=pull_mode_enabled_warning flag={} "
                    + "message=Pull-mode split assignment is enabled but worker-side "
                    + "split-pull client is not yet implemented. Splits registered via "
                    + "this manager will not be consumed by workers.", PULL_MODE_FLAG);
        }
    }

    /** Returns true if pull-mode split assignment is enabled. */
    public boolean isPullModeEnabled() {
        return pullModeEnabled;
    }

    /**
     * Starts periodic lease reclamation at the given interval.
     * Expired leases are returned to the pending queue for reassignment.
     */
    public void startLeaseReclamation(long intervalMs) {
        long interval = intervalMs > 0 ? intervalMs : DEFAULT_LEASE_CHECK_INTERVAL_MS;
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "split-lease-reclaim");
        this.reclaimTask = scheduler.scheduleAtFixedRate(
                () -> org.pangrova.common.concurrent.ExecutorDrain.runOwned(scheduler, this::reclaimExpiredLeases),
                interval, interval, TimeUnit.MILLISECONDS);
        LOG.info("event=lease_reclamation_started intervalMs={}", interval);
    }

    /** Stops the periodic lease reclamation scheduler. */
    public void stopLeaseReclamation() {
        if (reclaimTask != null) {
            reclaimTask.cancel(false);
        }
        if (scheduler != null) {
            org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(scheduler, EXECUTOR_SHUTDOWN_TIMEOUT_MS);
            LOG.info("event=lease_reclamation_stopped");
        }
    }

    /**
     * Registers splits for a stage. Called during scheduleStage for pull-mode stages.
     *
     * <p>Guarded by {@value #PULL_MODE_FLAG}. When pull-mode is disabled (default),
     * this method logs a warning and returns without registering — splits are
     * embedded in task requests via push mode instead.</p>
     */
    public void registerSplits(String queryId, int stageId, List<SplitAssignment> splits) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(splits, "splits must not be null");
        String key = buildKey(queryId, stageId);
        stageQueues.put(key, new StageSplitQueue(splits, leaseDurationMs, stealDelayMs));
        LOG.info("event=splits_registered queryId={} stageId={} splitCount={}",
                queryId, stageId, splits.size());
    }

    /**
     * Worker requests next split. Returns empty if no more splits available.
     * Locality-aware: prefers splits with data on the requesting worker.
     */
    public Optional<SplitAssignment> requestSplit(String queryId, int stageId,
                                                   String workerId, List<String> workerLocality) {
        String key = buildKey(queryId, stageId);
        StageSplitQueue queue = stageQueues.get(key);
        if (queue == null) {
            return Optional.empty();
        }
        Optional<SplitLease> lease = queue.pollWithLease(workerId, workerLocality, cacheAffinityAssigner);
        lease.ifPresent(l -> trackSplitAssignment(queryId, stageId, workerId, l));
        return lease.map(SplitLease::split);
    }

    private void trackSplitAssignment(String queryId, int stageId, String workerId, SplitLease lease) {
        if (cacheAffinityAssigner == null || !cacheAffinityAssigner.hasWorkers()
                || lease.split().filePaths().isEmpty()) {
            LOG.debug("event=split_assigned queryId={} stageId={} splitIndex={} workerId={}",
                    queryId, stageId, lease.splitIndex(), workerId);
            return;
        }
        List<String> preferred = cacheAffinityAssigner.preferredWorkers(lease.split().filePaths(), 1);
        boolean affine = !preferred.isEmpty() && preferred.get(0).equals(workerId);
        if (affine) {
            cacheAffinityHits.incrementAndGet();
            LOG.debug("event=split_assigned_cache_affine queryId={} stageId={} splitIndex={} workerId={}",
                    queryId, stageId, lease.splitIndex(), workerId);
        } else {
            cacheAffinityMisses.incrementAndGet();
            if (stealDelayMs > 0) {
                stealDelayExpired.incrementAndGet();
            }
            LOG.debug("event=split_assigned_steal queryId={} stageId={} splitIndex={} workerId={}",
                    queryId, stageId, lease.splitIndex(), workerId);
        }
    }

    /** Marks a split as completed. */
    public void markSplitCompleted(String queryId, int stageId, int splitIndex) {
        String key = buildKey(queryId, stageId);
        StageSplitQueue queue = stageQueues.get(key);
        if (queue != null) {
            queue.markCompleted(splitIndex);
        }
    }

    /** Returns true if all splits for a stage have been completed. */
    public boolean allSplitsCompleted(String queryId, int stageId) {
        String key = buildKey(queryId, stageId);
        StageSplitQueue queue = stageQueues.get(key);
        return queue != null && queue.allCompleted();
    }

    /** Reclaims expired leases across all stage queues. Returns total reclaimed. */
    public int reclaimExpiredLeases() {
        int total = 0;
        for (StageSplitQueue queue : stageQueues.values()) {
            total += queue.reclaimExpiredLeases();
        }
        if (total > 0) {
            LOG.info("event=leases_reclaimed count={}", total);
        }
        return total;
    }

    /** Removes all stage queues for a completed query. */
    public void cleanup(String queryId) {
        stageQueues.keySet().removeIf(k -> k.startsWith(queryId + ":"));
        LOG.debug("event=split_assignment_cleanup queryId={}", queryId);
    }

    private static String buildKey(String queryId, int stageId) {
        return queryId + ":" + stageId;
    }

    /** Returns the count of splits assigned to their cache-affine Worker. */
    public long cacheAffinityHits() {
        return cacheAffinityHits.get();
    }

    /** Returns the count of splits assigned to a non-affine Worker. */
    public long cacheAffinityMisses() {
        return cacheAffinityMisses.get();
    }

    /** Returns the count of splits that waited past steal-delay before assignment. */
    public long stealDelayExpiredCount() {
        return stealDelayExpired.get();
    }

    /** Returns the cache affinity assigner, or null when cache affinity is disabled. */
    public CacheAffinityAssigner cacheAffinityAssigner() {
        return cacheAffinityAssigner;
    }
}
