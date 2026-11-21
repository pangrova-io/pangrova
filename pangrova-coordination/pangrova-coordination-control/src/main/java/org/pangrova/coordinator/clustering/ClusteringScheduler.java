package org.pangrova.coordinator.clustering;

import org.pangrova.connector.api.clustering.ClusteringCapability;
import org.pangrova.connector.api.clustering.ClusteringDescriptor;
import org.pangrova.connector.api.clustering.ClusteringRequest;
import org.pangrova.connector.api.clustering.ClusteringResult;
import org.pangrova.connector.api.clustering.ColumnMinMax;
import org.pangrova.connector.api.clustering.FileClusteringStats;
import org.pangrova.connector.api.index.CommitEvent;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.index.CommitListener;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Schedules recluster tasks. Collects file stats, computes clustering ratio,
 * selects worst file groups by overlap depth, and dispatches recluster to connector.
 *
 * <p>When a {@link DistributedLease} is provided, acquires a per-table lease
 * ({@code recluster:{tableName}}) before execution and releases it in a finally
 * block, preventing concurrent recluster on the same table across coordinators
 * (BUG-D11 fix).</p>
 */
@ThreadSafe
public final class ClusteringScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ClusteringScheduler.class);
    private static final int DEFAULT_MAX_FILES_PER_GROUP = 128;
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(30);
    static final String LEASE_KEY_PREFIX = "recluster:";

    private final ExecutorService executor;
    private final CommitListener commitListener;
    private final DistributedLease distributedLease;
    private final String coordinatorId;
    private final String tenantId;

    public ClusteringScheduler(ExecutorService executor) {
        this(executor, null, null, null, null);
    }

    public ClusteringScheduler(
            ExecutorService executor, CommitListener commitListener, String tenantId) {
        this(executor, commitListener, null, null, tenantId);
    }

    /**
     * Full constructor with distributed lease support (BUG-D11).
     *
     * @param executor        thread pool for async recluster execution
     * @param commitListener  optional listener for index rebuild notifications
     * @param distributedLease optional lease for cross-coordinator mutual exclusion
     * @param coordinatorId   this coordinator's identifier (required if lease is non-null)
     */
    public ClusteringScheduler(ExecutorService executor, CommitListener commitListener,
                               DistributedLease distributedLease, String coordinatorId,
                               String tenantId) {
        this.executor = executor;
        this.commitListener = commitListener;
        this.distributedLease = distributedLease;
        this.coordinatorId = coordinatorId;
        if (commitListener != null && (tenantId == null || tenantId.isBlank())) {
            throw new IllegalArgumentException(
                    "tenantId is required when commit notification is enabled");
        }
        this.tenantId = tenantId;
    }

    /**
     * Manual recluster triggered by RECLUSTER TABLE statement.
     * Collects stats, selects worst files, and executes recluster via connector.
     *
     * <p>When a distributed lease is configured, acquires a per-table lease
     * before execution and releases it in a finally block.</p>
     */
    public CompletableFuture<ClusteringResult> scheduleManualRecluster(
            ClusteringDescriptor descriptor, ClusteringCapability capability) {
        return CompletableFuture.supplyAsync(
                () -> executeWithLease(descriptor, capability), executor);
    }

    private ClusteringResult executeWithLease(ClusteringDescriptor descriptor,
                                              ClusteringCapability capability) {
        if (distributedLease == null) {
            return executeRecluster(descriptor, capability);
        }
        String leaseKey = LEASE_KEY_PREFIX + descriptor.tableName();
        LeaseEntry held = acquireLeaseOrThrow(leaseKey, descriptor.tableName());
        try {
            return executeRecluster(descriptor, capability);
        } finally {
            distributedLease.release(leaseKey, coordinatorId, held.epoch());
            LOG.info("action=recluster_lease_released table={} coordinatorId={}",
                    descriptor.tableName(), coordinatorId);
        }
    }

    private LeaseEntry acquireLeaseOrThrow(String leaseKey, Object tableName) {
        LeaseEntry acquired = distributedLease.tryAcquire(
                leaseKey, coordinatorId, DEFAULT_LEASE_DURATION).orElse(null);
        if (acquired == null) {
            LOG.warn("action=recluster_lease_denied table={} coordinatorId={}",
                    tableName, coordinatorId);
            throw new IllegalStateException(
                    "Could not acquire recluster lease for table " + tableName
                            + "; another coordinator may be reclustering this table");
        }
        LOG.info("action=recluster_lease_acquired table={} coordinatorId={}",
                tableName, coordinatorId);
        return acquired;
    }

    private ClusteringResult executeRecluster(ClusteringDescriptor descriptor,
                                              ClusteringCapability capability) {
        List<String> worstFiles = identifyWorstFiles(descriptor, capability);
        if (worstFiles.isEmpty()) {
            return new ClusteringResult(0, List.of(), List.of(), 0);
        }
        ClusteringRequest request = new ClusteringRequest(
                descriptor.tableName(), descriptor.columnNames(),
                descriptor.mode(), worstFiles);
        long startMs = System.currentTimeMillis();
        ClusteringResult result = capability.recluster(request);
        LOG.info("action=recluster_complete table={} rewritten={} durationMs={}",
                descriptor.tableName(), result.rewrittenFileCount(),
                System.currentTimeMillis() - startMs);
        notifyIndexRebuild(descriptor.tableName().toString(), result);
        return result;
    }

    private List<String> identifyWorstFiles(ClusteringDescriptor descriptor,
                                            ClusteringCapability capability) {
        List<FileClusteringStats> stats = capability.collectFileStats(
                descriptor.tableName(), descriptor.columnNames());
        if (stats.isEmpty()) {
            LOG.info("action=recluster_skip table={} reason=no_files", descriptor.tableName());
            return List.of();
        }
        return selectWorstFiles(stats, descriptor.columnNames(), DEFAULT_MAX_FILES_PER_GROUP);
    }

    /**
     * Selects the worst-clustered files by overlap score (descending).
     * Uses O(N log N) sweep-line per column instead of O(N²) pairwise comparison.
     */
    static List<String> selectWorstFiles(
            List<FileClusteringStats> fileStats,
            List<String> clusteringColumns,
            int maxFiles) {
        double[] scores = new double[fileStats.size()];
        for (String col : clusteringColumns) {
            accumulateOverlapScores(fileStats, col, scores);
        }

        record FileScore(String filePath, double score) {}

        List<FileScore> ranked = new ArrayList<>(fileStats.size());
        for (int i = 0; i < fileStats.size(); i++) {
            ranked.add(new FileScore(fileStats.get(i).filePath(), scores[i]));
        }
        ranked.sort(Comparator.comparingDouble(FileScore::score).reversed());

        int limit = Math.min(ranked.size(), maxFiles);
        List<String> result = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            result.add(ranked.get(i).filePath());
        }
        return result;
    }

    /**
     * Sweep-line overlap counting for a single column.
     * Sorts files by min value, sweeps with a priority queue tracking active max values.
     * Each file's overlap count = number of active ranges when it starts.
     * Complexity: O(N log N) per column.
     */
    @SuppressWarnings("unchecked")
    private static void accumulateOverlapScores(
            List<FileClusteringStats> fileStats, String col, double[] scores) {
        List<Object[]> entries = collectBoundedEntries(fileStats, col);
        if (entries.size() <= 1) {
            return;
        }
        entries.sort(Comparator.comparing(e -> (Comparable) e[1]));
        PriorityQueue<Object[]> active = new PriorityQueue<>(
                Comparator.comparing(e -> (Comparable) e[2]));
        for (Object[] file : entries) {
            while (!active.isEmpty()
                    && ((Comparable) active.peek()[2]).compareTo(file[1]) < 0) {
                active.poll();
            }
            scores[(int) file[0]] += active.size();
            for (Object[] a : active) {
                scores[(int) a[0]] += 1.0;
            }
            active.add(file);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object[]> collectBoundedEntries(List<FileClusteringStats> fileStats, String col) {
        List<Object[]> entries = new ArrayList<>();
        for (int i = 0; i < fileStats.size(); i++) {
            ColumnMinMax cm = findBounds(fileStats.get(i), col);
            if (cm != null && cm.min() != null && cm.max() != null) {
                entries.add(new Object[]{i, (Comparable<Object>) cm.min(), (Comparable<Object>) cm.max()});
            }
        }
        return entries;
    }

    private static ColumnMinMax findBounds(FileClusteringStats stats, String col) {
        for (ColumnMinMax cm : stats.columnStats()) {
            if (cm.columnName().equals(col)) {
                return cm;
            }
        }
        return null;
    }

    /**
     * Notifies the commit listener that clustering has rewritten files,
     * triggering index rebuild for affected data files.
     */
    private void notifyIndexRebuild(String tableName, ClusteringResult result) {
        if (commitListener == null || result.rewrittenFileCount() == 0) {
            return;
        }
        try {
            CommitEvent event = new CommitEvent(
                    tenantId, "default", "default", tableName,
                    result.addedFilePaths(), result.removedFilePaths(),
                    Instant.now(), "recluster-" + Instant.now().toEpochMilli(),
                    CommitEventType.OVERWRITE, null);
            commitListener.onCommit(event);
            LOG.info("action=index_rebuild_triggered table={} newFiles={} deletedFiles={}",
                    tableName, result.addedFilePaths().size(), result.removedFilePaths().size());
        } catch (Exception e) {
            LOG.warn("action=index_rebuild_trigger_failed table={} error={}",
                    tableName, e.getMessage());
        }
    }
}
