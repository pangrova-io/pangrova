package org.pangrova.worker.execution.filter;

import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for DPP (Dynamic Partition Pruning) filter application on the Worker side.
 *
 * <p>Tracks the number of splits and estimated rows filtered out by runtime filters
 * before data is passed to the native execution engine.</p>
 *
 * <p>Thread safety: all counters use {@link LongAdder} for lock-free concurrent updates.</p>
 */
public final class DynamicPartitionFilterMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionFilterMetrics.class);

    private static final LongAdder TOTAL_SPLITS_SEEN = new LongAdder();
    private static final LongAdder SPLITS_FILTERED_OUT = new LongAdder();
    private static final LongAdder FILTER_APPLICATIONS = new LongAdder();

    private DynamicPartitionFilterMetrics() {}

    /**
     * Records the result of applying DPP filters to a task's splits.
     *
     * @param queryId        query identifier for structured logging
     * @param stageId        stage identifier for structured logging
     * @param totalSplits    total number of splits before filtering
     * @param filteredSplits number of splits pruned by filters
     */
    public static void recordSplitFiltering(String queryId, int stageId,
                                            int totalSplits, int filteredSplits) {
        TOTAL_SPLITS_SEEN.add(totalSplits);
        SPLITS_FILTERED_OUT.add(filteredSplits);
        FILTER_APPLICATIONS.increment();

        if (filteredSplits > 0) {
            LOG.info("event=dpp_splits_filtered queryId={} stageId={} totalSplits={} "
                            + "filteredSplits={} retainedSplits={} filterRatio={}",
                    queryId, stageId, totalSplits, filteredSplits,
                    totalSplits - filteredSplits,
                    totalSplits > 0 ? String.format("%.2f", (double) filteredSplits / totalSplits) : "0.00");
        }
    }

    /** Total splits seen across all filter applications. */
    public static long totalSplitsSeen() {
        return TOTAL_SPLITS_SEEN.sum();
    }

    /** Total splits filtered out across all filter applications. */
    public static long splitsFilteredOut() {
        return SPLITS_FILTERED_OUT.sum();
    }

    /** Total number of filter application invocations. */
    public static long filterApplications() {
        return FILTER_APPLICATIONS.sum();
    }

    /** Resets all counters (for testing). */
    public static void reset() {
        TOTAL_SPLITS_SEEN.reset();
        SPLITS_FILTERED_OUT.reset();
        FILTER_APPLICATIONS.reset();
    }
}
