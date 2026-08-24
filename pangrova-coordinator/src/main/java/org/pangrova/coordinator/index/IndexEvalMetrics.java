package org.pangrova.coordinator.index;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;

import java.util.List;

/**
 * Metrics collector for index evaluation (split filtering) operations.
 *
 * <p>Registered once at startup; recording methods called from
 * {@link IndexSplitFilter} during query execution.</p>
 */
public final class IndexEvalMetrics {

    private final DoubleHistogram evalDuration;
    private final LongCounter skippedSplits;
    private final LongCounter totalSplits;
    private final LongCounter timeouts;
    private final LongCounter accessorResolveFailures;
    private final DoubleHistogram metadataBatchMs;
    private final DoubleHistogram parallelTasks;
    private final LongCounter l0Prunes;
    private final LongCounter costSkips;
    private final LongCounter coverageSkips;
    private final LongCounter l1Prunes;
    private final LongCounter availabilityGapKeeps;
    private final LongCounter evalWithDeleteFiles;
    private final LongCounter nativeEvalFailures;

    public IndexEvalMetrics(MetricsProvider metricsProvider) {
        this.evalDuration = metricsProvider.createHistogram(
                "pangrova.index.eval.duration_ms",
                "Per-query index evaluation duration in milliseconds", "ms",
                List.of(1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0));
        this.skippedSplits = metricsProvider.createCounter(
                "pangrova.index.eval.skipped_splits",
                "Splits skipped by index evaluation", "splits");
        this.totalSplits = metricsProvider.createCounter(
                "pangrova.index.eval.total_splits",
                "Total splits evaluated", "splits");
        this.timeouts = metricsProvider.createCounter(
                "pangrova.index.eval.timeouts",
                "Per-split evaluation timeouts", "timeouts");
        this.accessorResolveFailures = metricsProvider.createCounter(
                "pangrova.index.eval.accessor_resolve_failures",
                "Accessor resolution failures", "failures");
        this.metadataBatchMs = metricsProvider.createHistogram(
                "pangrova.index.eval.metadata_batch_ms",
                "batchGetIndexFileMappings latency in milliseconds", "ms",
                List.of(1.0, 5.0, 10.0, 50.0, 100.0, 500.0));
        this.parallelTasks = metricsProvider.createHistogram(
                "pangrova.index.eval.parallel_tasks",
                "Parallel evaluation task count per query", "tasks",
                List.of(1.0, 10.0, 50.0, 100.0, 500.0, 1000.0));
        this.l0Prunes = metricsProvider.createCounter(
                "pangrova.index.eval.l0_prunes",
                "Splits pruned by L0 file column statistics", "splits");
        this.costSkips = metricsProvider.createCounter(
                "pangrova.index.eval.cost_skips",
                "Splits where L1/L2 skipped due to L0 pruning >95%", "splits");
        this.coverageSkips = metricsProvider.createCounter(
                "pangrova.index.eval.coverage_skips",
                "Index evaluations skipped due to coverage <50%", "indexes");
        this.evalWithDeleteFiles = metricsProvider.createCounter(
                "pangrova.index.eval.with_delete_files",
                "Splits evaluated that have associated MOR delete files", "splits");
        this.l1Prunes = metricsProvider.createCounter(
                "pangrova.index.eval.l1_prunes",
                "Splits pruned by L1 native index evaluation", "splits");
        this.availabilityGapKeeps = metricsProvider.createCounter(
                "pangrova.index.eval.availability_gap_keeps",
                "Files kept due to availability window bypass (outside indexed snapshot)", "files");
        this.nativeEvalFailures = metricsProvider.createCounter(
                "pangrova.index.eval.native_eval_failures",
                "Native index evaluator failures (unavailable or threw exception)", "failures");
    }

    /** Records a completed filterSplits call. */
    public void recordFilterCall(long durationMs, int totalCount, int skippedCount) {
        evalDuration.record(durationMs);
        totalSplits.add(totalCount);
        if (skippedCount > 0) {
            skippedSplits.add(skippedCount);
        }
    }

    public void recordTimeout() {
        timeouts.add(1);
    }

    public void recordAccessorResolveFailure() {
        accessorResolveFailures.add(1);
    }

    public void recordMetadataBatchLatency(long durationMs) {
        metadataBatchMs.record(durationMs);
    }

    public void recordParallelTaskCount(int taskCount) {
        parallelTasks.record(taskCount);
    }

    /** Records a split pruned by L0 file column statistics. */
    public void recordL0Prune() {
        l0Prunes.add(1);
    }

    /** Records a split where L1/L2 was skipped because L0 pruned >95%. */
    public void recordCostSkip() {
        costSkips.add(1);
    }

    /** Records an index evaluation skipped due to coverage <50%. */
    public void recordCoverageSkip() {
        coverageSkips.add(1);
    }

    /** Records a split evaluation where the split has MOR delete files. */
    public void recordEvalWithDeleteFiles() {
        evalWithDeleteFiles.add(1);
    }

    /** Records a split pruned by L1 native index evaluation. */
    public void recordL1Prune() {
        l1Prunes.add(1);
    }

    /** Records a file kept due to availability window bypass. */
    public void recordAvailabilityGapKeep() {
        availabilityGapKeeps.add(1);
    }

    /** Records a native index evaluation failure (evaluator unavailable or threw exception). */
    public void recordNativeEvalFailure() {
        nativeEvalFailures.add(1);
    }
}
