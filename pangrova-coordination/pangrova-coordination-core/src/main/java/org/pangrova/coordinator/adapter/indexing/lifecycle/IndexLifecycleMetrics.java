package org.pangrova.coordinator.adapter.indexing.lifecycle;

import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.MetricsProvider;

import java.util.List;

/**
 * Metrics collector for index lifecycle operations: GC, commit handling,
 * and consistency checking.
 *
 * <p>Registered once at coordinator startup. Each lifecycle component
 * calls the appropriate recording methods after completing operations.</p>
 */
public final class IndexLifecycleMetrics {

    // GC metrics
    private final LongCounter gcOrphanedFiles;
    private final LongCounter gcPurgedIndexes;

    // Commit handler metrics
    private final LongCounter commitEventsReceived;
    private final LongCounter commitEventsDropped;
    private final LongCounter commitBuildsTriggered;
    private final LongCounter commitBuildsDeferred;
    private final LongCounter commitProcessingFailures;
    private final DoubleHistogram commitLatency;

    // Consistency metrics
    private final LongCounter consistencyRebuildTriggered;
    private final LongCounter consistencyLowCoverageDetected;
    private final DoubleHistogram consistencyCoverageRatio;

    // Compaction metrics (R11.5)
    private final LongCounter compactionStaleEntries;

    public IndexLifecycleMetrics(MetricsProvider metricsProvider) {
        // GC
        this.gcOrphanedFiles = metricsProvider.createCounter(
                "pangrova.index.gc.orphaned_files",
                "Orphan index files cleaned by GC", "files");
        this.gcPurgedIndexes = metricsProvider.createCounter(
                "pangrova.index.gc.purged_indexes",
                "DROPPED indexes fully purged by GC", "indexes");

        // Commit handler
        this.commitEventsReceived = metricsProvider.createCounter(
                "pangrova.index.commit.events_received",
                "Commit events received from connectors", "events");
        this.commitEventsDropped = metricsProvider.createCounter(
                "pangrova.index.commit.events_dropped",
                "Commit events dropped due to full queue", "events");
        this.commitBuildsTriggered = metricsProvider.createCounter(
                "pangrova.index.commit.builds_triggered",
                "Incremental builds triggered by commit events", "builds");
        this.commitBuildsDeferred = metricsProvider.createCounter(
                "pangrova.index.commit.builds_deferred",
                "Incremental builds deferred due to pool saturation", "builds");
        this.commitProcessingFailures = metricsProvider.createCounter(
                "pangrova.index.commit.processing_failures",
                "Commit event processing failures", "failures");
        this.commitLatency = metricsProvider.createHistogram(
                "pangrova.index.commit.latency_ms",
                "Commit-to-build-trigger latency in milliseconds", "ms",
                List.of(10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0));

        // Consistency
        this.consistencyRebuildTriggered = metricsProvider.createCounter(
                "pangrova.index.consistency.rebuild_triggered",
                "Consistency-driven rebuilds triggered", "rebuilds");
        this.consistencyLowCoverageDetected = metricsProvider.createCounter(
                "pangrova.index.consistency.low_coverage_detected",
                "Indexes detected with coverage below threshold", "indexes");
        this.consistencyCoverageRatio = metricsProvider.createHistogram(
                "pangrova.index.consistency.coverage_ratio",
                "Index coverage ratio at consistency check time", "ratio",
                List.of(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0));

        // Compaction (R11.5)
        this.compactionStaleEntries = metricsProvider.createCounter(
                "pangrova.index.compaction.stale_entries",
                "Index file entries marked stale after compaction", "entries");
    }

    // GC recording
    public void recordGcCycle(int orphanedCount, int purgedCount) {
        if (orphanedCount > 0) {
            gcOrphanedFiles.add(orphanedCount);
        }
        if (purgedCount > 0) {
            gcPurgedIndexes.add(purgedCount);
        }
    }

    // Commit recording
    public void recordCommitEventReceived() {
        commitEventsReceived.add(1);
    }

    public void recordCommitEventDropped() {
        commitEventsDropped.add(1);
    }

    public void recordCommitBuildTriggered() {
        commitBuildsTriggered.add(1);
    }

    public void recordCommitBuildDeferred() {
        commitBuildsDeferred.add(1);
    }

    public void recordCommitProcessingFailure() {
        commitProcessingFailures.add(1);
    }

    public void recordCommitLatency(long durationMs) {
        commitLatency.record(durationMs);
    }

    // Consistency recording
    public void recordConsistencyRebuild() {
        consistencyRebuildTriggered.add(1);
    }

    public void recordLowCoverage(String indexId, double coverageRatio) {
        consistencyLowCoverageDetected.add(1);
        consistencyCoverageRatio.record(coverageRatio);
    }

    // Compaction recording (R11.5)
    public void recordCompactionStaleEntries(int count) {
        if (count > 0) {
            compactionStaleEntries.add(count);
        }
    }
}
