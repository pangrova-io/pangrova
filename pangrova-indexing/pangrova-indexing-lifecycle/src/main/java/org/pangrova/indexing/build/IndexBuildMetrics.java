package org.pangrova.indexing.build;

import org.pangrova.indexing.enums.IndexBuildError;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricsProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics collector for index build operations.
 *
 * <p>Registers counters and histograms via {@link MetricsProvider} at construction
 * time. Components call recording methods after each build operation.</p>
 */
public final class IndexBuildMetrics {

    /** Histogram bucket boundaries (ms) for build duration distribution. */
    private static final List<Double> DURATION_HISTOGRAM_BUCKETS_MS =
            List.of(10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 30000.0, 60000.0);

    private final DoubleHistogram buildDuration;
    private final LongCounter filesTotal;
    private final LongCounter failuresTotal;
    private final LongCounter rowsIndexedTotal;
    private final AtomicInteger inFlightBuilds = new AtomicInteger(0);

    /** Creates the collector and registers all metrics on the given provider. */
    public IndexBuildMetrics(MetricsProvider metricsProvider) {
        this.buildDuration = metricsProvider.createHistogram(
                "pangrova.index.build.duration_ms",
                "Index build task duration in milliseconds", "ms",
                DURATION_HISTOGRAM_BUCKETS_MS);
        this.filesTotal = metricsProvider.createCounter(
                "pangrova.index.build.files_total",
                "Total data files processed by index builds", "files");
        this.failuresTotal = metricsProvider.createCounter(
                "pangrova.index.build.failures_total",
                "Total data file build failures", "files");
        this.rowsIndexedTotal = metricsProvider.createCounter(
                "pangrova.index.build.rows_indexed_total",
                "Total rows indexed across all builds", "rows");
        metricsProvider.createGauge(
                "pangrova.index.build.in_flight",
                "Number of index build tasks currently in progress", "tasks",
                () -> (double) inFlightBuilds.get());
    }

    /** Records completion of a build task. */
    public void recordBuild(long durationMs, int filesProcessed, int filesFailed) {
        buildDuration.record(durationMs);
        filesTotal.add(filesProcessed);
        if (filesFailed > 0) {
            failuresTotal.add(filesFailed);
        }
    }

    /** Records completion of a build task with row count. */
    public void recordBuild(long durationMs, int filesProcessed, int filesFailed, long rowsIndexed) {
        recordBuild(durationMs, filesProcessed, filesFailed);
        if (rowsIndexed > 0) {
            rowsIndexedTotal.add(rowsIndexed);
        }
    }

    /** Records a build error with classification. */
    public void recordError(long durationMs, IndexBuildError type) {
        buildDuration.record(durationMs);
        failuresTotal.add(1);
    }

    /** Marks a build task as started (for in-flight tracking). */
    public void buildStarted() {
        inFlightBuilds.incrementAndGet();
    }

    /** Marks a build task as finished (for in-flight tracking). */
    public void buildFinished() {
        inFlightBuilds.decrementAndGet();
    }
}
