package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

import java.util.List;
import java.util.function.Supplier;

/**
 * Pre-defined worker-level metrics: task counters, duration histograms,
 * memory gauges, and I/O counters.
 *
 * <p>Metric naming follows OTel conventions:
 * <ul>
 *   <li>Component segment: {@code pangrova.worker.*}</li>
 *   <li>Histograms use "duration" (not "latency")</li>
 *   <li>Duration unit is seconds ("s")</li>
 * </ul>
 */
public final class WorkerMetrics {

    private static final List<Double> DURATION_BOUNDARIES_SECONDS =
            List.of(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0);

    private final LongCounter tasksReceived;
    private final LongCounter tasksCompleted;
    private final LongCounter tasksFailed;
    private final LongUpDownCounter tasksRunning;
    private final DoubleHistogram taskDuration;
    private final ObservableDoubleGauge memoryUsed;
    private final ObservableDoubleGauge memoryReserved;
    private final LongCounter ioBytesRead;
    private final LongCounter ioBytesWritten;
    private final LongCounter ioRowsRead;
    private final LongCounter ioRowsWritten;

    /**
     * Registers the full worker metric set, wiring memory gauges to the given suppliers.
     */
    public WorkerMetrics(MetricsProvider provider,
                         Supplier<Double> memoryUsedSupplier,
                         Supplier<Double> memoryReservedSupplier) {
        this.tasksReceived = provider.createCounter(
                "pangrova.worker.tasks.received", "Total tasks received", "tasks");
        this.tasksCompleted = provider.createCounter(
                "pangrova.worker.tasks.completed", "Total tasks completed", "tasks");
        this.tasksFailed = provider.createCounter(
                "pangrova.worker.tasks.failed", "Total tasks failed", "tasks");
        this.tasksRunning = provider.createUpDownCounter(
                "pangrova.worker.tasks.running", "Currently running tasks", "tasks");
        this.taskDuration = provider.createHistogram(
                "pangrova.worker.task.duration", "Task duration", "s", DURATION_BOUNDARIES_SECONDS);
        this.memoryUsed = provider.createGauge(
                "pangrova.worker.memory.used", "Memory used", "bytes", memoryUsedSupplier);
        this.memoryReserved = provider.createGauge(
                "pangrova.worker.memory.reserved", "Memory reserved", "bytes", memoryReservedSupplier);
        this.ioBytesRead = provider.createCounter(
                "pangrova.worker.io.bytes.read", "Total bytes read", "bytes");
        this.ioBytesWritten = provider.createCounter(
                "pangrova.worker.io.bytes.written", "Total bytes written", "bytes");
        this.ioRowsRead = provider.createCounter(
                "pangrova.worker.io.rows.read", "Total rows read", "rows");
        this.ioRowsWritten = provider.createCounter(
                "pangrova.worker.io.rows.written", "Total rows written", "rows");
    }

    public LongCounter getTasksReceived() { return tasksReceived; }

    public LongCounter getTasksCompleted() { return tasksCompleted; }

    public LongCounter getTasksFailed() { return tasksFailed; }

    public LongUpDownCounter getTasksRunning() { return tasksRunning; }

    public DoubleHistogram getTaskDuration() { return taskDuration; }

    public LongCounter getIoBytesRead() { return ioBytesRead; }

    public LongCounter getIoBytesWritten() { return ioBytesWritten; }

    public LongCounter getIoRowsRead() { return ioRowsRead; }

    public LongCounter getIoRowsWritten() { return ioRowsWritten; }

    public ObservableDoubleGauge getMemoryUsed() { return memoryUsed; }

    public ObservableDoubleGauge getMemoryReserved() { return memoryReserved; }
}
