package org.pangrova.observability.sdk.metrics.jvm;

import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.ObservableDoubleGauge;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * Collects standard JVM metrics via MXBeans and exports them through the Metrics SPI.
 *
 * <p>Metrics collected:
 * <ul>
 *   <li>Heap memory: used, committed, max</li>
 *   <li>GC: total collection count and total collection time (ms)</li>
 *   <li>Threads: live thread count, daemon thread count, peak thread count</li>
 * </ul>
 *
 * <p>All gauges are pull-based: values are read from MXBeans at collection time.
 * GC counters are also exposed as gauges since MXBeans provide cumulative totals.
 */
public final class JvmMetricsCollector {

    private final ObservableDoubleGauge heapUsed;
    private final ObservableDoubleGauge heapCommitted;
    private final ObservableDoubleGauge heapMax;
    private final ObservableDoubleGauge gcCount;
    private final ObservableDoubleGauge gcTime;
    private final ObservableDoubleGauge threadCount;
    private final ObservableDoubleGauge daemonThreadCount;
    private final ObservableDoubleGauge peakThreadCount;

    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private final List<GarbageCollectorMXBean> gcBeans;

    /**
     * Creates a collector using the platform MXBeans of the current JVM.
     */
    public JvmMetricsCollector(MetricsProvider provider) {
        this(provider,
             ManagementFactory.getMemoryMXBean(),
             ManagementFactory.getThreadMXBean(),
             ManagementFactory.getGarbageCollectorMXBeans());
    }

    /**
     * Constructor accepting explicit MXBean instances for testability and custom environments.
     */
    public JvmMetricsCollector(MetricsProvider provider,
                               MemoryMXBean memoryBean,
                               ThreadMXBean threadBean,
                               List<GarbageCollectorMXBean> gcBeans) {
        this.memoryBean = memoryBean;
        this.threadBean = threadBean;
        this.gcBeans = List.copyOf(gcBeans);

        this.heapUsed = provider.createGauge(
                "jvm.memory.heap.used", "JVM heap memory used", "bytes",
                this::getHeapUsed);
        this.heapCommitted = provider.createGauge(
                "jvm.memory.heap.committed", "JVM heap memory committed", "bytes",
                this::getHeapCommitted);
        this.heapMax = provider.createGauge(
                "jvm.memory.heap.max", "JVM heap memory max", "bytes",
                this::getHeapMax);
        this.gcCount = provider.createGauge(
                "jvm.gc.count", "Total GC collection count", "collections",
                this::getTotalGcCount);
        this.gcTime = provider.createGauge(
                "jvm.gc.time", "Total GC collection time", "ms",
                this::getTotalGcTime);
        this.threadCount = provider.createGauge(
                "jvm.threads.live", "Current live thread count", "threads",
                this::getLiveThreadCount);
        this.daemonThreadCount = provider.createGauge(
                "jvm.threads.daemon", "Current daemon thread count", "threads",
                this::getDaemonThreadCount);
        this.peakThreadCount = provider.createGauge(
                "jvm.threads.peak", "Peak thread count since JVM start", "threads",
                this::getPeakThreadCount);
    }

    public double getHeapUsed() {
        MemoryUsage usage = memoryBean.getHeapMemoryUsage();
        return (double) usage.getUsed();
    }

    public double getHeapCommitted() {
        MemoryUsage usage = memoryBean.getHeapMemoryUsage();
        return (double) usage.getCommitted();
    }

    public double getHeapMax() {
        MemoryUsage usage = memoryBean.getHeapMemoryUsage();
        long max = usage.getMax();
        return max == -1 ? 0.0 : (double) max;
    }

    /**
     * Returns the total number of GC collections across all collectors.
     */
    public double getTotalGcCount() {
        long total = 0;
        for (GarbageCollectorMXBean gc : gcBeans) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                total += count;
            }
        }
        return (double) total;
    }

    /**
     * Returns the total GC collection time in milliseconds across all collectors.
     */
    public double getTotalGcTime() {
        long total = 0;
        for (GarbageCollectorMXBean gc : gcBeans) {
            long time = gc.getCollectionTime();
            if (time >= 0) {
                total += time;
            }
        }
        return (double) total;
    }

    public double getLiveThreadCount() {
        return (double) threadBean.getThreadCount();
    }

    public double getDaemonThreadCount() {
        return (double) threadBean.getDaemonThreadCount();
    }

    public double getPeakThreadCount() {
        return (double) threadBean.getPeakThreadCount();
    }
}
