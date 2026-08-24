package org.pangrova.coordinator.worker;

import java.util.List;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.resource.MemoryPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates cluster-wide resource pressure from per-worker heartbeat data
 * and provides admission gate decisions for query submission.
 *
 * <p>This component bridges the gap between worker-level resource monitoring
 * (each worker runs its own {@code ResourceMonitor} loop, fixed in F-RES-08)
 * and coordinator-level admission control. Heartbeats already carry
 * {@link MemoryPressure} per worker into {@link WorkerManager}; this monitor
 * aggregates that data to answer "is the cluster healthy enough to accept
 * a new query?"</p>
 *
 * <p><b>Admission policy:</b>
 * <ul>
 *   <li>If <b>all</b> active workers are in {@code CRITICAL} pressure, reject
 *       unconditionally (no node can absorb work).</li>
 *   <li>If the fraction of workers at {@code HIGH} or {@code CRITICAL} exceeds
 *       a configurable threshold (default 50%), reject (cluster is overloaded).</li>
 *   <li>Otherwise, admit.</li>
 * </ul>
 * When no active workers exist, admission is denied (scheduling would fail
 * anyway).</p>
 *
 * <p>Concurrency: thread-safe (reads only from {@link WorkerManager}).</p>
 *
 * <p>G-RES-09 fix: provides the telemetry-feedback loop for elasticity/admission
 * that was previously missing.</p>
 */
@ThreadSafe
public class ClusterResourcePressureMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterResourcePressureMonitor.class);

    /** Default fraction of workers under HIGH/CRITICAL pressure that triggers rejection. */
    static final double DEFAULT_PRESSURE_THRESHOLD = 0.5;

    private final WorkerManager workerManager;
    private final double pressureThreshold;

    /**
     * Creates a monitor with the default 50% pressure threshold.
     */
    public ClusterResourcePressureMonitor(WorkerManager workerManager) {
        this(workerManager, DEFAULT_PRESSURE_THRESHOLD);
    }

    /**
     * Creates a monitor with a configurable pressure threshold.
     *
     * @param workerManager     the worker manager to query for active workers
     * @param pressureThreshold fraction (0.0-1.0) of workers under HIGH/CRITICAL
     *                          pressure that triggers query rejection
     */
    public ClusterResourcePressureMonitor(WorkerManager workerManager, double pressureThreshold) {
        if (workerManager == null) {
            throw new IllegalArgumentException("workerManager must not be null");
        }
        if (pressureThreshold < 0.0 || pressureThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "pressureThreshold must be between 0.0 and 1.0: " + pressureThreshold);
        }
        this.workerManager = workerManager;
        this.pressureThreshold = pressureThreshold;
    }

    /**
     * Immutable snapshot of cluster-wide resource pressure at evaluation time.
     *
     * @param totalActiveWorkers  number of active workers
     * @param criticalCount       number of workers in CRITICAL pressure
     * @param highCount           number of workers in HIGH pressure
     * @param stressedFraction    fraction of workers in HIGH or CRITICAL
     * @param admissionAllowed    whether queries should be admitted
     * @param rejectionReason     human-readable reason when rejected; null when allowed
     */
    public record ClusterPressureSnapshot(
            int totalActiveWorkers,
            int criticalCount,
            int highCount,
            double stressedFraction,
            boolean admissionAllowed,
            String rejectionReason) {}

    /**
     * Evaluates cluster pressure and returns whether a new query should be admitted.
     *
     * @return true if the cluster can accept a new query
     */
    public boolean shouldAdmit() {
        return evaluate().admissionAllowed();
    }

    /**
     * Evaluates cluster pressure and returns a detailed snapshot.
     */
    public ClusterPressureSnapshot evaluate() {
        List<WorkerInfo> activeWorkers = workerManager.getActiveWorkers();
        int total = activeWorkers.size();

        if (total == 0) {
            return new ClusterPressureSnapshot(0, 0, 0, 0.0, false,
                    "no active workers available");
        }

        int criticalCount = 0;
        int highCount = 0;
        for (WorkerInfo worker : activeWorkers) {
            MemoryPressure pressure = worker.memoryPressure();
            if (pressure == MemoryPressure.CRITICAL) {
                criticalCount++;
            } else if (pressure == MemoryPressure.HIGH) {
                highCount++;
            }
        }

        double stressedFraction = (double) (criticalCount + highCount) / total;

        // All workers in CRITICAL: unconditional reject
        if (criticalCount == total) {
            String reason = String.format(
                    "all %d active workers in CRITICAL memory pressure", total);
            LOG.warn("event=cluster_pressure_reject reason={}", reason);
            return new ClusterPressureSnapshot(
                    total, criticalCount, highCount, stressedFraction, false, reason);
        }

        // Stressed fraction exceeds threshold: reject
        if (stressedFraction > pressureThreshold) {
            String reason = String.format(
                    "%.0f%% of workers under HIGH/CRITICAL pressure (threshold: %.0f%%), "
                            + "critical=%d high=%d total=%d",
                    stressedFraction * 100, pressureThreshold * 100,
                    criticalCount, highCount, total);
            LOG.warn("event=cluster_pressure_reject reason={}", reason);
            return new ClusterPressureSnapshot(
                    total, criticalCount, highCount, stressedFraction, false, reason);
        }

        return new ClusterPressureSnapshot(
                total, criticalCount, highCount, stressedFraction, true, null);
    }

    /**
     * Returns the configured pressure threshold.
     */
    public double getPressureThreshold() {
        return pressureThreshold;
    }
}
