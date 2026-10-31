package org.pangrova.worker.resource.guard;

import org.pangrova.common.api.annotation.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Proactive per-query runaway guard: classifies queries that exceed configured CPU-time,
 * scanned-bytes, scanned-rows, or memory thresholds (StarRocks "big query" model).
 *
 * <p>This is the last-line defence against noisy-neighbour queries that the reactive
 * {@link org.pangrova.worker.resource.memory.CriticalPressureHandler} (kills the largest consumer only
 * once the node is already in CRITICAL pressure) does not cover. The guard is a pure decision
 * function: an external scheduler periodically feeds it
 * {@link QueryRuntimeMetrics} and cancels the returned {@link RunawayViolation violators}.</p>
 *
 * <p>Concurrency: immutable thresholds, no mutable state — safe to call from any thread.</p>
 */
@ThreadSafe
public final class RunawayQueryGuard {

    private final RunawayQueryThresholds thresholds;

    public RunawayQueryGuard(RunawayQueryThresholds thresholds) {
        this.thresholds = Objects.requireNonNull(thresholds, "thresholds must not be null");
    }

    /**
     * Evaluates a single query against the thresholds.
     *
     * @param metrics the query's current resource consumption
     * @return a violation describing the breached dimensions, or empty if within all limits
     */
    public Optional<RunawayViolation> evaluate(QueryRuntimeMetrics metrics) {
        Objects.requireNonNull(metrics, "metrics must not be null");
        List<String> reasons = new ArrayList<>();
        checkCpuTime(metrics, reasons);
        checkScannedBytes(metrics, reasons);
        checkScannedRows(metrics, reasons);
        checkMemory(metrics, reasons);
        return reasons.isEmpty()
                ? Optional.empty()
                : Optional.of(new RunawayViolation(metrics.queryId(), reasons));
    }

    private void checkCpuTime(QueryRuntimeMetrics metrics, List<String> reasons) {
        if (thresholds.enforcesCpuTime() && metrics.cpuTimeMs() > thresholds.maxCpuTimeMs()) {
            reasons.add("cpuTimeMs: " + metrics.cpuTimeMs() + " > " + thresholds.maxCpuTimeMs());
        }
    }

    private void checkScannedBytes(QueryRuntimeMetrics metrics, List<String> reasons) {
        if (thresholds.enforcesScannedBytes() && metrics.scannedBytes() > thresholds.maxScannedBytes()) {
            reasons.add("scannedBytes: " + metrics.scannedBytes() + " > " + thresholds.maxScannedBytes());
        }
    }

    private void checkScannedRows(QueryRuntimeMetrics metrics, List<String> reasons) {
        if (thresholds.enforcesScannedRows() && metrics.scannedRows() > thresholds.maxScannedRows()) {
            reasons.add("scannedRows: " + metrics.scannedRows() + " > " + thresholds.maxScannedRows());
        }
    }

    private void checkMemory(QueryRuntimeMetrics metrics, List<String> reasons) {
        if (thresholds.enforcesMemory() && metrics.memoryBytes() > thresholds.maxMemoryBytes()) {
            reasons.add("memoryBytes: " + metrics.memoryBytes() + " > " + thresholds.maxMemoryBytes());
        }
    }

    /**
     * Evaluates a batch of queries and returns the violators.
     *
     * @param metrics the queries' current resource consumption
     * @return the runaway violations, in input order; empty if all queries are within limits
     */
    public List<RunawayViolation> selectViolators(List<QueryRuntimeMetrics> metrics) {
        Objects.requireNonNull(metrics, "metrics must not be null");
        List<RunawayViolation> violations = new ArrayList<>();
        for (QueryRuntimeMetrics m : metrics) {
            evaluate(m).ifPresent(violations::add);
        }
        return violations;
    }

    public RunawayQueryThresholds thresholds() {
        return thresholds;
    }
}
