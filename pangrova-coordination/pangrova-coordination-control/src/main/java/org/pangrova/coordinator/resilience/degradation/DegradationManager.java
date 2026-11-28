package org.pangrova.coordinator.resilience.degradation;

import org.pangrova.coordinator.enums.DegradationLevel;
import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.DoubleSupplier;

/**
 * Evaluates system health metrics and determines degradation level.
 * Supports both push (explicit parameters) and pull (supplier-based) modes.
 *
 * <p>Concurrency note: {@link #evaluateCurrent()} reads two suppliers
 * non-atomically — workerRatio and memoryPressure may come from different
 * instants. This is acceptable for degradation heuristics.</p>
 */
@ThreadSafe
public class DegradationManager {

    private static final Logger LOG = LoggerFactory.getLogger(DegradationManager.class);

    private final DegradationThresholds thresholds;
    private final DoubleSupplier workerRatioSupplier;
    private final DoubleSupplier memoryPressureSupplier;

    /** Default thresholds, no live metric suppliers (push mode only). */
    public DegradationManager() {
        this(DegradationThresholds.defaults(), () -> 1.0, () -> 0.0);
    }

    /** Custom thresholds with live metric suppliers for periodic evaluation. */
    public DegradationManager(DegradationThresholds thresholds,
                              DoubleSupplier workerRatioSupplier,
                              DoubleSupplier memoryPressureSupplier) {
        this.thresholds = thresholds != null ? thresholds : DegradationThresholds.defaults();
        this.workerRatioSupplier = workerRatioSupplier;
        this.memoryPressureSupplier = memoryPressureSupplier;
    }

    /** Evaluates degradation from explicit metric values (push mode). */
    public DegradationLevel evaluateDegradation(double workerRatio, double memoryPressure) {
        DegradationLevel critical = evaluateCriticalOrSevere(workerRatio, memoryPressure);
        if (critical != null) {
            return critical;
        }
        return evaluateLightOrModerate(workerRatio, memoryPressure);
    }

    private DegradationLevel evaluateCriticalOrSevere(double workerRatio, double memoryPressure) {
        if (workerRatio < thresholds.criticalWorkerRatio()
                || memoryPressure > thresholds.criticalMemoryPressure()) {
            return DegradationLevel.CRITICAL;
        }
        if (workerRatio < thresholds.severeWorkerRatio()) {
            return DegradationLevel.SEVERE;
        }
        return null;
    }

    private DegradationLevel evaluateLightOrModerate(double workerRatio, double memoryPressure) {
        if (memoryPressure > thresholds.moderateMemoryPressure()) {
            return DegradationLevel.MODERATE;
        }
        if (workerRatio < thresholds.lightWorkerRatio()
                || memoryPressure > thresholds.lightMemoryPressure()) {
            return DegradationLevel.LIGHT;
        }
        return DegradationLevel.NONE;
    }

    /** Evaluates degradation from live metric suppliers (pull mode). */
    public DegradationLevel evaluateCurrent() {
        double workerRatio = workerRatioSupplier.getAsDouble();
        double memoryPressure = memoryPressureSupplier.getAsDouble();
        DegradationLevel level = evaluateDegradation(workerRatio, memoryPressure);
        if (level != DegradationLevel.NONE) {
            LOG.info("event=degradation_evaluated level={} workerRatio={} memoryPressure={}",
                    level, workerRatio, memoryPressure);
        }
        return level;
    }

    /** Returns true if new queries are allowed at the given degradation level. */
    public boolean checkQueryAllowed(DegradationLevel level) {
        boolean allowed = level != DegradationLevel.CRITICAL;
        if (!allowed) {
            LOG.warn("Query rejected due to CRITICAL degradation level");
        }
        return allowed;
    }
}
