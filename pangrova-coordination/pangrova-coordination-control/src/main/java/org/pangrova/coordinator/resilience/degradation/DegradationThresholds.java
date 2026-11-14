package org.pangrova.coordinator.resilience.degradation;

/**
 * Configurable thresholds for degradation level evaluation.
 * Immutable record — safe for concurrent access.
 * All worker ratio thresholds are "below this = degraded".
 * All memory pressure thresholds are "above this = degraded".
 *
 * @param criticalWorkerRatio      worker ratio below which → CRITICAL
 * @param criticalMemoryPressure   memory pressure above which → CRITICAL
 * @param severeWorkerRatio        worker ratio below which → SEVERE
 * @param moderateMemoryPressure   memory pressure above which → MODERATE
 * @param lightWorkerRatio         worker ratio below which → LIGHT
 * @param lightMemoryPressure      memory pressure above which → LIGHT
 */
public record DegradationThresholds(
        double criticalWorkerRatio,
        double criticalMemoryPressure,
        double severeWorkerRatio,
        double moderateMemoryPressure,
        double lightWorkerRatio,
        double lightMemoryPressure) {

    public static DegradationThresholds defaults() {
        return new DegradationThresholds(0.1, 0.95, 0.3, 0.9, 0.7, 0.7);
    }
}
