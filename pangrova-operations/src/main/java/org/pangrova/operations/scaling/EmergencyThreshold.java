package org.pangrova.operations.scaling;

/**
 * Thresholds that trigger immediate emergency scale-out.
 */
public record EmergencyThreshold(
        double cpuThreshold,
        double memoryThreshold,
        double queueThreshold,
        double latencyThreshold) {

    public boolean isBreached(MetricSnapshot snapshot) {
        return snapshot.cpuUtilization() > cpuThreshold
                || snapshot.memoryUtilization() > memoryThreshold
                || snapshot.queueDepthNormalized() > queueThreshold
                || snapshot.latencyNormalized() > latencyThreshold;
    }
}
