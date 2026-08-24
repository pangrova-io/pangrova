package org.pangrova.operations.scaling;

/**
 * Composite scoring policy for auto-scaling decisions.
 * Weights: CPU 0.3, Memory 0.2, Queue 0.3, Latency 0.2.
 */
public final class ScalingPolicy {

    private static final double CPU_WEIGHT = 0.3;
    private static final double MEMORY_WEIGHT = 0.2;
    private static final double QUEUE_WEIGHT = 0.3;
    private static final double LATENCY_WEIGHT = 0.2;

    private static final double SCALE_OUT_HIGH = 0.8;
    private static final double SCALE_OUT_LOW = 0.6;
    private static final double SCALE_IN_THRESHOLD = 0.2;

    public double calculateScore(MetricSnapshot snapshot) {
        return snapshot.cpuUtilization() * CPU_WEIGHT
                + snapshot.memoryUtilization() * MEMORY_WEIGHT
                + snapshot.queueDepthNormalized() * QUEUE_WEIGHT
                + snapshot.latencyNormalized() * LATENCY_WEIGHT;
    }

    public int calculateScaleOutTarget(double score, int currentWorkers) {
        if (score > SCALE_OUT_HIGH) {
            return currentWorkers + 2;
        }
        if (score > SCALE_OUT_LOW) {
            return currentWorkers + 1;
        }
        return currentWorkers;
    }

    public int calculateScaleInTarget(double score, int currentWorkers) {
        if (score < SCALE_IN_THRESHOLD && currentWorkers > 1) {
            return currentWorkers - 1;
        }
        return currentWorkers;
    }
}
