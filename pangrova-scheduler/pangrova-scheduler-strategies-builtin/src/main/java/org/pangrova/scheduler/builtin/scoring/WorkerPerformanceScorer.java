package org.pangrova.scheduler.builtin.scoring;

/**
 * Computes a composite performance score for Worker selection.
 * Score = 0.3*(1-memRatio) + 0.3*(1-cpuUtil) + 0.2*sigmoid(diskLat) + 0.2*sigmoid(netLat).
 */
public class WorkerPerformanceScorer {

    private static final double MEMORY_WEIGHT = 0.3;
    private static final double CPU_WEIGHT = 0.3;
    private static final double DISK_WEIGHT = 0.2;
    private static final double NETWORK_WEIGHT = 0.2;
    private static final double DEPRIORITIZE_THRESHOLD = 0.4;
    private static final double BLACKLIST_THRESHOLD = 0.2;

    /**
     * Worker resource statistics for scoring.
     */
    public record WorkerStats(
            double memoryPressureRatio,
            double cpuUtilization,
            double p99DiskLatencyMs,
            double p99NetworkLatencyMs) {}

    /** Combines resource headroom and normalized latency using the fixed selection weights. */
    public double computeScore(WorkerStats stats) {
        double memScore = 1.0 - stats.memoryPressureRatio();
        double cpuScore = 1.0 - stats.cpuUtilization();
        double diskScore = normalizeLatency(stats.p99DiskLatencyMs());
        double netScore = normalizeLatency(stats.p99NetworkLatencyMs());
        return MEMORY_WEIGHT * memScore + CPU_WEIGHT * cpuScore
                + DISK_WEIGHT * diskScore + NETWORK_WEIGHT * netScore;
    }

    /**
     * Sigmoid normalization: lower latency → higher score.
     * Center at 10ms, scale factor 5ms.
     */
    public double normalizeLatency(double latencyMs) {
        return 1.0 / (1.0 + Math.exp((latencyMs - 10.0) / 5.0));
    }

    public boolean isDeprioritized(double score) {
        return score < DEPRIORITIZE_THRESHOLD;
    }

    public boolean isBlacklisted(double score) {
        return score < BLACKLIST_THRESHOLD;
    }
}
