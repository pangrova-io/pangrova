package org.pangrova.testing.benchmark;

import org.pangrova.testing.benchmark.enums.PerformanceRegressionStatus;

/**
 * Detects performance regressions by comparing baseline vs current metrics.
 */
public final class PerformanceRegressionDetector {

    private static final double FAIL_THRESHOLD = 0.15;
    private static final double WARN_THRESHOLD = 0.10;
    private static final double INFO_THRESHOLD = 0.05;

    private PerformanceRegressionDetector() {}

    public static PerformanceRegressionStatus detect(double baseline, double current) {
        if (baseline <= 0) {
            return PerformanceRegressionStatus.OK;
        }
        double degradation = (current - baseline) / baseline;
        return classify(degradation);
    }

    private static PerformanceRegressionStatus classify(double degradation) {
        if (degradation > FAIL_THRESHOLD) {
            return PerformanceRegressionStatus.FAIL;
        }
        if (degradation > WARN_THRESHOLD) {
            return PerformanceRegressionStatus.WARN;
        }
        if (degradation > INFO_THRESHOLD) {
            return PerformanceRegressionStatus.INFO;
        }
        return PerformanceRegressionStatus.OK;
    }
}
