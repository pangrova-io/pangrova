package org.pangrova.testing.benchmark;

/**
 * Detects performance regressions by comparing baseline vs current metrics.
 */
public final class PerformanceRegressionDetector {

    public enum Status {
        OK, INFO, WARN, FAIL
    }

    private static final double FAIL_THRESHOLD = 0.15;
    private static final double WARN_THRESHOLD = 0.10;
    private static final double INFO_THRESHOLD = 0.05;

    private PerformanceRegressionDetector() {}

    public static Status detect(double baseline, double current) {
        if (baseline <= 0) {
            return Status.OK;
        }
        double degradation = (current - baseline) / baseline;
        return classify(degradation);
    }

    private static Status classify(double degradation) {
        if (degradation > FAIL_THRESHOLD) {
            return Status.FAIL;
        }
        if (degradation > WARN_THRESHOLD) {
            return Status.WARN;
        }
        if (degradation > INFO_THRESHOLD) {
            return Status.INFO;
        }
        return Status.OK;
    }
}
