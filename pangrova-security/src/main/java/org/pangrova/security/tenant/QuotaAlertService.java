package org.pangrova.security.tenant;

import java.util.Optional;

/**
 * Evaluates tenant resource usage against warning (80%) and critical (95%) thresholds.
 */
public class QuotaAlertService {

    private static final double WARNING_THRESHOLD = 0.80;
    private static final double CRITICAL_THRESHOLD = 0.95;

    /**
     * Evaluates current usage against the configured alert thresholds.
     *
     * @param tenantId tenant identifier
     * @param metric resource metric name
     * @param used current resource usage
     * @param limit configured resource limit
     * @return an alert when usage crosses a threshold
     */
    public Optional<QuotaAlert> checkUsage(String tenantId, String metric, long used, long limit) {
        if (limit <= 0) {
            return Optional.empty();
        }
        double ratio = (double) used / limit;
        return evaluateThreshold(tenantId, metric, ratio);
    }

    private Optional<QuotaAlert> evaluateThreshold(String tenantId, String metric, double ratio) {
        if (ratio >= CRITICAL_THRESHOLD) {
            return Optional.of(new QuotaAlert(tenantId, metric, AlertSeverity.CRITICAL, ratio));
        }
        if (ratio >= WARNING_THRESHOLD) {
            return Optional.of(new QuotaAlert(tenantId, metric, AlertSeverity.WARNING, ratio));
        }
        return Optional.empty();
    }
}
