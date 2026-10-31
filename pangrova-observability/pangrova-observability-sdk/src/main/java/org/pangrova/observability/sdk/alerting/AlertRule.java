package org.pangrova.observability.sdk.alerting;

import java.time.Duration;

/**
 * Immutable definition of a single alert rule.
 *
 * <p>An alert rule monitors a named metric and fires when the metric value
 * exceeds the configured threshold for the specified evaluation window.
 *
 * @param name           unique rule identifier
 * @param metricName     the metric to monitor (e.g. "query.failure.rate")
 * @param condition      the comparison condition (GREATER_THAN, LESS_THAN, EQUALS)
 * @param threshold      the threshold value that triggers the alert
 * @param evaluationWindow the time window over which the metric is evaluated
 * @param severity       the alert severity level
 */
public record AlertRule(
        String name,
        String metricName,
        AlertCondition condition,
        double threshold,
        Duration evaluationWindow,
        AlertSeverity severity
) {
    public AlertRule {
        requireNonBlank(name, "name");
        requireNonBlank(metricName, "metricName");
        requireNonNull(condition, "condition");
        requirePositiveWindow(evaluationWindow);
        requireNonNull(severity, "severity");
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }

    private static void requirePositiveWindow(Duration window) {
        if (window == null || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("evaluationWindow must be a positive duration");
        }
    }
}
