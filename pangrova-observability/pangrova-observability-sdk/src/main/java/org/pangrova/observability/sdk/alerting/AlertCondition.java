package org.pangrova.observability.sdk.alerting;

/**
 * Comparison conditions for alert rule evaluation.
 */
public enum AlertCondition {
    GREATER_THAN,
    LESS_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN_OR_EQUAL;

    /**
     * Evaluates whether the observed value satisfies this condition against the threshold.
     */
    public boolean evaluate(double observed, double threshold) {
        return switch (this) {
            case GREATER_THAN -> observed > threshold;
            case LESS_THAN -> observed < threshold;
            case GREATER_THAN_OR_EQUAL -> observed >= threshold;
            case LESS_THAN_OR_EQUAL -> observed <= threshold;
        };
    }
}
