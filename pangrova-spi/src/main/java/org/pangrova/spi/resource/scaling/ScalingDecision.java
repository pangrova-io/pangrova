package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * The outcome of an {@link AutoScaler#evaluate} call: a direction plus target worker count.
 *
 * @param direction     the scaling action to take
 * @param targetWorkers desired worker count after the action (must be non-negative)
 * @param reason        human-readable justification for observability/audit
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI")
public record ScalingDecision(
        ScalingDirection direction,
        int targetWorkers,
        String reason) {

    public ScalingDecision {
        Objects.requireNonNull(direction, "direction must not be null");
        if (targetWorkers < 0) {
            throw new IllegalArgumentException(
                    "targetWorkers must be non-negative, got " + targetWorkers);
        }
        Objects.requireNonNull(reason, "reason must not be null");
    }

    /** Convenience factory for a HOLD decision (no change). */
    public static ScalingDecision hold(int currentWorkers) {
        return new ScalingDecision(ScalingDirection.HOLD, currentWorkers, "within thresholds");
    }

    /** Convenience factory for a SCALE_OUT decision. */
    public static ScalingDecision scaleOut(int targetWorkers, String reason) {
        return new ScalingDecision(ScalingDirection.SCALE_OUT, targetWorkers, reason);
    }

    /** Convenience factory for a SCALE_IN decision. */
    public static ScalingDecision scaleIn(int targetWorkers, String reason) {
        return new ScalingDecision(ScalingDirection.SCALE_IN, targetWorkers, reason);
    }

    /** Convenience factory for a SUSPEND decision. */
    public static ScalingDecision suspend(int targetWorkers, String reason) {
        return new ScalingDecision(ScalingDirection.SUSPEND, targetWorkers, reason);
    }
}
