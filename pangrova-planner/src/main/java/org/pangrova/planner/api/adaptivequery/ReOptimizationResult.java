package org.pangrova.planner.api.adaptivequery;

import org.pangrova.planner.api.enums.JoinStrategy;

import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;
import java.util.Optional;

/**
 * Result of evaluating whether a stage needs re-optimization.
 *
 * @param triggered        whether re-optimization was triggered
 * @param reason           human-readable reason (empty when not triggered)
 * @param newJoinStrategy  new join strategy if a switch is recommended
 */
@Internal
public record ReOptimizationResult(
        boolean triggered,
        String reason,
        Optional<JoinStrategy> newJoinStrategy) {

    public ReOptimizationResult {
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(newJoinStrategy, "newJoinStrategy must not be null");
    }

    /** Convenience factory for no re-optimization needed. */
    public static ReOptimizationResult none() {
        return new ReOptimizationResult(false, "", Optional.empty());
    }
}
