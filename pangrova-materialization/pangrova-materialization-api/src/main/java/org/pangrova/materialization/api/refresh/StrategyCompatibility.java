package org.pangrova.materialization.api.refresh;

/**
 * Result of validating whether a refresh strategy applies.
 *
 * @param compatible whether the strategy can execute the definition
 * @param reason explanation; empty when compatible
 */
public record StrategyCompatibility(boolean compatible, String reason) {

    public StrategyCompatibility {
        reason = reason == null ? "" : reason;
        if (!compatible && reason.isBlank()) {
            throw new IllegalArgumentException("an incompatible decision requires a reason");
        }
    }

    /**
     * Returns a compatible decision.
     *
     * @return compatible decision
     */
    public static StrategyCompatibility supported() {
        return new StrategyCompatibility(true, "");
    }

    /**
     * Returns an incompatible decision.
     *
     * @param reason why the strategy does not apply
     * @return incompatible decision
     */
    public static StrategyCompatibility incompatible(String reason) {
        return new StrategyCompatibility(false, reason);
    }
}
