package org.pangrova.spi.resource.memory;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * The outcome of a {@link MemoryBudgetEnforcer#enforce} call.
 *
 * @param action       the recommended enforcement action
 * @param targetDomain the memory domain to act on (may be null if action is NONE)
 * @param reason       human-readable justification for observability
 */
@Experimental(reason = "G-RES-07: unified memory budget enforcement SPI")
public record EnforcementDecision(
        EnforcementAction action,
        MemoryDomain targetDomain,
        String reason) {

    public EnforcementDecision {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        // targetDomain may be null when action is NONE
    }

    /** Convenience factory for no-action decision. */
    public static EnforcementDecision none() {
        return new EnforcementDecision(EnforcementAction.NONE, null, "within budget");
    }

    /** Convenience factory for a spill decision. */
    public static EnforcementDecision spill(MemoryDomain domain, String reason) {
        return new EnforcementDecision(EnforcementAction.SPILL, domain, reason);
    }

    /** Convenience factory for a shrink-native decision. */
    public static EnforcementDecision shrinkNative(String reason) {
        return new EnforcementDecision(
                EnforcementAction.SHRINK_NATIVE, MemoryDomain.NATIVE, reason);
    }

    /** Convenience factory for a kill-largest decision. */
    public static EnforcementDecision killLargest(MemoryDomain domain, String reason) {
        return new EnforcementDecision(EnforcementAction.KILL_LARGEST, domain, reason);
    }
}
