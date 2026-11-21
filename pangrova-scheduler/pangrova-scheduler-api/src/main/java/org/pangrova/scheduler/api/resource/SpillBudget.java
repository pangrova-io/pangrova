package org.pangrova.scheduler.api.resource;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable per-task spill byte grant. Zero disables task spilling.
 *
 * <p>A positive grant permits spilling only when the Worker enables its local spill policy.
 * The Worker owns the directory and may enforce a smaller positive node limit. This value
 * neither selects a remote filesystem path nor reserves disk space.</p>
 *
 * @param maxSpillBytes maximum bytes one task may spill; non-negative, zero disables spilling
 */
@Experimental(reason = "Per-task spill grant; Worker-local policy determines the effective limit")
public record SpillBudget(long maxSpillBytes) {
    public static final SpillBudget DISABLED = new SpillBudget(0);

    public SpillBudget {
        if (maxSpillBytes < 0) {
            throw new IllegalArgumentException("maxSpillBytes must be >= 0, got " + maxSpillBytes);
        }
    }
}
