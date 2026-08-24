package org.pangrova.coordinator.query.execution.dispatch.dag;

/**
 * Immutable spill budget configuration propagated from coordinator to workers.
 *
 * @param maxSpillBytes  maximum bytes a task may spill to disk (0 = spill disabled)
 * @param spillDirectory directory path for spill files (empty = spill disabled)
 */
public record SpillBudget(long maxSpillBytes, String spillDirectory) {

    public SpillBudget {
        if (maxSpillBytes < 0) {
            throw new IllegalArgumentException("maxSpillBytes must be >= 0, got " + maxSpillBytes);
        }
        if (spillDirectory == null) {
            spillDirectory = "";
        }
    }
}
