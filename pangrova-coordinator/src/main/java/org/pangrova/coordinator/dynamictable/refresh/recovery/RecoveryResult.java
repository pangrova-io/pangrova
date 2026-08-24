package org.pangrova.coordinator.dynamictable.refresh.recovery;

/**
 * Immutable result of a Dynamic Table state recovery operation.
 *
 * @param recovered number of DTs successfully recovered and registered
 * @param skipped   number of DTs skipped (invalid checkpoint, etc.)
 * @param suspended number of DTs suspended due to schema incompatibility
 */
public record RecoveryResult(int recovered, int skipped, int suspended) {

    public RecoveryResult {
        if (recovered < 0) {
            throw new IllegalArgumentException("recovered must be non-negative");
        }
        if (skipped < 0) {
            throw new IllegalArgumentException("skipped must be non-negative");
        }
        if (suspended < 0) {
            throw new IllegalArgumentException("suspended must be non-negative");
        }
    }

    /** Total number of DTs processed during recovery. */
    public int total() {
        return recovered + skipped + suspended;
    }
}
