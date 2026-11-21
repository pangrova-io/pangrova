package org.pangrova.shuffle.api.identity;

/** Monotonic retry/speculation fence within one physical plan epoch. */
public record ExchangeAttemptFence(
        long planEpoch,
        int attemptId,
        long fencingToken) {

    public ExchangeAttemptFence {
        if (planEpoch <= 0) {
            throw new IllegalArgumentException("planEpoch must be positive");
        }
        if (attemptId < 0) {
            throw new IllegalArgumentException("attemptId must not be negative");
        }
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
    }
}
