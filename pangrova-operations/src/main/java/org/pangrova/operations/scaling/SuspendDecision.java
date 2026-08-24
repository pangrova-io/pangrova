package org.pangrova.operations.scaling;

import java.time.Duration;
import java.util.Objects;

/**
 * Outcome of an {@link AutoSuspendController} evaluation: whether to suspend (scale the cluster
 * down to its idle floor) and how long the cluster has been continuously idle.
 *
 * @param suspend       true when sustained idleness warrants scaling down to {@code targetWorkers}
 * @param targetWorkers the worker count to apply (the idle floor when suspending, else unchanged)
 * @param idleFor       how long the cluster has been continuously idle as of this evaluation
 */
public record SuspendDecision(boolean suspend, int targetWorkers, Duration idleFor) {

    public SuspendDecision {
        Objects.requireNonNull(idleFor, "idleFor must not be null");
        if (targetWorkers < 0) {
            throw new IllegalArgumentException("targetWorkers must be >= 0, got " + targetWorkers);
        }
    }

    static SuspendDecision hold(int workers, Duration idleFor) {
        return new SuspendDecision(false, workers, idleFor);
    }

    static SuspendDecision suspend(int target, Duration idleFor) {
        return new SuspendDecision(true, target, idleFor);
    }
}
