package org.pangrova.worker.task;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker-side zombie-coordinator fencing for the SI-6 ownership epoch.
 *
 * <p>Each {@code SubmitTaskRequest} carries the stamping coordinator's monotonic
 * ownership epoch. After a failover the new leader advances the epoch; a deposed
 * (zombie) coordinator that keeps issuing instructions still stamps its old, lower
 * epoch. This fence tracks the highest epoch seen and rejects any later instruction
 * carrying a strictly lower epoch, so a zombie can no longer mutate worker state.
 *
 * <p>Epoch {@code 0} is the unstamped sentinel (no failover task has run, or a legacy
 * dispatch path): it is always accepted and never advances the fence, keeping the
 * fence inert until real epochs flow.
 */
public final class TaskEpochFence {

    private final AtomicLong maxSeenEpoch = new AtomicLong(0L);

    /**
     * Validates an incoming instruction's ownership epoch against the highest seen,
     * advancing the fence when the epoch is fresh.
     *
     * @param epoch the request's ownership epoch
     * @return {@code true} if the instruction is accepted; {@code false} if it is
     *         stale (a lower epoch than already seen) and must be rejected
     */
    public boolean accept(long epoch) {
        if (epoch <= 0L) {
            return true;
        }
        long observed;
        do {
            observed = maxSeenEpoch.get();
            if (epoch < observed) {
                return false;
            }
            if (epoch == observed) {
                return true;
            }
        } while (!maxSeenEpoch.compareAndSet(observed, epoch));
        return true;
    }

    /** Returns the highest ownership epoch observed so far (0 if none stamped). */
    public long currentEpoch() {
        return maxSeenEpoch.get();
    }
}
