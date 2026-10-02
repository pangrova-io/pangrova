package org.pangrova.worker.task;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Worker admission fence for the authenticated cluster-control term. */
final class ControlEpochFence {

    private static final long UNARMED = 0;

    private final AtomicBoolean required = new AtomicBoolean(false);
    private final AtomicLong controlEpoch = new AtomicLong(UNARMED);

    void requireHandshake() {
        required.set(true);
    }

    void observe(long epoch) {
        if (epoch <= 0) {
            throw new IllegalArgumentException("Control epoch must be positive: " + epoch);
        }
        controlEpoch.accumulateAndGet(epoch, Math::max);
    }

    void verify(Long taskEpoch) {
        if (!required.get()) {
            return;
        }
        long expected = controlEpoch.get();
        if (expected == UNARMED) {
            throw rejected(ControlEpochFenceException.Reason.CONTROL_PLANE_NOT_READY,
                    "Worker has not completed a control-epoch handshake");
        }
        if (taskEpoch == null) {
            throw rejected(ControlEpochFenceException.Reason.MISSING_EPOCH,
                    "Task submission is missing control_epoch; expected " + expected);
        }
        if (taskEpoch <= 0) {
            throw rejected(ControlEpochFenceException.Reason.INVALID_EPOCH,
                    "Task control_epoch must be positive: " + taskEpoch);
        }
        if (taskEpoch != expected) {
            throw mismatch(taskEpoch, expected);
        }
        long confirmed = controlEpoch.get();
        if (taskEpoch != confirmed) {
            throw mismatch(taskEpoch, confirmed);
        }
    }

    OptionalLong currentEpoch() {
        long epoch = controlEpoch.get();
        return epoch == UNARMED ? OptionalLong.empty() : OptionalLong.of(epoch);
    }

    private static ControlEpochFenceException mismatch(long actual, long expected) {
        ControlEpochFenceException.Reason reason = actual > expected
                ? ControlEpochFenceException.Reason.EPOCH_AHEAD
                : ControlEpochFenceException.Reason.EPOCH_MISMATCH;
        return rejected(reason,
                "Task control_epoch " + actual
                        + " does not match Worker control-plane epoch " + expected);
    }

    private static ControlEpochFenceException rejected(
            ControlEpochFenceException.Reason reason, String message) {
        return new ControlEpochFenceException(reason, message);
    }
}
