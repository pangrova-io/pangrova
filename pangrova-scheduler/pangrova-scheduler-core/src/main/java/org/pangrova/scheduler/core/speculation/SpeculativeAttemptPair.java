package org.pangrova.scheduler.core.speculation;

import org.pangrova.common.concurrent.ThreadSafe;

/** Routing metadata only; the task registry remains the sole issued/drain authority. */
@ThreadSafe
final class SpeculativeAttemptPair {
    final String originalId;
    final String copyId;
    volatile boolean copyIssued;
    volatile boolean aborted;
    volatile boolean cleanupRequested;
    volatile boolean originalDrained;
    volatile boolean copyDrained;
    volatile boolean dispatching = true;
    final java.util.concurrent.atomic.AtomicBoolean retiring = new java.util.concurrent.atomic.AtomicBoolean();

    SpeculativeAttemptPair(String originalId, String copyId) {
        this.originalId = originalId;
        this.copyId = copyId;
    }

    boolean readyToRetire() {
        return !dispatching && cleanupRequested && (!copyIssued || copyDrained && (aborted || originalDrained));
    }
}
