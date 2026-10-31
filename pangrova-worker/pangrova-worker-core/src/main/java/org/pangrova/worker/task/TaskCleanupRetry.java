package org.pangrova.worker.task;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** One exact release continuation, bounded by its tracked TaskInfo owner. */
final class TaskCleanupRetry {
    private final AtomicReference<Runnable> pending = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();

    void retain(Runnable continuation) {
        pending.compareAndSet(null, java.util.Objects.requireNonNull(continuation));
    }

    boolean retry() {
        if (!running.compareAndSet(false, true)) return false;
        try {
            Runnable action = pending.get();
            if (action == null) return false;
            action.run();
            pending.compareAndSet(action, null);
            return true;
        } finally {
            // A thrown real release leaves the exact action and owner retryable.
            running.set(false);
        }
    }
}
