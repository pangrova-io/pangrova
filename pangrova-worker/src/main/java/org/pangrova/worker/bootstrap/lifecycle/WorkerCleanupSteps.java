package org.pangrova.worker.bootstrap.lifecycle;

import org.pangrova.common.concurrent.NotThreadSafe;

/** Attempts independent bootstrap cleanup steps and retains their actual failures. */
@NotThreadSafe
public final class WorkerCleanupSteps {
    private Throwable failure;

    /** Runs a step even if a previous independent step failed. */
    public void attempt(Runnable close) {
        try { close.run(); }
        catch (RuntimeException | Error closeFailure) {
            if (failure == null) failure = closeFailure;
            else if (failure != closeFailure) failure.addSuppressed(closeFailure);
        }
    }

    /** Prevents entering a dependent cleanup phase after an earlier failure. */
    public void requireSuccess() {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }
}
