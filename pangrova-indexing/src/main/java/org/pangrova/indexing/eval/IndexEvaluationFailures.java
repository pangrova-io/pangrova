package org.pangrova.indexing.eval;

import org.pangrova.spi.security.AccessDeniedException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Keeps query authority and cancellation failures out of conservative index pruning. */
public final class IndexEvaluationFailures {

    private IndexEvaluationFailures() {}

    /** Propagates terminal failures, unwrapping only standard asynchronous carriers. */
    public static void propagateIfTerminal(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof ExecutionException || cause instanceof CompletionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof AccessDeniedException denied) {
            throw denied;
        }
        if (cause instanceof SecurityException denied) {
            throw denied;
        }
        if (cause instanceof CancellationException cancelled) {
            throw cancelled;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            CancellationException cancelled = new CancellationException("Index evaluation interrupted");
            cancelled.initCause(cause);
            throw cancelled;
        }
        checkInterrupted();
    }

    /** Checks without clearing the caller's interrupt flag. */
    public static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Index evaluation interrupted");
        }
    }
}
