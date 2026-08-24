package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Set;

/**
 * Classifies task execution failures as transient (retryable) or permanent.
 *
 * <p>Transient failures include network timeouts, connection resets, and
 * OOM after spill (indicating temporary resource pressure). Permanent
 * failures include logic errors, unsupported operations, and cancellations.</p>
 *
 * <p>Concurrency strategy: stateless — all methods are pure functions.</p>
 */
@ThreadSafe
public final class TransientFailureClassifier {

    private static final Set<String> TRANSIENT_ERROR_PATTERNS = Set.of(
            "connection reset",
            "connection refused",
            "broken pipe",
            "network timeout",
            "read timed out",
            "no route to host",
            "temporary failure in name resolution"
    );

    private static final Set<String> OOM_AFTER_SPILL_PATTERNS = Set.of(
            "memory pool exhausted after spill",
            "failed to allocate after spill",
            "spill failed: disk full"
    );

    /** Maximum cause-chain depth inspected, to prevent infinite loops. */
    private static final int MAX_CAUSE_DEPTH = 5;

    private TransientFailureClassifier() {}

    /**
     * Returns true if the failure is transient and the task should be retried.
     */
    public static boolean isTransient(Exception failure) {
        if (failure == null) {
            return false;
        }
        if (isNetworkFailure(failure)) {
            return true;
        }
        if (isOomAfterSpill(failure)) {
            return true;
        }
        // Check cause chain (max depth 5 to prevent infinite loops)
        Throwable cause = failure.getCause();
        int depth = 0;
        while (cause != null && depth < MAX_CAUSE_DEPTH) {
            if (isTransientSignal(cause)) {
                return true;
            }
            cause = cause.getCause();
            depth++;
        }
        return false;
    }

    /** Returns true if a single cause-chain element carries a transient failure signal. */
    private static boolean isTransientSignal(Throwable cause) {
        return cause instanceof ConnectException
                || cause instanceof SocketTimeoutException
                || matchesTransientPattern(cause.getMessage());
    }

    private static boolean isNetworkFailure(Exception failure) {
        if (failure instanceof ConnectException || failure instanceof SocketTimeoutException) {
            return true;
        }
        return matchesTransientPattern(failure.getMessage());
    }

    private static boolean isOomAfterSpill(Exception failure) {
        String message = failure.getMessage();
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        for (String pattern : OOM_AFTER_SPILL_PATTERNS) {
            if (lower.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesTransientPattern(String message) {
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        for (String pattern : TRANSIENT_ERROR_PATTERNS) {
            if (lower.contains(pattern)) {
                return true;
            }
        }
        return false;
    }
}
