package org.pangrova.ai.api.invocation;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Provider-owned local transport lifetime, registered with the engine before sending.
 * Implementations must support concurrent cancellation and sender finalization.
 * Local termination makes no claim about a remote POST outcome or its replay safety.
 */
@Experimental
public interface ClientExchange {
    /** Idempotently requests cancellation, including a request not yet submitted. */
    void cancel();

    /**
     * Joins actual local sender/body ownership after cancellation, not just a cancelled
     * future. Restores an interrupted caller's interrupt flag after joining. Calling
     * from the exchange's own active sender or delivery callback is rejected to avoid
     * self-deadlock. Engine callers must not hold budget locks while joining.
     */
    void awaitTermination();
}
