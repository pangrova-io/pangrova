package org.pangrova.query.spi.lifecycle;

import java.time.Duration;
import java.time.Instant;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Query owner's timeout entry point, independent of scheduling and transport.
 *
 * <p>The deadline is inclusive: only nonterminal queries created at or before it
 * are eligible. The owner must arbitrate against concurrent completion/cancellation
 * using its existing state machine. It must retain the exact observed query lifetime,
 * never resolve a replacement lifetime by query ID to apply the decision.</p>
 *
 * <p>A winning timeout uses the ordinary journal, audit, cancellation and cleanup
 * path. Logical timeout is not evidence of physical drain and must not release
 * live operation handles or reader pins. Repeated sweeps must not repeat terminal
 * effects. Implementations must tolerate concurrent query changes and propagate
 * failures; they must not replace failed persistence with another backend.</p>
 */
@Experimental
@ThreadSafe
@FunctionalInterface
public interface QueryTimeoutHandler {
    /**
     * Attempts terminal timeout transitions for eligible queries owned by this process.
     * The duration is retained for diagnostics; the supplied deadline determines eligibility.
     */
    void timeoutQueriesCreatedBy(Instant deadline, Duration timeout);
}
