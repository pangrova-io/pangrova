package org.pangrova.shuffle.exchange;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/**
 * Control-plane SPI for atomically binding Native exchange inputs.
 *
 * @param <A> admitted execution attempt type
 * @param <I> admitted exchange input type
 */
@Internal
public interface ExchangeTransport<A, I> {

    /**
     * Registers every source and seals each input before Native task start.
     * Implementations must fail closed on duplicates, stale attempts, missing
     * tickets, or source-count mismatches.
     */
    void bindInputs(
            long executorHandle,
            A consumerAttempt,
            List<I> inputs);

    /** Cancels task-scoped input work and releases provider calls. */
    void releaseInputs(long executorHandle);
}
