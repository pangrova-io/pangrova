package org.pangrova.shuffle.exchange;

import org.pangrova.spi.annotation.Internal;

import java.util.List;

/** Control-plane SPI for atomically binding Native ExchangeInput nodes. */
@Internal
public interface ExchangeTransport {

    /**
     * Registers every source and seals each input before Native task start.
     * Implementations must fail closed on duplicates, stale attempts, missing
     * tickets, or source-count mismatches.
     */
    void bindInputs(
            long executorHandle,
            ExchangeTaskAttempt consumerAttempt,
            List<ExchangeInput> inputs);

    /** Cancels task-scoped input work and releases provider calls. */
    void releaseInputs(long executorHandle);
}
