package org.pangrova.worker.task;

import org.pangrova.worker.result.ResultTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.api.ShuffleTransport;

/**
 * Groups transport SPI references for TaskManager.
 * Extracted from {@link TaskManagerContext} to comply with the ≤4 param governance limit.
 *
 * <p>A null field denotes an unavailable capability. A task requiring that
 * transport must be rejected; an absent implementation is never a no-op.</p>
 *
 * @param shuffleTransport shuffle data transport SPI, or unavailable
 * @param resultTransport result streaming transport SPI, or unavailable
 * @param exchangeTransport exchange data transport SPI, or unavailable
 */
public record TransportRefs(
        ShuffleTransport shuffleTransport,
        ResultTransport resultTransport,
        ExchangeTransport exchangeTransport) {

    /** Returns true if all transport SPIs are wired. */
    public boolean isFullyWired() {
        return shuffleTransport != null && resultTransport != null && exchangeTransport != null;
    }
}
