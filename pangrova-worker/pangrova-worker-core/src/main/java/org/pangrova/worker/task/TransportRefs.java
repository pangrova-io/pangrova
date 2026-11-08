package org.pangrova.worker.task;

import org.pangrova.worker.result.ResultTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.nativebridge.shuffle.NativeShuffleTransport;

/**
 * Groups Native serving and result/exchange transport references for TaskManager.
 *
 * <p>A null field denotes an unavailable capability. A task requiring that
 * transport must be rejected; an absent implementation is never a no-op.</p>
 *
 * @param shuffleTransport task-owned Native serving adapter, or unavailable
 * @param resultTransport result streaming transport SPI, or unavailable
 * @param exchangeTransport exchange data transport SPI, or unavailable
 */
public record TransportRefs(
        NativeShuffleTransport shuffleTransport,
        ResultTransport resultTransport,
        ExchangeTransport exchangeTransport) {

    /** Returns true if all required transports are wired. */
    public boolean isFullyWired() {
        return shuffleTransport != null && resultTransport != null && exchangeTransport != null;
    }
}
