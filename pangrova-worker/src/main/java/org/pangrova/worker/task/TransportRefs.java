package org.pangrova.worker.task;

import java.util.List;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.spi.execution.ResultTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.ShuffleTransport;

/**
 * Groups transport SPI references for TaskManager.
 * Extracted from {@link TaskManagerContext} to comply with the ≤4 param governance limit.
 *
 * <p>All fields are nullable — when null, the transport capability is unavailable
 * and the worker operates in degraded mode (no shuffle/result/exchange).</p>
 *
 * @param shuffleTransport  shuffle data transport SPI (nullable — fallback to legacy wirer)
 * @param resultTransport   result streaming transport SPI (nullable — fallback to legacy wirer)
 * @param exchangeTransport exchange data transport SPI (nullable — fallback to legacy wirer)
 */
public record TransportRefs(
        ShuffleTransport shuffleTransport,
        ResultTransport resultTransport,
        ExchangeTransport exchangeTransport) {

    /**
     * Empty transport refs with no-op implementations.
     * Used by legacy TaskManagerContext constructor and test configurations.
     */
    public static TransportRefs empty() {
        ExchangeTransport noOpExchange = new ExchangeTransport() {
            @Override public void startExchange(long h, List<ExchangeEndpoint> s) {}
            @Override public void addSources(long h, List<ExchangeEndpoint> s) {}
            @Override public void noMoreSources(long h) {}
        };
        ResultTransport noOpResult = new ResultTransport() {
            @Override public int startServer(int preferredPort) { return preferredPort; }
            @Override public void stopServer() {}
            @Override public int boundPort() { return -1; }
            @Override public void close() {}
        };
        return new TransportRefs(null, noOpResult, noOpExchange);
    }

    /** Returns true if all transport SPIs are wired. */
    public boolean isFullyWired() {
        return shuffleTransport != null && resultTransport != null && exchangeTransport != null;
    }
}
