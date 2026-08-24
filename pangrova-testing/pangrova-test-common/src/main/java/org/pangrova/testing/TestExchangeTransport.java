package org.pangrova.testing;

import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeTransport;

import java.util.List;

/**
 * No-op exchange transport for tests that don't exercise the exchange data plane.
 * Use {@link #INSTANCE} to avoid creating new instances in every test.
 */
public final class TestExchangeTransport implements ExchangeTransport {

    public static final ExchangeTransport INSTANCE = new TestExchangeTransport();

    private TestExchangeTransport() {}

    @Override
    public void startExchange(long executorHandle, List<ExchangeEndpoint> sources) {
        // no-op
    }

    @Override
    public void addSources(long executorHandle, List<ExchangeEndpoint> newSources) {
        // no-op
    }

    @Override
    public void noMoreSources(long executorHandle) {
        // no-op
    }
}
