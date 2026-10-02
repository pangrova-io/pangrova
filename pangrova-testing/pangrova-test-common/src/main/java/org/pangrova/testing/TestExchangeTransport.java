package org.pangrova.testing;

import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
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
    public void bindInputs(
            long executorHandle,
            ExchangeTaskAttempt consumerAttempt,
            List<ExchangeInput> inputs) {
        // no-op
    }

    @Override
    public void releaseInputs(long executorHandle) {
        // no-op
    }
}
