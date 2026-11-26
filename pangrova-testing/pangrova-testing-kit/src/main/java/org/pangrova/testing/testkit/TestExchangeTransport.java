package org.pangrova.testing.testkit;

import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;

import java.util.List;

/**
 * No-op exchange transport for tests that don't exercise the exchange data plane.
 * Use {@link #INSTANCE} to avoid creating new instances in every test.
 */
public final class TestExchangeTransport implements ExchangeTransport<TaskAttemptId, ExchangeInputBinding> {

    public static final ExchangeTransport<TaskAttemptId, ExchangeInputBinding> INSTANCE = new TestExchangeTransport();

    private TestExchangeTransport() {}

    @Override
    public void bindInputs(
            long executorHandle,
            TaskAttemptId consumerAttempt,
            List<ExchangeInputBinding> inputs) {
        // no-op
    }

    @Override
    public void releaseInputs(long executorHandle) {
        // no-op
    }
}
