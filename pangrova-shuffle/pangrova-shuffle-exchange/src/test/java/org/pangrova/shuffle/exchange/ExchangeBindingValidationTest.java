package org.pangrova.shuffle.exchange;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class ExchangeBindingValidationTest {

    @Test
    void endpointRejectsMissingPlanEpoch() {
        assertThatThrownBy(() -> endpoint(0L, (1L << 32) + 1L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("planEpoch");
    }

    @Test
    void endpointRejectsMissingFence() {
        assertThatThrownBy(() -> endpoint(1L, 0L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fencingToken");
    }

    @Test
    void inputRejectsDuplicateLogicalProducerAcrossAttempts() {
        ExchangeEndpoint first = endpoint(1L, (1L << 32) + 1L, 0);
        ExchangeEndpoint retry = endpoint(1L, (1L << 32) + 2L, 1);

        assertThatThrownBy(() -> new ExchangeInput(
                "exchange-input-1", "exchange-1", 2,
                new DirectExchangeTransport(List.of(first, retry))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate logical producer");
    }

    private static ExchangeEndpoint endpoint(
            long planEpoch, long fencingToken, int attemptId) {
        return new ExchangeEndpoint(
                "task-" + attemptId, 3, 7, 0,
                planEpoch, attemptId, fencingToken,
                "worker", 9090, "", new byte[] {1},
                System.currentTimeMillis() + 60_000L, 0L, 0L);
    }
}
