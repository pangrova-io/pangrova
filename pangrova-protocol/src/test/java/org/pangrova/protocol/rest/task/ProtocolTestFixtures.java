package org.pangrova.protocol.rest.task;

import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;

/** Canonical protocol fixtures used by REST DTO tests. */
public final class ProtocolTestFixtures {

    private static final long TEST_TICKET_EXPIRY_EPOCH_MS = 4_102_444_800_000L;

    private ProtocolTestFixtures() {}

    public static ExchangeSourceDescriptor exchangeSource(
            String sourceTaskId, String host, int transportPort,
            int partitionId, String readMode, long planEpoch) {
        long normalizedEpoch = Math.max(1L, planEpoch);
        return new ExchangeSourceDescriptor(
                "exchange-node", "exchange-1", 1,
                sourceTaskId, 1, partitionId, partitionId,
                normalizedEpoch, 0, (normalizedEpoch << 32) + 1,
                host, transportPort, "", "dGlja2V0",
                TEST_TICKET_EXPIRY_EPOCH_MS, 0, 0, readMode);
    }
}
