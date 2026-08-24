package org.pangrova.shuffle.exchange;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Describes an upstream exchange endpoint for data retrieval by {@link ExchangeTransport}.
 *
 * <p>This is the SPI-level exchange endpoint descriptor. Worker-internal
 * {@code org.pangrova.worker.exchange.ExchangeEndpoint} adds transport-specific
 * fields (exchangeType, sequenceId) and is mapped from this SPI type at the
 * worker boundary.</p>
 *
 * @param sourceTaskId  the upstream task identifier producing exchange data
 * @param host          the upstream worker hostname
 * @param transportPort the upstream worker transport port (must be positive)
 * @param partitionId   the exchange partition identifier
 */
public record ExchangeEndpoint(
        String sourceTaskId,
        String host,
        int transportPort,
        int partitionId) {

    public ExchangeEndpoint {
        checkNotNull(sourceTaskId, "sourceTaskId must not be null");
        checkNotNull(host, "host must not be null");
        if (transportPort <= 0) {
            throw new IllegalArgumentException("transportPort must be positive, got: " + transportPort);
        }
    }
}
