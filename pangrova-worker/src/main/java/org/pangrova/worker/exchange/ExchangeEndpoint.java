package org.pangrova.worker.exchange;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Describes an upstream exchange endpoint for shuffle data retrieval.
 */
public record ExchangeEndpoint(
        String sourceTaskId,
        String host,
        int transportPort,
        int partitionId,
        ExchangeType exchangeType,
        long sequenceId) {

    public ExchangeEndpoint {
        checkNotNull(sourceTaskId, "sourceTaskId must not be null");
        checkNotNull(host, "host must not be null");
        checkNotNull(exchangeType, "exchangeType must not be null");
    }
}
