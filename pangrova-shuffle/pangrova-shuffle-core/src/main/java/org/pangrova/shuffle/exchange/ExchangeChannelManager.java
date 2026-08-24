package org.pangrova.shuffle.exchange;

import java.io.Closeable;

/**
 * SPI for managing data exchanges between stages in a distributed query.
 *
 * <p>Implementations must be thread-safe. Multiple threads may create sinks
 * and sources concurrently for different partitions of the same query.</p>
 *
 * @see ExchangeSink
 * @see ExchangeSource
 */
public interface ExchangeChannelManager extends Closeable {

    ExchangeSink createSink(String queryId, int partitionId);

    ExchangeSource createSource(String queryId, int partitionId, ExchangeReadMode readMode);

    default ExchangeSource createSource(String queryId, int partitionId) {
        return createSource(queryId, partitionId, ExchangeReadMode.BLOCKING);
    }
}
