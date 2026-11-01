package org.pangrova.shuffle.api.exchange;

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

    ExchangeSink createSink(ExchangeChannelContext context);

    ExchangeSource createSource(ExchangeChannelContext context, ExchangeReadMode readMode);

    default ExchangeSource createSource(ExchangeChannelContext context) {
        return createSource(context, ExchangeReadMode.BLOCKING);
    }
}
