package org.pangrova.shuffle;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.shuffle.exchange.ExchangeChannelManager;

/**
 * A cohesive shuffle implementation: one provider owns both the control plane
 * ({@link ShuffleLifecycleManager}) and the data plane ({@link ExchangeChannelManager}), and declares
 * the fault-tolerance guarantees it backs (ADR-002 D1).
 *
 * <p>Built by a {@link ShuffleManagerFactory} discovered via {@code ServiceLoader}; selected by name
 * ({@code shuffle.manager = native | local-spill | object-store | <plugin>}) rather than a
 * closed enum.
 */
@Experimental(reason = "ADR-002 D1 pluggable shuffle SPI; evolving alongside D4 providers")
public interface ShuffleProvider extends AutoCloseable {

    /** Control plane: shuffle registration, mapper/stage finalize, cleanup. */
    ShuffleLifecycleManager lifecycleManager();

    /** Data plane: sink/source channel creation. */
    ExchangeChannelManager exchangeManager();

    /** Durability / FTE guarantees this implementation backs. */
    ShuffleCapabilities capabilities();
}
