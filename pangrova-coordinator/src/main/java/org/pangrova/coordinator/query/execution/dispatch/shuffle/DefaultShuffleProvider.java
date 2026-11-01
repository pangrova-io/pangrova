package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.api.capability.ShuffleCapabilities;
import org.pangrova.shuffle.api.contract.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.contract.ShuffleProvider;

/**
 * Simple {@link ShuffleProvider} that holds a fixed control plane, data plane, and capability set
 * (ADR-002 D1).
 *
 * <p>Either plane may be {@code null}: the coordinator only consumes {@link #lifecycleManager()},
 * while the worker only consumes {@link #exchangeManager()}, so a provider built on one side leaves
 * the other plane unset.
 */
public final class DefaultShuffleProvider implements ShuffleProvider {

    private final ShuffleLifecycleManager lifecycleManager;
    private final ExchangeChannelManager exchangeManager;
    private final ShuffleCapabilities capabilities;

    /** Creates a shuffle provider backed by the lifecycle and exchange managers. */
    public DefaultShuffleProvider(
            ShuffleLifecycleManager lifecycleManager,
            ExchangeChannelManager exchangeManager,
            ShuffleCapabilities capabilities) {
        if (capabilities == null) {
            throw new IllegalArgumentException("capabilities must not be null");
        }
        this.lifecycleManager = lifecycleManager;
        this.exchangeManager = exchangeManager;
        this.capabilities = capabilities;
    }

    @Override
    public ShuffleLifecycleManager lifecycleManager() {
        return lifecycleManager;
    }

    @Override
    public ExchangeChannelManager exchangeManager() {
        return exchangeManager;
    }

    @Override
    public ShuffleCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public void close() throws Exception {
        if (lifecycleManager instanceof AutoCloseable closeable) {
            closeable.close();
        }
        if (exchangeManager != null) {
            exchangeManager.close();
        }
    }
}
