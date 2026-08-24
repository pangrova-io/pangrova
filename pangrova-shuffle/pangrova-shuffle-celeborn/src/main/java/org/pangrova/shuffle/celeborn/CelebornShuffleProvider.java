/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.shuffle.celeborn;

import java.util.Objects;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleProvider;
import org.pangrova.shuffle.exchange.ExchangeChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Celeborn {@link ShuffleProvider} implementation (ADR-002 D4).
 *
 * <p>Combines the Celeborn lifecycle manager (control plane) and exchange channel manager
 * (data plane) into a cohesive provider with DURABLE / PERSISTENT / PUSH capabilities.
 *
 * <p>Lifecycle: closing this provider shuts down the underlying Celeborn client connections,
 * unregistering all active shuffles. The close order is: lifecycle manager → exchange manager
 * → client wrapper → ID resolver.
 */
public final class CelebornShuffleProvider implements ShuffleProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornShuffleProvider.class);

    private final CelebornLifecycleManager lifecycleManager;
    private final CelebornExchangeChannelManager exchangeManager;
    private final ShuffleCapabilities capabilities;
    private final CelebornClientWrapper clientWrapper;
    private final CelebornShuffleIdResolver idResolver;

    /**
     * Creates a provider from the fully wired Celeborn components.
     *
     * @param lifecycleManager shuffle lifecycle manager
     * @param exchangeManager exchange data-plane manager
     * @param capabilities provider capabilities
     * @param clientWrapper Celeborn client connection owner
     * @param idResolver query and stage identifier resolver
     */
    public CelebornShuffleProvider(CelebornLifecycleManager lifecycleManager,
                                   CelebornExchangeChannelManager exchangeManager,
                                   ShuffleCapabilities capabilities,
                                   CelebornClientWrapper clientWrapper,
                                   CelebornShuffleIdResolver idResolver) {
        this.lifecycleManager = Objects.requireNonNull(lifecycleManager, "lifecycleManager");
        this.exchangeManager = Objects.requireNonNull(exchangeManager, "exchangeManager");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.clientWrapper = Objects.requireNonNull(clientWrapper, "clientWrapper");
        this.idResolver = Objects.requireNonNull(idResolver, "idResolver");
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

    /**
     * Returns the byte egress instance for native→JVM data path integration.
     * This is used by the worker's egress registry to route committed partition bytes
     * to Celeborn instead of the default SpoolStore path.
     */
    public CelebornByteEgress byteEgress() {
        return new CelebornByteEgress(clientWrapper, idResolver);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing CelebornShuffleProvider");
        try {
            lifecycleManager.close();
        } catch (Exception e) {
            LOG.warn("Error closing lifecycle manager: {}", e.getMessage());
        }
        try {
            exchangeManager.close();
        } catch (Exception e) {
            LOG.warn("Error closing exchange manager: {}", e.getMessage());
        }
        try {
            clientWrapper.close();
        } catch (Exception e) {
            LOG.warn("Error closing client wrapper: {}", e.getMessage());
        }
        idResolver.clear();
    }
}
