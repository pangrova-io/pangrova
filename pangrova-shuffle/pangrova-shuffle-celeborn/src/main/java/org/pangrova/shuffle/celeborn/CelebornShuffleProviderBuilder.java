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

import java.util.Map;
import java.util.function.Function;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleProvider;
import org.pangrova.shuffle.ShuffleRetentionPolicy;
import org.pangrova.shuffle.ShuffleTransportMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotBlank;

/**
 * Builds a fully-wired Celeborn {@link ShuffleProvider} from a {@link ShuffleContext} (ADR-002 D4).
 *
 * <p>This class is the entry point loaded by {@code CelebornShuffleManagerFactory} via reflection.
 * It assembles the complete Celeborn provider stack:
 * <ol>
 *   <li>{@link CelebornClientWrapper} — Celeborn client connection management</li>
 *   <li>{@link CelebornShuffleIdResolver} — queryId→shuffleId mapping</li>
 *   <li>{@link CelebornLifecycleManager} — shuffle registration/commit/cleanup</li>
 *   <li>{@link CelebornExchangeChannelManager} — data plane sink/source</li>
 *   <li>{@link CelebornByteEgress} — native→JVM byte egress via Celeborn push</li>
 * </ol>
 *
 * <p>Configuration properties (from {@code ShuffleContext}):
 * <ul>
 *   <li>{@code celeborn.master.endpoints} — Celeborn master endpoints (required;
 *       there is deliberately no default, see CHANGELOG)</li>
 *   <li>{@code celeborn.*} — any additional Celeborn configuration properties</li>
 * </ul>
 */
public final class CelebornShuffleProviderBuilder implements Function<ShuffleContext, ShuffleProvider> {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornShuffleProviderBuilder.class);

    @Override
    public ShuffleProvider apply(ShuffleContext context) {
        LOG.info("Building Celeborn shuffle provider from context: {}", context.managerName());

        // Parse master endpoint configuration. There is deliberately no default:
        // silently falling back to localhost:9097 turned a misconfiguration into a
        // connection timeout far from its cause.
        String masterEndpointsProperty = "celeborn.master.endpoints";
        String masterEndpoints = checkNotBlank(
                context.getOrDefault(masterEndpointsProperty, System.getProperty(masterEndpointsProperty)),
                masterEndpointsProperty + " must be configured, e.g. \"host1:9097\" "
                        + "or \"host1:9097,host2:9097\"");

        String[] parts = masterEndpoints.split(":");
        String masterHost = parts[0];
        String masterPortValue = parts.length > 1 ? parts[1] : "9097";
        int masterPort = Integer.parseInt(masterPortValue);

        // Collect all celeborn.* properties
        Map<String, String> celebornProps = context.properties();

        // Build the component stack
        CelebornClientWrapper clientWrapper = new CelebornClientWrapper(masterHost, masterPort, celebornProps);
        CelebornShuffleIdResolver idResolver = new CelebornShuffleIdResolver();

        CelebornLifecycleManager lifecycleManager = new CelebornLifecycleManager(clientWrapper, idResolver);
        CelebornExchangeChannelManager exchangeManager =
                new CelebornExchangeChannelManager(clientWrapper, idResolver);

        // Capabilities: DURABLE durability, PERSISTENT retention, PUSH transport
        ShuffleCapabilities capabilities = ShuffleCapabilities.of(
                ShuffleDurability.DURABLE,
                ShuffleRetentionPolicy.PERSISTENT,
                ShuffleTransportMode.PUSH);

        LOG.info("Celeborn shuffle provider built: master={}:{}, durability=DURABLE, transport=PUSH",
                masterHost, masterPort);

        return new CelebornShuffleProvider(
                lifecycleManager, exchangeManager, capabilities, clientWrapper, idResolver);
    }
}
