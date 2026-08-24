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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Celeborn implementation of {@link ShuffleLifecycleManager} (ADR-002 D4).
 *
 * <p>Maps Pangrova shuffle lifecycle operations to Celeborn's LifecycleManager:
 * <ul>
 *   <li>{@code registerShuffle} → Celeborn {@code registerShuffle(shuffleId, numPartitions)}</li>
 *   <li>{@code commitShuffle} → Celeborn {@code mapperEnd()} + {@code stageEnd()}</li>
 *   <li>{@code cleanupShuffle} → Celeborn {@code unregisterShuffle(shuffleId)}</li>
 * </ul>
 *
 * <p>Thread safety: all public methods are safe for concurrent use. The internal state is managed
 * via ConcurrentHashMap.
 */
public final class CelebornLifecycleManager implements ShuffleLifecycleManager, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornLifecycleManager.class);

    private final CelebornClientWrapper clientWrapper;
    private final CelebornShuffleIdResolver idResolver;

    /**
     * Maps queryId → shuffleId assigned by Celeborn.
     * A stage's shuffle is keyed as "{queryId}_{stageId}" internally.
     */
    private final ConcurrentMap<String, Integer> shuffleIdRegistry = new ConcurrentHashMap<>();

    /**
     * Tracks registration metadata for commit/cleanup operations.
     */
    private final ConcurrentMap<String, ShuffleRegistration> registrationCache = new ConcurrentHashMap<>();

    public CelebornLifecycleManager(CelebornClientWrapper clientWrapper,
                                     CelebornShuffleIdResolver idResolver) {
        this.clientWrapper = Objects.requireNonNull(clientWrapper, "clientWrapper");
        this.idResolver = Objects.requireNonNull(idResolver, "idResolver");
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        String key = shuffleKey(registration.queryId(), registration.stageId());

        LOG.info("Registering Celeborn shuffle: queryId={}, stageId={}, numPartitions={}",
                registration.queryId(), registration.stageId(), registration.numPartitions());

        int shuffleId = clientWrapper.registerShuffle(
                registration.queryId(),
                registration.stageId(),
                registration.numPartitions());

        shuffleIdRegistry.put(key, shuffleId);
        registrationCache.put(key, registration);

        // Publish to the shared ID resolver so that data-plane components
        // (ExchangeChannelManager, ByteEgress) can resolve shuffleId for operations.
        idResolver.register(registration.queryId(), registration.stageId(), shuffleId);

        // Celeborn Master assigns the shuffle; endpoint info is managed by the LifecycleManager
        // internally. Return the shuffle ID and master endpoint.
        ShuffleServerEndpoint masterEndpoint = new ShuffleServerEndpoint(
                clientWrapper.masterHost(), clientWrapper.masterPort());

        LOG.debug("Celeborn shuffle registered: key={}, shuffleId={}", key, shuffleId);
        return new ShuffleHandle(String.valueOf(shuffleId), List.of(masterEndpoint));
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        String key = shuffleKey(queryId, stageId);
        Integer shuffleId = shuffleIdRegistry.get(key);
        if (shuffleId == null) {
            LOG.warn("No Celeborn shuffle registered for commit: queryId={}, stageId={}", queryId, stageId);
            return ShuffleCommitResult.EMPTY;
        }

        ShuffleRegistration registration = registrationCache.get(key);
        int numMappers = registration != null ? registration.numPartitions() : 1;

        LOG.info("Committing Celeborn shuffle: shuffleId={}, queryId={}, stageId={}",
                shuffleId, queryId, stageId);

        // Signal all mappers have ended and finalize the stage.
        CelebornClientWrapper.CommitResult result = clientWrapper.commitShuffle(shuffleId, numMappers);

        // Convert Celeborn partition statistics to Pangrova format.
        Map<Integer, ShufflePartitionStats> partitionStats = new ConcurrentHashMap<>();
        if (result.partitionBytes() != null) {
            for (Map.Entry<Integer, Long> entry : result.partitionBytes().entrySet()) {
                long rows = result.partitionRows() != null
                        ? result.partitionRows().getOrDefault(entry.getKey(), 0L) : 0L;
                partitionStats.put(entry.getKey(),
                        new ShufflePartitionStats(entry.getKey(), entry.getValue(), rows));
            }
        }

        return new ShuffleCommitResult(partitionStats, result.totalBytes(), result.totalRows());
    }

    @Override
    public void cleanupShuffle(String queryId) {
        LOG.info("Cleaning up Celeborn shuffles for queryId={}", queryId);

        // Find all stages registered for this query and unregister them.
        shuffleIdRegistry.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(queryId + "_")) {
                int shuffleId = entry.getValue();
                try {
                    clientWrapper.unregisterShuffle(shuffleId);
                    LOG.debug("Unregistered Celeborn shuffle: shuffleId={}", shuffleId);
                } catch (Exception e) {
                    LOG.warn("Failed to unregister Celeborn shuffle {}: {}", shuffleId, e.getMessage());
                }
                registrationCache.remove(entry.getKey());
                return true;
            }
            return false;
        });

        // Clean up the shared ID resolver to prevent stale lookups and memory leaks.
        idResolver.removeQuery(queryId);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing CelebornLifecycleManager, cleaning up {} registered shuffles",
                shuffleIdRegistry.size());
        shuffleIdRegistry.forEach((key, shuffleId) -> {
            try {
                clientWrapper.unregisterShuffle(shuffleId);
            } catch (Exception e) {
                LOG.debug("Cleanup unregister failed for shuffle {}: {}", shuffleId, e.getMessage());
            }
        });
        shuffleIdRegistry.clear();
        registrationCache.clear();
    }

    private static String shuffleKey(String queryId, int stageId) {
        return queryId + "_" + stageId;
    }
}
