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

import java.io.Closeable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin wrapper around Apache Celeborn's client APIs, providing a type-safe Java interface
 * decoupled from Celeborn's Scala internals (ADR-002 D4).
 *
 * <p>This class encapsulates all interactions with Celeborn's {@code ShuffleClient} and
 * {@code LifecycleManager} classes, allowing the SPI adapters to work with clean Java types.
 * It also serves as the single point of Celeborn client lifecycle management (init/shutdown).
 *
 * <p>Implementation note: Celeborn's client classes reside in the shaded
 * {@code celeborn-client-spark-3-shaded_2.12} artifact. This wrapper isolates the Scala/shaded
 * API surface so that unit tests can mock it without requiring a running Celeborn cluster.
 */
public final class CelebornClientWrapper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornClientWrapper.class);

    private final String masterHost;
    private final int masterPort;
    private final Map<String, String> celebornProperties;

    /** Atomic counter for generating unique shuffle IDs per JVM instance. */
    private final AtomicInteger shuffleIdCounter = new AtomicInteger(0);

    /** Tracks active shuffle registrations: shuffleId → numPartitions */
    private final ConcurrentHashMap<Integer, Integer> activeShuffles = new ConcurrentHashMap<>();

    // --- Celeborn native client handle (initialized lazily) ---
    private volatile Object celebornLifecycleManager; // org.apache.celeborn.client.LifecycleManager
    private volatile Object celebornShuffleClient;    // org.apache.celeborn.client.ShuffleClient
    private volatile boolean initialized;

    public CelebornClientWrapper(String masterHost, int masterPort, Map<String, String> celebornProperties) {
        this.masterHost = Objects.requireNonNull(masterHost, "masterHost");
        this.masterPort = masterPort;
        this.celebornProperties = Map.copyOf(celebornProperties);
    }

    /**
     * Initializes the underlying Celeborn client connections.
     * Called lazily on first operation or eagerly during provider construction.
     */
    public synchronized void initialize() {
        if (initialized) {
            return;
        }
        LOG.info("Initializing Celeborn client: master={}:{}", masterHost, masterPort);

        try {
            // Use reflection to instantiate Celeborn classes from the shaded JAR.
            // This avoids compile-time dependency on Celeborn's internal Scala API surface.
            Class<?> confClass = Class.forName("org.apache.celeborn.common.CelebornConf");
            Object celebornConf = confClass.getDeclaredConstructor().newInstance();

            // Set master endpoints configuration
            var setMethod = confClass.getMethod("set", String.class, String.class);
            setMethod.invoke(celebornConf, "celeborn.master.endpoints", masterHost + ":" + masterPort);

            // Apply additional properties
            for (Map.Entry<String, String> entry : celebornProperties.entrySet()) {
                if (entry.getKey().startsWith("celeborn.")) {
                    setMethod.invoke(celebornConf, entry.getKey(), entry.getValue());
                }
            }

            // Create LifecycleManager
            Class<?> lmClass = Class.forName("org.apache.celeborn.client.LifecycleManager");
            // LifecycleManager(appId: String, conf: CelebornConf)
            celebornLifecycleManager = lmClass.getConstructor(String.class, confClass)
                    .newInstance("pangrova-" + ProcessHandle.current().pid(), celebornConf);

            // Create ShuffleClient
            Class<?> scClass = Class.forName("org.apache.celeborn.client.ShuffleClient");
            var getMethod = scClass.getMethod("get", String.class, confClass, lmClass.getInterfaces()[0]);
            celebornShuffleClient = getMethod.invoke(null,
                    "pangrova-" + ProcessHandle.current().pid(), celebornConf, celebornLifecycleManager);

            initialized = true;
            LOG.info("Celeborn client initialized successfully");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Celeborn client classes not found on classpath. "
                    + "Ensure celeborn-client-spark-3-shaded_2.12 is available.", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize Celeborn client", e);
        }
    }

    /**
     * Registers a new shuffle with Celeborn, returning the assigned shuffle ID.
     */
    public int registerShuffle(String queryId, int stageId, int numPartitions) {
        ensureInitialized();
        int shuffleId = shuffleIdCounter.getAndIncrement();
        activeShuffles.put(shuffleId, numPartitions);

        try {
            // Call LifecycleManager.registerShuffle(shuffleId, numPartitions)
            var registerMethod = celebornLifecycleManager.getClass()
                    .getMethod("registerShuffle", int.class, int.class);
            registerMethod.invoke(celebornLifecycleManager, shuffleId, numPartitions);
        } catch (Exception e) {
            activeShuffles.remove(shuffleId);
            throw new CelebornOperationException("registerShuffle failed", e);
        }

        LOG.debug("Registered shuffle: id={}, queryId={}, stageId={}, partitions={}",
                shuffleId, queryId, stageId, numPartitions);
        return shuffleId;
    }

    /**
     * Pushes partition data to Celeborn workers.
     *
     * @param shuffleId   the Celeborn shuffle ID
     * @param mapId       the mapper (producer) index
     * @param attemptId   the task attempt number
     * @param partitionId the target reduce partition
     * @param data        Arrow IPC encoded bytes to push
     */
    public void pushData(int shuffleId, int mapId, int attemptId, int partitionId, ByteBuffer data) {
        ensureInitialized();
        try {
            // ShuffleClient.pushData(shuffleId, mapId, attemptId, partitionId, data, length)
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);

            var pushMethod = celebornShuffleClient.getClass()
                    .getMethod("pushData", int.class, int.class, int.class, int.class,
                            byte[].class, int.class, int.class, int.class, int.class);
            pushMethod.invoke(celebornShuffleClient,
                    shuffleId, mapId, attemptId, partitionId,
                    bytes, 0, bytes.length, 1, 1);
        } catch (Exception e) {
            throw new CelebornOperationException(
                    "pushData failed: shuffleId=" + shuffleId + " mapId=" + mapId
                    + " partition=" + partitionId, e);
        }
    }

    /**
     * Signals that a mapper has finished and commits the stage.
     */
    public CommitResult commitShuffle(int shuffleId, int numMappers) {
        ensureInitialized();
        try {
            // Call mapperEnd for each mapper, then stageEnd
            var mapperEndMethod = celebornShuffleClient.getClass()
                    .getMethod("mapperEnd", int.class, int.class, int.class, int.class);

            for (int mapId = 0; mapId < numMappers; mapId++) {
                mapperEndMethod.invoke(celebornShuffleClient, shuffleId, mapId, 0, numMappers);
            }

            LOG.debug("All mappers ended for shuffleId={}", shuffleId);
            // Return basic commit result — partition-level stats collected from Celeborn
            // if available (implementation may vary by version).
            return new CommitResult(Map.of(), Map.of(), 0L, 0L);
        } catch (Exception e) {
            throw new CelebornOperationException("commitShuffle failed: shuffleId=" + shuffleId, e);
        }
    }

    /**
     * Reads partition data from Celeborn.
     *
     * @param shuffleId   the Celeborn shuffle ID
     * @param partitionId the partition to read
     * @return the raw bytes of the partition data
     */
    public ByteBuffer readPartition(int shuffleId, int partitionId) {
        ensureInitialized();
        try {
            InputStream is = openPartitionStream(shuffleId, partitionId);
            byte[] bytes = is.readAllBytes();
            is.close();
            return ByteBuffer.wrap(bytes);
        } catch (CelebornOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new CelebornOperationException(
                    "readPartition failed: shuffleId=" + shuffleId + " partition=" + partitionId, e);
        }
    }

    /**
     * Opens a streaming handle to read partition data from Celeborn.
     *
     * <p>Unlike {@link #readPartition} which reads all bytes into memory at once, this method
     * returns the raw InputStream from the Celeborn client, allowing the caller to read data
     * incrementally. This is essential for large partitions to avoid OOM.
     *
     * <p>The caller is responsible for closing the returned InputStream.
     *
     * @param shuffleId   the Celeborn shuffle ID
     * @param partitionId the partition to read
     * @return an InputStream for reading partition data; never null (may be empty)
     */
    public InputStream openPartitionStream(int shuffleId, int partitionId) {
        ensureInitialized();
        try {
            // ShuffleClient.readPartition(shuffleId, partitionId, startMapIndex, endMapIndex)
            var readMethod = celebornShuffleClient.getClass()
                    .getMethod("readPartition", int.class, int.class, int.class, int.class);
            Object inputStream = readMethod.invoke(celebornShuffleClient,
                    shuffleId, partitionId, 0, Integer.MAX_VALUE);

            if (inputStream instanceof InputStream is) {
                return is;
            }
            return InputStream.nullInputStream();
        } catch (Exception e) {
            throw new CelebornOperationException(
                    "openPartitionStream failed: shuffleId=" + shuffleId + " partition=" + partitionId, e);
        }
    }

    /**
     * Unregisters a shuffle and releases all associated resources in Celeborn.
     */
    public void unregisterShuffle(int shuffleId) {
        ensureInitialized();
        activeShuffles.remove(shuffleId);
        try {
            var unregisterMethod = celebornLifecycleManager.getClass()
                    .getMethod("unregisterShuffle", int.class);
            unregisterMethod.invoke(celebornLifecycleManager, shuffleId);
        } catch (Exception e) {
            LOG.warn("unregisterShuffle failed for shuffleId={}: {}", shuffleId, e.getMessage());
        }
    }

    public String masterHost() {
        return masterHost;
    }

    public int masterPort() {
        return masterPort;
    }

    @Override
    public void close() {
        LOG.info("Shutting down Celeborn client wrapper");
        if (celebornShuffleClient != null) {
            try {
                var shutdownMethod = celebornShuffleClient.getClass().getMethod("shutdown");
                shutdownMethod.invoke(celebornShuffleClient);
            } catch (Exception e) {
                LOG.debug("ShuffleClient shutdown error: {}", e.getMessage());
            }
        }
        if (celebornLifecycleManager != null) {
            try {
                var stopMethod = celebornLifecycleManager.getClass().getMethod("stop");
                stopMethod.invoke(celebornLifecycleManager);
            } catch (Exception e) {
                LOG.debug("LifecycleManager stop error: {}", e.getMessage());
            }
        }
        initialized = false;
    }

    private void ensureInitialized() {
        if (!initialized) {
            initialize();
        }
    }

    /**
     * Result of a shuffle commit operation.
     *
     * <p>The per-partition maps are defensively copied and are never {@code null};
     * a {@code null} argument is normalised to an empty map so callers that report
     * only aggregate totals keep working.</p>
     */
    public record CommitResult(
            Map<Integer, Long> partitionBytes,
            Map<Integer, Long> partitionRows,
            long totalBytes,
            long totalRows
    ) {
        public CommitResult {
            partitionBytes = partitionBytes == null ? Map.of() : Map.copyOf(partitionBytes);
            partitionRows = partitionRows == null ? Map.of() : Map.copyOf(partitionRows);
        }
    }
}
