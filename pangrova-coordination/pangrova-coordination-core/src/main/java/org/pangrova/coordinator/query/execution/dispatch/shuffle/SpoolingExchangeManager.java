package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.shuffle.api.ShuffleCommitResult;
import org.pangrova.shuffle.api.ShuffleCapabilities;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolStoreContext;
import org.pangrova.shuffle.spool.SpoolStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side shuffle lifecycle manager that persists shuffle output via
 * {@link SpoolStore} for fault-tolerant execution (FTE).
 *
 * <p>When FTE is enabled with a durable shuffle provider ({@code local-spill} or
 * {@code object-store}), this manager replaces
 * {@link org.pangrova.shuffle.provider.spool.LocalShuffleLifecycleManager}
 * and ensures that committed shuffle partitions survive task failure, enabling
 * the DAG scheduler to retry failed downstream tasks by re-reading spooled data
 * rather than re-running upstream stages.
 *
 * <h3>Design rationale (Trino FTE alignment)</h3>
 * <p>Trino FTE uses a spooling exchange manager backed by S3/local disk to persist
 * all shuffle output. On task failure, the FTE scheduler re-dispatches the failed
 * task on a different worker; the new task reads committed partitions from the spool
 * rather than requesting the upstream to re-execute. This is the model Pangrova adopts
 * via the {@link SpoolStore} SPI (ADR-002 D6).</p>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link #registerShuffle} — records the stage's partition count for later commit/cleanup</li>
 *   <li>{@link #commitShuffle} — no-op (commit is per-partition via SpoolWriter.commit on the worker side)</li>
 *   <li>{@link #cleanupShuffle} — delegates to SpoolStore.delete(queryId) for best-effort cleanup</li>
 * </ol>
 *
 * <p>Thread-safe: per-query state is tracked in ConcurrentHashMap.</p>
 */
@ThreadSafe
public final class SpoolingExchangeManager implements ShuffleLifecycleManager, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SpoolingExchangeManager.class);

    private final SpoolStore spoolStore;
    private final ShuffleCapabilities capabilities;

    /** Tracks registered shuffles: key = "queryId:stageId", value = partition count. */
    private final ConcurrentHashMap<String, Integer> registrations = new ConcurrentHashMap<>();
    private final AtomicLong cleanupFailureCount = new AtomicLong();

    /**
     * Creates a spooling exchange manager backed by the given spool store.
     *
     * @param spoolStore   the persistent storage backend (e.g. local-fs, s3)
     * @param capabilities the FTE capabilities derived from the spool store's durability
     */
    public SpoolingExchangeManager(SpoolStore spoolStore, ShuffleCapabilities capabilities) {
        if (spoolStore == null) {
            throw new IllegalArgumentException("spoolStore must not be null");
        }
        if (capabilities == null) {
            throw new IllegalArgumentException("capabilities must not be null");
        }
        this.spoolStore = spoolStore;
        this.capabilities = capabilities;
        LOG.info("event=spooling_exchange_manager_created durability={}",
                spoolStore.durability());
    }

    /**
     * Factory: creates a SpoolingExchangeManager from a named spool store backend.
     *
     * <p>Discovers {@link SpoolStoreFactory} implementations via ServiceLoader,
     * selects by name (e.g. "local-fs", "s3"), and builds the SpoolStore.</p>
     *
     * @param storeName  the spool store backend name
     * @param properties spool.* configuration properties
     * @return a new SpoolingExchangeManager
     * @throws IllegalArgumentException if no factory matches the name
     */
    public static SpoolingExchangeManager create(String storeName,
                                                  Map<String, String> properties) {
        if (storeName == null || storeName.isBlank()) {
            throw new IllegalArgumentException("storeName must not be blank");
        }
        SpoolStoreFactory factory = findFactory(storeName);
        SpoolStoreContext context = new SpoolStoreContext(storeName, properties);
        SpoolStore store = factory.create(context);
        ShuffleCapabilities caps = ShuffleCapabilities.of(store.durability());
        return new SpoolingExchangeManager(store, caps);
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        if (registration == null) {
            throw new IllegalArgumentException("registration must not be null");
        }
        String key = buildKey(registration.queryId(), registration.stageId());
        registrations.put(key, registration.numPartitions());
        LOG.info("event=spooled_shuffle_registered queryId={} stageId={} partitions={} durability={}",
                registration.queryId(), registration.stageId(),
                registration.numPartitions(), spoolStore.durability());
        // Return LOCAL handle — the spool store handles persistence transparently;
        // no external server endpoints are needed (unlike RSS).
        return ShuffleHandle.LOCAL;
    }

    /**
     * Commits shuffle for a stage. For SpoolStore-backed shuffles, actual commit
     * happens per-partition on the worker side via {@code SpoolWriter.commit()}.
     * The coordinator-side commit is a visibility acknowledgment.
     *
     * @return {@link ShuffleCommitResult#EMPTY} — per-partition stats are collected
     *         from the spool store by the consumer task, not aggregated here
     */
    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        String key = buildKey(queryId, stageId);
        Integer partitions = registrations.get(key);
        if (partitions == null) {
            LOG.debug("event=spooled_shuffle_commit_no_registration queryId={} stageId={}",
                    queryId, stageId);
            return ShuffleCommitResult.EMPTY;
        }
        LOG.info("event=spooled_shuffle_committed queryId={} stageId={} partitions={}",
                queryId, stageId, partitions);
        return ShuffleCommitResult.EMPTY;
    }

    /**
     * Best-effort cleanup of all spooled data for the query.
     * Tolerates failures (logs and continues).
     */
    @Override
    public void cleanupShuffle(String queryId) {
        try {
            spoolStore.delete(queryId);
            LOG.info("event=spooled_shuffle_cleanup queryId={}", queryId);
        } catch (Exception e) {
            long failures = cleanupFailureCount.incrementAndGet();
            LOG.warn("event=spooled_shuffle_cleanup_failed queryId={} "
                    + "totalCleanupFailures={} error={}",
                    queryId, failures, e.getMessage(), e);
        }
        registrations.keySet().removeIf(k -> k.startsWith(queryId + ":"));
    }

    /** Returns the cumulative count of shuffle cleanup failures for monitoring. */
    public long cleanupFailureCount() {
        return cleanupFailureCount.get();
    }

    /** Returns the underlying spool store for direct access by task retry logic. */
    public SpoolStore spoolStore() {
        return spoolStore;
    }

    /** Returns the FTE capabilities (durability, task retry support). */
    public ShuffleCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public void close() {
        registrations.clear();
        try {
            spoolStore.close();
        } catch (Exception e) {
            LOG.warn("event=spooling_exchange_manager_close_failed error={}",
                    e.getMessage(), e);
        }
        LOG.info("event=spooling_exchange_manager_closed");
    }

    private static SpoolStoreFactory findFactory(String name) {
        for (SpoolStoreFactory factory : ServiceLoader.load(
                SpoolStoreFactory.class,
                SpoolingExchangeManager.class.getClassLoader())) {
            if (name.equals(factory.name())) {
                return factory;
            }
        }
        throw new IllegalArgumentException(
                "No SpoolStoreFactory registered for name '" + name
                        + "'. Ensure the provider JAR is on the classpath and "
                        + "META-INF/services is configured.");
    }

    private static String buildKey(String queryId, int stageId) {
        return queryId + ":" + stageId;
    }
}
