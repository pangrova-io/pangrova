package org.pangrova.coordinator.query.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Tracks commit metadata fragments from native write tasks per query.
 *
 * <p>When native DML (DELETE/UPDATE/MERGE) is dispatched to workers, each worker
 * produces commit metadata via {@code NativeWriteExecutor.commitWrite()}. The
 * coordinator collects these fragments as task status reports arrive, then
 * aggregates them for a single atomic catalog commit via
 * {@code ConnectorMetadata.finishDistributedWrite()}.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for per-query entries; each
 * {@link WriteCommitState} uses synchronized access for fragment collection
 * to ensure thread-safe aggregation from concurrent task status reports.</p>
 */
@ThreadSafe
public class NativeWriteCommitTracker {

    private static final Logger LOG = LoggerFactory.getLogger(NativeWriteCommitTracker.class);

    /** Default stale entry timeout: 30 minutes with no fragment updates. */
    private static final long DEFAULT_STALE_TIMEOUT_MS = 30L * 60 * 1000;

    private final ConcurrentHashMap<String, WriteCommitState> queryWriteStates =
            new ConcurrentHashMap<>();
    private final long staleTimeoutMs;

    public NativeWriteCommitTracker() {
        this(DEFAULT_STALE_TIMEOUT_MS);
    }

    public NativeWriteCommitTracker(long staleTimeoutMs) {
        this.staleTimeoutMs = staleTimeoutMs > 0 ? staleTimeoutMs : DEFAULT_STALE_TIMEOUT_MS;
    }

    /**
     * Registers write context for a query before native DML tasks are dispatched.
     *
     * @param queryId      the query identifier
     * @param catalogName  the target catalog name
     * @param outputHandle the output table handle from beginInsert/beginDelete/beginOverwrite
     * @param session      the connector session for the write operation
     */
    public void registerWriteContext(String queryId, String catalogName,
                                     ConnectorOutputTableHandle outputHandle,
                                     ConnectorSession session) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(outputHandle, "outputHandle must not be null");
        checkNotNull(session, "session must not be null");
        queryWriteStates.put(queryId, new WriteCommitState(catalogName, outputHandle, session));
    }

    /**
     * Adds a commit metadata fragment from a completed write task.
     *
     * @param queryId        the query identifier
     * @param commitMetadata the serialized commit metadata from the worker
     * @throws IllegalStateException if no write context is registered for the query
     * @throws IllegalArgumentException if commitMetadata is null or empty
     */
    public void addCommitFragment(String queryId, byte[] commitMetadata) {
        checkNotNull(queryId, "queryId must not be null");
        if (commitMetadata == null || commitMetadata.length == 0) {
            throw new IllegalArgumentException(
                    "commitMetadata must not be null or empty for queryId=" + queryId);
        }
        WriteCommitState state = queryWriteStates.get(queryId);
        if (state == null) {
            throw new IllegalStateException(
                    "No write context registered for queryId=" + queryId
                            + "; native write tasks completed but coordinator has no record"
                            + " of the write operation");
        }
        state.addFragment(commitMetadata.clone());
    }

    /**
     * Returns whether a write context is registered for the given query.
     */
    public boolean hasWriteContext(String queryId) {
        return queryWriteStates.containsKey(queryId);
    }

    /**
     * Retrieves and removes the write commit state for a query.
     * Returns null if no write context was registered.
     *
     * @param queryId the query identifier
     * @return the write commit state, or null if not registered
     */
    public WriteCommitState removeWriteState(String queryId) {
        return queryWriteStates.remove(queryId);
    }

    /**
     * Retrieves the write commit state for a query WITHOUT removing it.
     *
     * <p>F-WRITE-001 / A-COMMIT-01: the commit state machine peeks the state,
     * persists a durable commit intent, performs the catalog commit, and only
     * removes the tracker entry after the commit is durably marked COMMITTED.
     * Peeking (instead of removing up front) guarantees the write context
     * (session / output handle / fragments) survives a failure or crash that
     * occurs before the commit is durable, so the recovery path can reconcile
     * it rather than losing the only copy.</p>
     *
     * @param queryId the query identifier
     * @return the write commit state, or null if not registered
     */
    public WriteCommitState peekWriteState(String queryId) {
        return queryWriteStates.get(queryId);
    }

    /**
     * Removes write state without returning it (cleanup on query failure/cancellation).
     */
    public void clearWriteState(String queryId) {
        queryWriteStates.remove(queryId);
    }

    /**
     * Clears write state for multiple query IDs in bulk.
     * Used by multi-phase DML (MERGE decomposition) to clean up all
     * pending phase write contexts when a later phase fails.
     *
     * @param queryIds the query identifiers to clear
     * @return the number of entries actually removed
     */
    public int clearWriteStates(java.util.Collection<String> queryIds) {
        int removed = 0;
        for (String queryId : queryIds) {
            if (queryWriteStates.remove(queryId) != null) {
                removed++;
            }
        }
        return removed;
    }

    /**
     * Evicts write states that have not received any fragment updates within
     * the configured stale timeout. Should be called periodically (e.g., from
     * a scheduled cleanup task) to prevent memory leaks from abandoned writes.
     *
     * @return the number of stale entries evicted
     */
    public int evictStaleEntries() {
        long now = System.currentTimeMillis();
        int evicted = 0;
        for (var entry : queryWriteStates.entrySet()) {
            if (now - entry.getValue().lastUpdateTimeMs() > staleTimeoutMs) {
                if (queryWriteStates.remove(entry.getKey(), entry.getValue())) {
                    evicted++;
                    LOG.warn("event=stale_write_state_evicted queryId={} ageMs={}",
                            entry.getKey(), now - entry.getValue().lastUpdateTimeMs());
                }
            }
        }
        return evicted;
    }

    /**
     * Holds the write context and collected commit metadata fragments for a single query.
     */
    public static final class WriteCommitState {

        private final String catalogName;
        private final ConnectorOutputTableHandle outputHandle;
        private final ConnectorSession session;
        private final List<byte[]> fragments = new ArrayList<>();
        private volatile long lastUpdateTimeMs;

        WriteCommitState(String catalogName, ConnectorOutputTableHandle outputHandle,
                         ConnectorSession session) {
            this.catalogName = catalogName;
            this.outputHandle = outputHandle;
            this.session = session;
            this.lastUpdateTimeMs = System.currentTimeMillis();
        }

        synchronized void addFragment(byte[] fragment) {
            fragments.add(fragment);
            lastUpdateTimeMs = System.currentTimeMillis();
        }

        /** Returns the timestamp of the last fragment addition or creation. */
        long lastUpdateTimeMs() {
            return lastUpdateTimeMs;
        }

        public String catalogName() {
            return catalogName;
        }

        public ConnectorOutputTableHandle outputHandle() {
            return outputHandle;
        }

        public ConnectorSession session() {
            return session;
        }

        /**
         * Returns an immutable copy of collected fragments.
         */
        public synchronized List<byte[]> fragments() {
            return List.copyOf(fragments);
        }

        /**
         * Returns the number of collected fragments.
         */
        public synchronized int fragmentCount() {
            return fragments.size();
        }
    }
}
