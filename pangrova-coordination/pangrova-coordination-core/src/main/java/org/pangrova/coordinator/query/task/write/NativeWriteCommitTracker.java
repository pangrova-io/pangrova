package org.pangrova.coordinator.query.task.write;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.common.file.CopyFileCommitBinding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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

    private final ConcurrentHashMap<String, ReservationEntry> reservations =
            new ConcurrentHashMap<>();
    private final long staleTimeoutMs;

    public NativeWriteCommitTracker() {
        this(DEFAULT_STALE_TIMEOUT_MS);
    }

    public NativeWriteCommitTracker(long staleTimeoutMs) {
        this.staleTimeoutMs = staleTimeoutMs > 0 ? staleTimeoutMs : DEFAULT_STALE_TIMEOUT_MS;
    }

    /** Elects one allocator without performing SDK I/O under a map or monitor lock. */
    public WriteReservation reserveWriteContext(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        var candidate = new ReservationEntry();
        var existing = reservations.putIfAbsent(queryId, candidate);
        return new WriteReservation(queryId, existing == null ? candidate : existing, existing == null);
    }

    /**
     * Registers write context for a query before native DML tasks are dispatched.
     *
     * @param queryId      the query identifier
     * @param catalogName  the target catalog name
     * @param outputHandle the output table handle from beginInsert/beginDelete/beginOverwrite
     * @param session      the connector session for the write operation
     * @param writeOutputBinding exact canonical binding dispatched to Workers
     * @param copyFile exact durable COPY admission, absent for ordinary writes
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public void registerWriteContext(String queryId, String catalogName,
                                     ConnectorOutputTableHandle outputHandle,
                                     ConnectorSession session,
                                     byte[] writeOutputBinding,
                                     WriteAuthorizationReceipt authorizationReceipt,
                                     WriteAllocationIntent allocationIntent,
                                     CopyFileCommitBinding copyFile) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(outputHandle, "outputHandle must not be null");
        checkNotNull(session, "session must not be null");
        checkNotNull(writeOutputBinding, "writeOutputBinding must not be null");
        checkNotNull(authorizationReceipt, "authorizationReceipt must not be null");
        checkNotNull(allocationIntent, "allocationIntent must not be null");
        if (session.getUser() == null || session.getUser().isBlank()) {
            throw new IllegalArgumentException("Write context requires its real authenticated principal");
        }
        if (writeOutputBinding.length == 0) {
            throw new IllegalArgumentException("writeOutputBinding must not be empty");
        }
        var state = new WriteCommitState(catalogName, outputHandle, session, writeOutputBinding,
                authorizationReceipt, allocationIntent, copyFile);
        var candidate = new ReservationEntry();
        candidate.result.complete(state);
        var existing = reservations.putIfAbsent(queryId, candidate);
        if (existing != null && !existing.result.complete(state)) {
            throw new IllegalStateException("Write context already registered or quarantined: " + queryId);
        }
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
        WriteCommitState state = peekWriteState(queryId);
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
        return peekWriteState(queryId) != null;
    }

    /**
     * Retrieves and removes the write commit state for a query.
     * Returns null if no write context was registered.
     *
     * @param queryId the query identifier
     * @return the write commit state, or null if not registered
     */
    public WriteCommitState removeWriteState(String queryId) {
        var removed = new java.util.concurrent.atomic.AtomicReference<WriteCommitState>();
        reservations.computeIfPresent(queryId, (id, entry) -> {
            removed.set(completedState(entry));
            if (entry.result.isDone() && !entry.cancelled) return null;
            entry.cancelled = true;
            entry.result.completeExceptionally(new IllegalStateException("Write allocation was cancelled: " + queryId));
            return entry.allocationStarted ? entry : null;
        });
        return removed.get();
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
        return completedState(reservations.get(queryId));
    }

    /**
     * Removes write state without returning it (cleanup on query failure/cancellation).
     */
    public void clearWriteState(String queryId) {
        removeWriteState(queryId);
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
            if (removeWriteState(queryId) != null) {
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
        for (var entry : reservations.entrySet()) {
            var state = completedState(entry.getValue());
            long updated = state == null ? entry.getValue().createdAtMs : state.lastUpdateTimeMs();
            if (!entry.getValue().cancelled && now - updated > staleTimeoutMs) {
                if (removeWriteState(entry.getKey()) != null) evicted++;
            }
        }
        return evicted;
    }

    /** Retains exact known, never-dispatched allocations when no real generic abort exists. */
    public Optional<ConnectorOutputTableHandle> unpublishedAllocation(String queryId) {
        var entry = reservations.get(queryId);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.unpublishedOutput);
    }

    private static WriteCommitState completedState(ReservationEntry entry) {
        return entry == null || !entry.result.isDone() || entry.result.isCompletedExceptionally()
                ? null : entry.result.join();
    }

    private static final class ReservationEntry {
        final CompletableFuture<WriteCommitState> result = new CompletableFuture<>();
        final long createdAtMs = System.currentTimeMillis();
        volatile boolean allocationStarted;
        volatile boolean cancelled;
        volatile ConnectorOutputTableHandle unpublishedOutput;
    }

    /** One reservation generation; cancellation cannot let its old winner publish into a retry. */
    public final class WriteReservation {
        private final String queryId;
        private final ReservationEntry entry;
        private final boolean owner;

        private WriteReservation(String queryId, ReservationEntry entry, boolean owner) {
            this.queryId = queryId;
            this.entry = entry;
            this.owner = owner;
        }

        public boolean owner() { return owner; }

        /** Waits outside every map/monitor lock for the actual winner result. */
        public WriteCommitState awaitResult() {
            try {
                return entry.result.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for write allocation", interrupted);
            } catch (ExecutionException failed) {
                throw new IllegalStateException("Write allocation did not complete", failed.getCause());
            }
        }

        /** Marks the exact point before SDK allocation so cancellation is not mistaken for compensation. */
        public void beginAllocation() {
            requireOwner();
            reservations.compute(queryId, (id, current) -> {
                requireActive(current);
                entry.allocationStarted = true;
                return current;
            });
        }

        /** Publishes only already-produced in-memory metadata; callbacks must never do SDK or codec I/O. */
        public void publish(Runnable registration) {
            requireOwner();
            reservations.compute(queryId, (id, current) -> {
                requireActive(current);
                registration.run();
                if (completedState(entry) == null) {
                    throw new IllegalStateException("Write registration did not publish");
                }
                return current;
            });
        }

        /** Removes a failed allocation for retry, or quarantines a known handle instead of inventing an abort. */
        public void fail(Throwable failure, ConnectorOutputTableHandle knownOutput) {
            requireOwner();
            reservations.computeIfPresent(queryId, (id, current) -> {
                if (current != entry) return current;
                entry.result.completeExceptionally(failure);
                if (knownOutput != null || entry.cancelled) {
                    entry.unpublishedOutput = knownOutput;
                    entry.cancelled = true;
                    return entry;
                }
                return null;
            });
            if (knownOutput != null) {
                LOG.error("event=undispatched_allocation_quarantined queryId={} target={} mode={}",
                        queryId, knownOutput.targetTable(), knownOutput.writeMode());
            }
        }

        private void requireOwner() {
            if (!owner) throw new IllegalStateException("Only the elected allocator can publish");
        }

        private void requireActive(ReservationEntry current) {
            if (current != entry || entry.cancelled || entry.result.isDone()) {
                throw new IllegalStateException("Write allocation reservation is no longer active: " + queryId);
            }
        }
    }

    /**
     * Holds the write context and collected commit metadata fragments for a single query.
     */
    public static final class WriteCommitState {

        private final String catalogName;
        private final ConnectorOutputTableHandle outputHandle;
        private final ConnectorSession session;
        private final byte[] writeOutputBinding;
        private final WriteAuthorizationReceipt authorizationReceipt;
        private final WriteAllocationIntent allocationIntent;
        private final CopyFileCommitBinding copyFile;
        private final List<byte[]> fragments = new ArrayList<>();
        private volatile long lastUpdateTimeMs;

        WriteCommitState(String catalogName, ConnectorOutputTableHandle outputHandle,
                         ConnectorSession session, byte[] writeOutputBinding,
                         WriteAuthorizationReceipt authorizationReceipt,
                         WriteAllocationIntent allocationIntent, CopyFileCommitBinding copyFile) {
            this.catalogName = catalogName;
            this.outputHandle = outputHandle;
            this.session = session;
            this.writeOutputBinding = writeOutputBinding.clone();
            this.authorizationReceipt = authorizationReceipt;
            this.allocationIntent = allocationIntent;
            this.copyFile = copyFile;
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

        public byte[] writeOutputBinding() {
            return writeOutputBinding.clone();
        }

        /** Returns the actual scope observed when this write was admitted, never reconstructed groups. */
        public WriteAuthorizationReceipt authorizationReceipt() { return authorizationReceipt; }

        public WriteAllocationIntent allocationIntent() { return allocationIntent; }

        public CopyFileCommitBinding copyFile() { return copyFile; }

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
