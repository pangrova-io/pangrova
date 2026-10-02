package org.pangrova.coordinator.result;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-local buffer for bounded control-plane statement responses.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for result storage.
 * Expiration scheduling is thread-safe via ScheduledExecutorService.</p>
 *
 * <p>Implements {@link Closeable} and tracks buffered byte usage for quota
 * enforcement. Relational query results must never be inserted here.</p>
 */
@ThreadSafe
public final class CoordinatorResultBuffer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultBuffer.class);

    public static final long DEFAULT_RETENTION_MS = 300_000L;
    public static final int DEFAULT_MAX_ENTRIES = 10_000;
    public static final long DEFAULT_MAX_OFF_HEAP_BYTES = 512L * 1024 * 1024; // 512 MB

    private static final class ResultEntry {
        private final byte[] serializedBytes;
        private ResultEntry(byte[] serializedBytes) {
            this.serializedBytes = serializedBytes;
        }

        static ResultEntry fromBytes(byte[] bytes) {
            return new ResultEntry(bytes);
        }

        byte[] toSerializedBytes() {
            return serializedBytes;
        }

        long estimatedBytes() {
            return serializedBytes.length;
        }

        void release() {
            // Heap byte arrays are reclaimed by the GC after removal.
        }
    }

    private final ConcurrentHashMap<String, ResultEntry> results = new ConcurrentHashMap<>();
    private final ScheduledExecutorService expirationExecutor;
    private final long retentionMs;
    private final int maxEntries;
    private final long maxOffHeapBytes;
    private final AtomicLong currentOffHeapBytes = new AtomicLong(0);

    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor, long retentionMs) {
        this(expirationExecutor, retentionMs, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_OFF_HEAP_BYTES);
    }

    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor, long retentionMs, int maxEntries) {
        this(expirationExecutor, retentionMs, maxEntries, DEFAULT_MAX_OFF_HEAP_BYTES);
    }

    /** Creates a buffer with explicit entry-count and off-heap byte limits. */
    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor,
            long retentionMs, int maxEntries, long maxOffHeapBytes) {
        this.expirationExecutor = Objects.requireNonNull(
                expirationExecutor, "expirationExecutor");
        if (retentionMs <= 0) {
            throw new IllegalArgumentException(
                    "retentionMs must be positive, got: " + retentionMs);
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException(
                    "maxEntries must be positive, got: " + maxEntries);
        }
        if (maxOffHeapBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxOffHeapBytes must be positive, got: " + maxOffHeapBytes);
        }
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
        this.maxOffHeapBytes = maxOffHeapBytes;
    }

    /** Buffers serialized result bytes for later client fetch, subject to capacity limits. */
    public void put(String queryId, byte[] resultBytes) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(resultBytes, "resultBytes");
        ResultEntry entry = ResultEntry.fromBytes(resultBytes);
        if (!tryInsert(queryId, entry)) {
            return;
        }
        LOG.debug("event=result_buffered queryId={} sizeBytes={}", queryId, resultBytes.length);
    }

    public byte[] get(String queryId) {
        ResultEntry entry = results.get(queryId);
        return entry != null ? entry.toSerializedBytes() : null;
    }

    public void remove(String queryId) {
        releaseEntry(queryId);
    }

    public boolean contains(String queryId) {
        return results.containsKey(queryId);
    }

    public int size() {
        return results.size();
    }

    /** Current tracked off-heap byte usage across all buffered entries. */
    public long offHeapBytes() {
        return currentOffHeapBytes.get();
    }

    @Override
    public void close() {
        for (String queryId : results.keySet()) {
            releaseEntry(queryId);
        }
        LOG.info("event=result_buffer_closed remainingEntries={}", results.size());
    }

    // ── Internal ──

    private boolean tryInsert(String queryId, ResultEntry entry) {
        long entryBytes = entry.estimatedBytes();
        if (!checkBufferQuota(queryId, entryBytes)) {
            return false;
        }
        ResultEntry prev = results.putIfAbsent(queryId, entry);
        if (prev != null) {
            return false; // idempotent — already buffered
        }
        currentOffHeapBytes.addAndGet(entryBytes);
        scheduleExpiration(queryId);
        return true;
    }

    /**
     * Checks whether the buffer has capacity for a new entry.
     * Returns false and logs a warning if the buffer is full, instead of
     * throwing an unchecked exception that callers may not anticipate.
     */
    private boolean checkBufferQuota(String queryId, long entryBytes) {
        if (results.size() >= maxEntries) {
            LOG.warn("event=result_buffer_full reason=entry_count maxEntries={} queryId={}",
                    maxEntries, queryId);
            return false;
        }
        long projectedTotal = currentOffHeapBytes.get() + entryBytes;
        if (projectedTotal > maxOffHeapBytes) {
            LOG.warn("event=result_buffer_full reason=off_heap_quota "
                    + "offHeapBytes={} maxOffHeapBytes={} queryId={}",
                    projectedTotal, maxOffHeapBytes, queryId);
            return false;
        }
        return true;
    }

    private void releaseEntry(String queryId) {
        ResultEntry removed = results.remove(queryId);
        if (removed != null) {
            currentOffHeapBytes.addAndGet(-removed.estimatedBytes());
            removed.release();
            LOG.debug("event=result_released queryId={}", queryId);
        }
    }

    private void scheduleExpiration(String queryId) {
        expirationExecutor.schedule(
                () -> releaseEntry(queryId),
                retentionMs, TimeUnit.MILLISECONDS);
    }
}
