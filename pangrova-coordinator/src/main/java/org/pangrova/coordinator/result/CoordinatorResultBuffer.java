package org.pangrova.coordinator.result;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.data.page.NativePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-local buffer for query results.
 * Supports pre-serialized columnar IPC bytes and columnar page passthrough.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for result storage.
 * Expiration scheduling is thread-safe via ScheduledExecutorService.</p>
 *
 * <p>Implements {@link Closeable} to release all buffered NativePages on shutdown.
 * Tracks off-heap byte usage via {@link #offHeapBytes()} for quota enforcement.</p>
 */
@ThreadSafe
public final class CoordinatorResultBuffer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultBuffer.class);

    public static final long DEFAULT_RETENTION_MS = 300_000L;
    public static final int DEFAULT_MAX_ENTRIES = 10_000;
    public static final long DEFAULT_MAX_OFF_HEAP_BYTES = 512L * 1024 * 1024; // 512 MB

    private static final class ResultEntry {
        private final byte[] serializedBytes;
        private final NativePage arrowPage;

        private ResultEntry(byte[] serializedBytes, NativePage arrowPage) {
            this.serializedBytes = serializedBytes;
            this.arrowPage = arrowPage;
        }

        static ResultEntry fromBytes(byte[] bytes) {
            return new ResultEntry(bytes, null);
        }

        static ResultEntry fromArrowPage(NativePage page) {
            return new ResultEntry(null, page);
        }

        byte[] toSerializedBytes() {
            return serializedBytes != null ? serializedBytes : arrowPage.serializedBatch();
        }

        long estimatedBytes() {
            return serializedBytes != null ? serializedBytes.length : arrowPage.serializedBatch().length;
        }

        void release() {
            if (arrowPage != null && !arrowPage.isClosed()) {
                arrowPage.close();
            }
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

    /** Buffers a native Arrow page, releasing it when insertion is rejected. */
    public void putArrowPage(String queryId, NativePage arrowPage) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(arrowPage, "arrowPage");
        ResultEntry entry = ResultEntry.fromArrowPage(arrowPage);
        boolean inserted = false;
        try {
            inserted = tryInsert(queryId, entry);
        } finally {
            if (!inserted) {
                entry.release();
            }
        }
        LOG.debug("event=arrow_page_buffered queryId={} rows={}", queryId, arrowPage.getRowCount());
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
