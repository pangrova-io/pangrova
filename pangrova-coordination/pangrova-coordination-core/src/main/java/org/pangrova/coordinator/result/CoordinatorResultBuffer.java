package org.pangrova.coordinator.result;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-local buffer for bounded control-plane statement responses.
 *
 * <p>Admission, publication and retirement share one owner monitor. Reads use
 * ConcurrentHashMap; expiration releases only its exact published entry.</p>
 *
 * <p>Implements {@link Closeable} and tracks buffered byte usage for quota
 * enforcement. Relational query results must never be inserted here.</p>
 */
@ThreadSafe
public final class CoordinatorResultBuffer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultBuffer.class);

    public static final long DEFAULT_RETENTION_MS = 300_000L;
    public static final int DEFAULT_MAX_ENTRIES = 10_000;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 512L * 1024 * 1024; // 512 MB

    private static final class ResultEntry {
        private final byte[] serializedBytes;
        private ScheduledFuture<?> expiration;

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
            if (expiration != null) {
                expiration.cancel(false);
                expiration = null;
            }
        }
    }

    private final ConcurrentHashMap<String, ResultEntry> results = new ConcurrentHashMap<>();
    private final ScheduledExecutorService expirationExecutor;
    private final long retentionMs;
    private final int maxEntries;
    private final long maxBufferedBytes;
    private final AtomicLong currentBufferedBytes = new AtomicLong(0);
    private boolean closed;
    private boolean expirationSealed;

    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor, long retentionMs) {
        this(expirationExecutor, retentionMs, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor, long retentionMs, int maxEntries) {
        this(expirationExecutor, retentionMs, maxEntries, DEFAULT_MAX_BUFFERED_BYTES);
    }

    /** Creates a buffer with explicit entry-count and buffered heap byte limits. */
    public CoordinatorResultBuffer(
            ScheduledExecutorService expirationExecutor,
            long retentionMs, int maxEntries, long maxBufferedBytes) {
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
        if (maxBufferedBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxBufferedBytes must be positive, got: " + maxBufferedBytes);
        }
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
        this.maxBufferedBytes = maxBufferedBytes;
    }

    /** Buffers serialized result bytes for later client fetch, subject to capacity limits. */
    public synchronized void put(String queryId, byte[] resultBytes) {
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

    /** Current tracked buffered heap byte usage across all buffered entries. */
    public long bufferedBytes() {
        return currentBufferedBytes.get();
    }

    /** Stops acknowledgement expiry after accepted publication owners drain, retaining readable bytes. */
    public synchronized void stopExpirationScheduling() {
        expirationSealed = true;
        for (var entry : results.values()) {
            entry.release();
        }
    }

    @Override
    public synchronized void close() {
        stopExpirationScheduling();
        closed = true;
        for (String queryId : results.keySet()) {
            releaseEntry(queryId);
        }
        LOG.info("event=result_buffer_closed remainingEntries={}", results.size());
    }

    // ── Internal ──

    private boolean tryInsert(String queryId, ResultEntry entry) {
        if (closed || expirationSealed) {
            throw new IllegalStateException("Control result publication is closed");
        }
        ResultEntry previous = results.get(queryId);
        if (previous != null) {
            if (!Arrays.equals(previous.serializedBytes, entry.serializedBytes)) {
                throw new IllegalStateException("Control result identity already owns different bytes: " + queryId);
            }
            return false;
        }
        long entryBytes = entry.estimatedBytes();
        if (results.size() >= maxEntries
                || entryBytes > maxBufferedBytes - currentBufferedBytes.get()) {
            throw new IllegalStateException("Control result buffer capacity exceeded for " + queryId);
        }
        boolean accounted = false;
        boolean published = false;
        try {
            results.put(queryId, entry);
            currentBufferedBytes.addAndGet(entryBytes);
            accounted = true;
            entry.expiration = expirationExecutor.schedule(
                    () -> releaseExactEntry(queryId, entry), retentionMs, TimeUnit.MILLISECONDS);
            published = true;
            return true;
        } finally {
            if (!published) {
                results.remove(queryId, entry);
                if (accounted) {
                    currentBufferedBytes.addAndGet(-entryBytes);
                }
                entry.release();
            }
        }
    }

    private synchronized void releaseExactEntry(String queryId, ResultEntry expected) {
        if (!expirationSealed && results.get(queryId) == expected) {
            releaseEntry(queryId);
        }
    }

    private synchronized void releaseEntry(String queryId) {
        ResultEntry removed = results.remove(queryId);
        if (removed != null) {
            currentBufferedBytes.addAndGet(-removed.estimatedBytes());
            removed.release();
            LOG.debug("event=result_released queryId={}", queryId);
        }
    }

}
