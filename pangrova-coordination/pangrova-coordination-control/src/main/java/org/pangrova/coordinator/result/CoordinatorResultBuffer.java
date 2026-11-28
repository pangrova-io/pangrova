package org.pangrova.coordinator.result;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.query.api.lifecycle.QueryOperation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Accounts for available and claimed results until their Arrow allocation has actually closed. */
@ThreadSafe
public final class CoordinatorResultBuffer implements AutoCloseable {
    public static final long DEFAULT_RETENTION_MS = 300_000L;
    public static final int DEFAULT_MAX_ENTRIES = 10_000;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 512L * 1024 * 1024;

    private final Map<String, Entry> results = new HashMap<>();
    private final ScheduledExecutorService expirationExecutor;
    private final long retentionMs;
    private final int maxEntries;
    private final long maxBytes;
    private long bufferedBytes;
    private boolean sealed;

    public CoordinatorResultBuffer(ScheduledExecutorService expirationExecutor, long retentionMs) {
        this(expirationExecutor, retentionMs, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BUFFERED_BYTES);
    }

    /** Creates a buffer with explicit limits for batches and retained Arrow bytes. */
    public CoordinatorResultBuffer(ScheduledExecutorService expirationExecutor, long retentionMs, int maxEntries,
            long maxBytes) {
        this.expirationExecutor = Objects.requireNonNull(expirationExecutor, "expirationExecutor");
        if (retentionMs <= 0 || maxEntries <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("Result retention and capacity limits must be positive");
        }
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    /** Takes ownership of the batch and operation, including on admission failure. */
    public void publish(String queryId, CoordinatorResultBatch batch, QueryOperation operation) {
        try {
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(batch, "batch");
            Objects.requireNonNull(operation, "operation");
            retain(queryId, new Entry(batch, operation));
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, () -> {
                if (batch != null) batch.close();
                if (operation != null) operation.close();
            });
            throw failure;
        }
    }

    private synchronized void retain(String queryId, Entry entry) {
        if (sealed) throw new IllegalStateException("Control result publication is closed");
        if (results.containsKey(queryId)) {
            throw new IllegalStateException("Control result identity is already owned: " + queryId);
        }
        if (results.size() >= maxEntries || entry.batch.bytes() > maxBytes - bufferedBytes) {
            throw new IllegalStateException("Control result buffer capacity exceeded for " + queryId);
        }
        results.put(queryId, entry);
        bufferedBytes += entry.batch.bytes();
        try {
            entry.expiration = expirationExecutor.schedule(() -> retire(queryId, entry, false),
                    retentionMs, TimeUnit.MILLISECONDS);
        } catch (RuntimeException | Error failure) {
            results.remove(queryId);
            bufferedBytes -= entry.batch.bytes();
            throw failure;
        }
    }

    /** Takes the one serving lease. Expiration and shutdown cannot free a claimed batch. */
    public synchronized ReadLease claim(String queryId) {
        var entry = results.get(queryId);
        if (sealed || entry == null || entry.claimed || entry.retiring) return null;
        entry.claimed = true;
        entry.expiration.cancel(false);
        return new ReadLease(queryId, entry);
    }

    /** Revokes an unclaimed batch when its query fails or is cancelled. */
    public void discardUnopened(String queryId) {
        Entry entry;
        synchronized (this) {
            entry = results.get(queryId);
        }
        if (entry != null) retire(queryId, entry, false);
    }

    /** Seals publication and new reads. Existing readers retain their exact allocation. */
    public void seal() {
        List<String> pending;
        synchronized (this) {
            sealed = true;
            pending = List.copyOf(results.keySet());
        }
        for (String queryId : pending) discardUnopened(queryId);
    }

    @Override
    public void close() {
        seal();
        if (size() != 0) throw new IllegalStateException("Control result readers still own allocations");
    }

    private void retire(String queryId, Entry entry, boolean reader) {
        synchronized (this) {
            if (results.get(queryId) != entry || entry.claimed != reader || entry.retiring) return;
            entry.retiring = true;
            entry.expiration.cancel(false);
        }
        try {
            entry.batch.close();
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                entry.retiring = false;
            }
            throw failure;
        }
        synchronized (this) {
            results.remove(queryId);
            bufferedBytes -= entry.batch.bytes();
        }
        entry.operation.close();
    }

    public synchronized boolean contains(String queryId) { return results.containsKey(queryId); }

    public synchronized int size() { return results.size(); }

    public synchronized long bufferedBytes() { return bufferedBytes; }

    public final class ReadLease implements AutoCloseable {
        private final String queryId;
        private final Entry entry;

        private ReadLease(String queryId, Entry entry) {
            this.queryId = queryId;
            this.entry = entry;
        }

        public void streamTo(ServerStreamListener listener) { entry.batch.streamTo(listener); }

        @Override
        public void close() { retire(queryId, entry, true); }
    }

    private static final class Entry {
        private final CoordinatorResultBatch batch;
        private final QueryOperation operation;
        private ScheduledFuture<?> expiration;
        private boolean claimed;
        private boolean retiring;

        private Entry(CoordinatorResultBatch batch, QueryOperation operation) {
            this.batch = batch;
            this.operation = operation;
        }
    }
}
