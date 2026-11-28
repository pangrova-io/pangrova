package org.pangrova.worker.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Manages per-partition bounded buffers for shuffle data exchange.
 * Provides backpressure via blocking when buffers are full.
 *
 * <p>Buffers are keyed by {@link ShuffleBufferKey} (queryId + stageId + partitionId)
 * to ensure query/stage isolation on the same worker (P0-FPV-7).</p>
 *
 * <p>When a {@link ShuffleDiskSpiller} is provided (MEMORY_AND_DISK mode),
 * the manager spills the largest partition to disk when memory usage exceeds
 * the configured threshold.</p>
 *
 * <p>Each partition owns a bounded queue and separate producer/consumer conditions.
 * Publication, removal and byte accounting share that queue's lock. Closing seals
 * admission and wakes every waiter without inserting synthetic data.</p>
 */
@ThreadSafe
public final class ShuffleBufferManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);
    private static final int DEFAULT_CAPACITY = 100;
    private static final double DEFAULT_SPILL_THRESHOLD = 0.8;
    /** Default total memory budget: 512 MB for all shuffle partitions combined. */
    private static final long DEFAULT_MAX_TOTAL_BYTES = 512L * 1024 * 1024;

    private final int capacity;
    private final Object partitionOwnership = new Object();
    private final ConcurrentHashMap<ShuffleBufferKey, PartitionBuffer> partitionBuffers =
            new ConcurrentHashMap<>();
    private final Set<ShuffleBufferKey> closedPartitions = ConcurrentHashMap.newKeySet();
    private final ShuffleDiskSpiller spiller;  // null = MEMORY_ONLY
    private final double spillThreshold;
    private final AtomicLong totalBufferedBytes = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long maxTotalBytes;
    private final AtomicLong spillCount = new AtomicLong(0);

    public ShuffleBufferManager() {
        this(DEFAULT_CAPACITY, null, DEFAULT_SPILL_THRESHOLD);
    }

    public ShuffleBufferManager(int capacity) {
        this(capacity, null, DEFAULT_SPILL_THRESHOLD);
    }

    /**
     * @param capacity       per-partition buffer capacity (number of batches)
     * @param spiller        disk spiller (null = MEMORY_ONLY mode)
     * @param spillThreshold memory usage ratio to trigger spill (0.0-1.0)
     */
    public ShuffleBufferManager(int capacity, ShuffleDiskSpiller spiller, double spillThreshold) {
        this(capacity, spiller, spillThreshold, DEFAULT_MAX_TOTAL_BYTES);
    }

    /**
     * @param capacity       per-partition buffer capacity (number of batches)
     * @param spiller        disk spiller (null = MEMORY_ONLY mode)
     * @param spillThreshold memory usage ratio to trigger spill (0.0-1.0)
     * @param maxTotalBytes  total memory budget for all partitions combined
     */
    public ShuffleBufferManager(int capacity, ShuffleDiskSpiller spiller,
                                double spillThreshold, long maxTotalBytes) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got: " + capacity);
        }
        if (spillThreshold <= 0.0 || spillThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "spillThreshold must be in (0.0, 1.0], got: " + spillThreshold);
        }
        if (maxTotalBytes <= 0) {
            throw new IllegalArgumentException("maxTotalBytes must be positive, got: " + maxTotalBytes);
        }
        this.capacity = capacity;
        this.spiller = spiller;
        this.spillThreshold = spillThreshold;
        this.maxTotalBytes = maxTotalBytes;
    }

    // ── Composite-key API (P0-FPV-7) ──

    /** Creates a buffer for the given composite key.
     *
     * @throws IllegalStateException if the manager has been closed (BUG-D10 fix)
     */
    public void createPartition(ShuffleBufferKey key) {
        synchronized (partitionOwnership) {
            requireOpen();
            partitionBuffers.putIfAbsent(key, new PartitionBuffer());
        }
        LOG.debug("event=partition_buffer_created key={}", key);
    }

    /** Adds a batch, waiting for actual capacity until admission is sealed. */
    public void addBatch(ShuffleBufferKey key, byte[] batch) throws InterruptedException {
        java.util.Objects.requireNonNull(batch, "batch");
        requireOpen();
        getBuffer(key).put(batch);
        requireOpen();
        maybeSpill();
    }

    /**
     * Polls one actual batch, returning null on timeout or manager closure.
     *
     * @param key composite partition key
     * @param timeoutMs maximum wait in milliseconds
     * @return the next nonempty batch, or null on timeout, empty batch or closure
     */
    public byte[] pollBatch(ShuffleBufferKey key, long timeoutMs) throws InterruptedException {
        if (closed.get()) {
            return null;
        }
        PartitionBuffer buffer = partitionBuffers.get(key);
        if (buffer == null) {
            if (closed.get()) {
                return null;
            }
            throw new IllegalArgumentException("No buffer for key: " + key);
        }
        byte[] batch = buffer.poll(TimeUnit.MILLISECONDS.toNanos(timeoutMs));
        return batch != null && batch.length != 0 ? batch : null;
    }

    /** Signals that the given partition has finished producing data. */
    public void closePartition(ShuffleBufferKey key) {
        closedPartitions.add(key);
        LOG.debug("event=partition_closed key={}", key);
    }

    /** Returns whether the given partition has been closed. */
    public boolean isPartitionClosed(ShuffleBufferKey key) {
        return closedPartitions.contains(key);
    }

    // ── Lifecycle ──

    /** Seals admission, discards queued data and wakes every blocked queue operation. */
    @Override
    public void close() {
        synchronized (partitionOwnership) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
        }
        for (PartitionBuffer buffer : partitionBuffers.values()) {
            buffer.discardAndWake();
        }
        closedPartitions.clear();
        if (spiller != null) {
            spiller.close();
        }
        LOG.info("event=shuffle_buffer_manager_closed spillCount={}", spillCount.get());
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ShuffleBufferManager is closed");
        }
    }

    // ── Spill internals ──

    /** Masks the sign bit to guarantee a non-negative derived spill id. */
    private static final int NON_NEGATIVE_INT_MASK = 0x7FFFFFFF;

    /**
     * P1-BUG-2 fix: Removed misleading {@code incomingBytes} parameter.
     * Since {@code totalBufferedBytes} is updated BEFORE this call,
     * the counter already includes the incoming batch. The old parameter
     * was always passed as 0, creating API confusion.
     */
    private void maybeSpill() {
        if (spiller == null) {
            return;
        }
        long threshold = (long) (maxTotalBytes * spillThreshold);
        // Loop until below threshold or no more data to spill.
        while (totalBufferedBytes.get() > threshold) {
            if (!spillOneBatch()) {
                break;
            }
        }
    }

    private boolean spillOneBatch() {
        ShuffleBufferKey largestKey = findLargestPartition();
        if (largestKey == null) {
            return false;
        }
        PartitionBuffer buffer = partitionBuffers.get(largestKey);
        if (buffer == null) {
            return false;
        }
        byte[] batch = buffer.removeReady();
        if (batch == null) {
            return false;
        }
        // Use deterministic spill ID derived from the composite key's string form.
        // Raw hashCode() is unsuitable: can be negative and collisions between
        // different keys would mix spill data across queries/stages.
        // Bitwise AND with 0x7FFFFFFF guarantees non-negative (unlike Math.abs
        // which returns Integer.MIN_VALUE for Integer.MIN_VALUE input).
        // In practice, collisions are acceptable for spill isolation because each
        // ShuffleBufferManager instance is scoped to a single task execution.
        int spillId = largestKey.toString().hashCode() & NON_NEGATIVE_INT_MASK;
        spiller.spill(spillId, batch);
        spillCount.incrementAndGet();
        LOG.info("event=shuffle_spill_triggered key={} spillId={} bytes={}",
                largestKey, spillId, batch.length);
        return true;
    }

    private ShuffleBufferKey findLargestPartition() {
        ShuffleBufferKey largestKey = null;
        int largestSize = 0;
        for (var entry : partitionBuffers.entrySet()) {
            int size = entry.getValue().size();
            if (size > largestSize) {
                largestSize = size;
                largestKey = entry.getKey();
            }
        }
        return largestKey;
    }

    private PartitionBuffer getBuffer(ShuffleBufferKey key) {
        PartitionBuffer buffer = partitionBuffers.get(key);
        if (buffer == null) {
            if (closed.get()) {
                throw new IllegalStateException("ShuffleBufferManager is closed");
            }
            throw new IllegalArgumentException("No buffer for key: " + key);
        }
        return buffer;
    }

    /** Owns queue publication and byte accounting, including all closure wakeups. */
    private final class PartitionBuffer {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notFull = lock.newCondition();
        private final Condition notEmpty = lock.newCondition();
        @GuardedBy("lock")
        private final ArrayDeque<byte[]> batches = new ArrayDeque<>();

        private void put(byte[] batch) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (batches.size() >= capacity && !closed.get()) {
                    notFull.await();
                }
                requireOpen();
                batches.addLast(batch);
                totalBufferedBytes.addAndGet(batch.length);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        private byte[] poll(long nanos) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (batches.isEmpty() && !closed.get()) {
                    if (nanos <= 0) {
                        return null;
                    }
                    nanos = notEmpty.awaitNanos(nanos);
                }
                return closed.get() ? null : removeBatch();
            } finally {
                lock.unlock();
            }
        }

        private byte[] removeReady() {
            lock.lock();
            try {
                return closed.get() ? null : removeBatch();
            } finally {
                lock.unlock();
            }
        }

        @GuardedBy("lock")
        private byte[] removeBatch() {
            byte[] batch = batches.pollFirst();
            if (batch != null) {
                totalBufferedBytes.addAndGet(-batch.length);
                notFull.signal();
            }
            return batch;
        }

        private int size() {
            lock.lock();
            try {
                return batches.size();
            } finally {
                lock.unlock();
            }
        }

        private void discardAndWake() {
            lock.lock();
            try {
                for (byte[] batch : batches) {
                    totalBufferedBytes.addAndGet(-batch.length);
                }
                batches.clear();
                notFull.signalAll();
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    // ── Observability ──

    /** Total bytes currently buffered across all partitions. */
    public long getTotalBufferedBytes() {
        return totalBufferedBytes.get();
    }

    /** Total number of batches spilled to disk since creation. */
    public long getSpillCount() {
        return spillCount.get();
    }

    /** Maximum memory budget for all partitions combined. */
    public long getMaxTotalBytes() {
        return maxTotalBytes;
    }
}
