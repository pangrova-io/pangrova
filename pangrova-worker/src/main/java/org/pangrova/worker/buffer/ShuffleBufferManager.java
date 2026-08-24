package org.pangrova.worker.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;

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
 * <p>Concurrency strategy: ConcurrentHashMap for partition buffer registry,
 * ConcurrentHashMap.newKeySet for closed partitions tracking.
 * ArrayBlockingQueue provides thread-safe bounded buffering per partition.
 * The {@code closed} flag is checked before and after blocking operations
 * to ensure clean shutdown without data corruption.</p>
 */
@ThreadSafe
public class ShuffleBufferManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);
    private static final int DEFAULT_CAPACITY = 100;
    private static final double DEFAULT_SPILL_THRESHOLD = 0.8;
    /** Default total memory budget: 512 MB for all shuffle partitions combined. */
    private static final long DEFAULT_MAX_TOTAL_BYTES = 512L * 1024 * 1024;

    private final int capacity;
    private final ConcurrentHashMap<ShuffleBufferKey, ArrayBlockingQueue<byte[]>> partitionBuffers =
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
        if (closed.get()) {
            throw new IllegalStateException("ShuffleBufferManager is closed");
        }
        partitionBuffers.putIfAbsent(key, new ArrayBlockingQueue<>(capacity));
        LOG.debug("event=partition_buffer_created key={}", key);
    }

    /** Adds a batch to the partition buffer. Blocks when full (backpressure).
     *
     * <p>BUG-03 fix: Updates {@code totalBufferedBytes} AFTER successful enqueue
     * instead of before. The previous "update before put" design caused the
     * counter to be inflated while {@code put()} blocked, triggering premature
     * spills. The spill check now runs after the counter is accurate.</p>
     *
     * <p>Race condition handling: after {@code put()} unblocks (either by space
     * becoming available or by close() injecting a sentinel), we re-check the
     * closed flag. If closed, we do NOT update the byte counter (the buffer was
     * already cleared by close()) and throw IllegalStateException. This prevents
     * the counter from drifting when close() races with blocked producers.</p>
     */
    public void addBatch(ShuffleBufferKey key, byte[] batch) throws InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("ShuffleBufferManager is closed");
        }
        ArrayBlockingQueue<byte[]> buffer = getBuffer(key);
        buffer.put(batch);
        // Re-check after blocking: close() may have fired while we were in put().
        if (closed.get()) {
            throw new IllegalStateException("ShuffleBufferManager is closed");
        }
        totalBufferedBytes.addAndGet(batch.length);
        maybeSpill();
    }

    /**
     * Polls a batch from the partition buffer.
     *
     * <p>Returns null when the manager is closed or when the poll times out.
     * Filters out zero-length sentinel arrays injected by {@link #close()}
     * to prevent downstream consumers from misinterpreting them as EOF.</p>
     *
     * @param key       the composite key to poll from
     * @param timeoutMs maximum wait time in milliseconds
     * @return the next batch, or null on timeout or closed state
     */
    public byte[] pollBatch(ShuffleBufferKey key, long timeoutMs) throws InterruptedException {
        if (closed.get()) {
            return null;
        }
        ArrayBlockingQueue<byte[]> buffer = partitionBuffers.get(key);
        if (buffer == null) {
            if (closed.get()) {
                return null;
            }
            throw new IllegalArgumentException("No buffer for key: " + key);
        }
        byte[] batch = buffer.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (batch == null) {
            return null;
        }
        // Filter zero-length sentinel injected by close() to unblock producers.
        if (batch.length == 0) {
            return null;
        }
        totalBufferedBytes.addAndGet(-batch.length);
        return batch;
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

    /** Clears all partition buffers and closed state. Idempotent — safe to call multiple times.
     *
     * <p>After setting the closed flag, offers a zero-length sentinel to each
     * partition queue to unblock threads waiting in {@code put()}. The sentinel
     * is filtered out by {@link #pollBatch} so it never reaches consumers.</p>
     *
     * <p>Partition buffers are NOT removed from the map during close. This
     * prevents a race where a producer thread wakes from {@code put()}, passes
     * the closed re-check in {@code addBatch()}, and then calls
     * {@code getBuffer()} which would throw IllegalArgumentException if the
     * map had been cleared. Instead, the closed flag ensures no further
     * meaningful operations occur.</p>
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // Unblock waiting producers by offering a sentinel to each queue.
        for (var entry : partitionBuffers.entrySet()) {
            ArrayBlockingQueue<byte[]> buf = entry.getValue();
            buf.clear();
            // Wake any thread blocked in put() — sentinel is filtered by pollBatch().
            buf.offer(new byte[0]);
        }
        // Do NOT clear partitionBuffers — producers that wake from put() already
        // hold a buffer reference, and the closed flag prevents further operations.
        // Only clear auxiliary state.
        closedPartitions.clear();
        totalBufferedBytes.set(0);
        if (spiller != null) {
            spiller.close();
        }
        LOG.info("event=shuffle_buffer_manager_closed spillCount={}", spillCount.get());
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
        ArrayBlockingQueue<byte[]> buffer = partitionBuffers.get(largestKey);
        if (buffer == null) {
            return false;
        }
        byte[] batch = buffer.poll();
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
        totalBufferedBytes.addAndGet(-batch.length);
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

    private ArrayBlockingQueue<byte[]> getBuffer(ShuffleBufferKey key) {
        ArrayBlockingQueue<byte[]> buffer = partitionBuffers.get(key);
        if (buffer == null) {
            if (closed.get()) {
                throw new IllegalStateException("ShuffleBufferManager is closed");
            }
            throw new IllegalArgumentException("No buffer for key: " + key);
        }
        return buffer;
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
