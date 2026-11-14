package org.pangrova.worker.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Bounded buffer for client result streaming.
 * Provides backpressure via blocking when the buffer is full.
 * Exposes capacity signals for BackpressureController integration.
 *
 * <p>Concurrency strategy: ArrayBlockingQueue for thread-safe bounded buffer,
 * AtomicBoolean for closed flag, AtomicLong for cumulative counters.</p>
 */
@ThreadSafe
public class OutputBufferManager {

    private static final Logger LOG = LoggerFactory.getLogger(OutputBufferManager.class);
    public static final int DEFAULT_MAX_BATCHES = 50;
    /**
     * Backpressure timeout in milliseconds. When the buffer stays full past this
     * deadline, {@link #addBatch(byte[])} throws {@link IllegalStateException} to
     * fail the producing task (fail-loud) — batches are never silently dropped.
     */
    public static final long BACKPRESSURE_TIMEOUT_MS = 30_000L;

    private final ArrayBlockingQueue<byte[]> buffer;
    private final int maxBatches;
    private final long maxBufferedBytes;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger pendingBatchCount = new AtomicInteger(0);
    private final AtomicLong totalOutputBatches = new AtomicLong(0);
    private final AtomicLong totalOutputBytes = new AtomicLong(0);
    private final AtomicLong totalBackpressureWaitMs = new AtomicLong(0);

    public OutputBufferManager() {
        this(DEFAULT_MAX_BATCHES, 0L);
    }

    public OutputBufferManager(int maxBatches) {
        this(maxBatches, 0L);
    }

    /**
     * Creates an output buffer with batch and byte-level capacity limits.
     *
     * @param maxBatches      maximum number of buffered batches (backpressure trigger)
     * @param maxBufferedBytes optional byte-level capacity limit (0 = unlimited, for future backpressure)
     */
    public OutputBufferManager(int maxBatches, long maxBufferedBytes) {
        this.maxBatches = maxBatches;
        this.maxBufferedBytes = maxBufferedBytes;
        this.buffer = new ArrayBlockingQueue<>(maxBatches);
    }

    private static final byte[] POISON_PILL = new byte[0];

    /**
     * Adds a batch to the output buffer.
     * Blocks up to the configured timeout when full, providing backpressure.
     * Tracks cumulative output bytes for stats reporting.
     *
     * @throws IllegalStateException if the buffer has been closed or remains full after timeout
     */
    public void addBatch(byte[] batch) throws InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("OutputBufferManager is closed, cannot add batch");
        }
        long start = System.nanoTime();
        boolean added = buffer.offer(batch, BACKPRESSURE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        trackBackpressureWait(start);
        if (added) {
            trackAddedBatch(batch);
        } else {
            throw new IllegalStateException(
                    "Output buffer full after " + BACKPRESSURE_TIMEOUT_MS + "ms timeout");
        }
    }

    private void trackBackpressureWait(long startNanos) {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (elapsedMs > 0) {
            totalBackpressureWaitMs.addAndGet(elapsedMs);
        }
    }

    private void trackAddedBatch(byte[] batch) {
        if (closed.get()) {
            buffer.remove(batch);
            throw new IllegalStateException("OutputBufferManager closed during addBatch");
        }
        pendingBatchCount.incrementAndGet();
        totalOutputBatches.incrementAndGet();
        totalOutputBytes.addAndGet(batch.length);
    }

    /**
     * Polls the next batch for client streaming.
     * Returns null on timeout or when a poison pill (close signal) is received.
     *
     * @param timeoutMs maximum wait time in milliseconds
     * @return the next batch, or null on timeout/close
     */
    public byte[] pollBatch(long timeoutMs) throws InterruptedException {
        byte[] batch = buffer.poll(timeoutMs, TimeUnit.MILLISECONDS);
        // BUG-X06: Poison pill signals close — return null to unblock poller
        if (batch == POISON_PILL) {
            return null;
        }
        if (batch != null) {
            pendingBatchCount.decrementAndGet();
        }
        return batch;
    }

    /**
     * Returns the number of real data batches currently buffered.
     * Excludes internal control signals (poison pill).
     */
    public int getBufferedCount() {
        return pendingBatchCount.get();
    }

    /**
     * Returns the maximum capacity of this buffer.
     */
    public int getMaxBatches() {
        return maxBatches;
    }

    /**
     * Returns the byte-level capacity limit (0 = unlimited).
     * Reserved for future backpressure integration.
     */
    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    /**
     * Returns true when the buffer has reached capacity.
     */
    public boolean isFull() {
        return buffer.remainingCapacity() == 0;
    }

    /**
     * Returns the remaining capacity before the buffer is full.
     */
    public int getRemainingCapacity() {
        return buffer.remainingCapacity();
    }

    /**
     * Closes this output buffer, preventing further additions.
     * Inserts a poison pill to unblock any thread waiting in {@link #pollBatch(long)}.
     *
     * <p>BUG-04 fix: The interrupt path now spins with {@code offer()} until
     * the poison pill is successfully inserted. A racing {@code addBatch()}
     * thread that passed the {@code closed} check before CAS could refill
     * the buffer after {@code clear()}, causing a single {@code offer()} to
     * fail. The spin loop guarantees eventual insertion because:
     * (a) no new addBatch() can start (closed=true), and
     * (b) pollBatch() or addBatch()'s post-offer cleanup will drain space.</p>
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Do NOT clear the buffer — allow readers to drain remaining data.
            // The poison pill signals EOF after all real data is consumed.
            // If the buffer is full, poll one element to make room for the pill.
            pendingBatchCount.set(0);
            while (!buffer.offer(POISON_PILL)) {
                buffer.poll();
            }
            LOG.debug("event=output_buffer_closed");
        }
    }

    /**
     * Returns whether this buffer has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Returns the cumulative number of output batches added.
     */
    public long getTotalOutputBatches() {
        return totalOutputBatches.get();
    }

    /**
     * Returns the cumulative number of output bytes added.
     */
    public long getTotalOutputBytes() {
        return totalOutputBytes.get();
    }

    /**
     * Returns the cumulative backpressure wait time in milliseconds.
     * Useful for observability: metric {@code worker.result_puller.backpressure_wait_ms}.
     */
    public long getTotalBackpressureWaitMs() {
        return totalBackpressureWaitMs.get();
    }
}
