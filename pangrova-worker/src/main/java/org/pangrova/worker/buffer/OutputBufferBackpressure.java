package org.pangrova.worker.buffer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.worker.buffer.BackpressureController.BackpressureResult;
import org.pangrova.worker.execution.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges OutputBufferManager capacity signals to BackpressureController.
 * When the buffer is full, signals the native engine to pause producing results.
 *
 * <p>Concurrency strategy: {@link AtomicBoolean} for closed flag ensures
 * idempotent close and visibility across threads without synchronization.</p>
 */
public class OutputBufferBackpressure {

    private static final Logger LOG = LoggerFactory.getLogger(OutputBufferBackpressure.class);

    private final ActiveOutputBuffer activeOutputBuffer;
    private final BackpressureController controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public OutputBufferBackpressure(
            ActiveOutputBuffer activeOutputBuffer,
            BackpressureController controller,
            TaskExecutor taskExecutor) {
        this.activeOutputBuffer = activeOutputBuffer;
        this.controller = controller;
        // taskExecutor retained for future use (e.g., pause/resume signaling)
    }

    /**
     * Evaluates current buffer pressure and returns the action to take.
     * Uses query-scoped buffer when available, falls back to global buffer.
     *
     * @param executorHandle native executor handle for the task
     * @param queryId        query identifier for scoped buffer lookup (nullable)
     */
    public BackpressureResult evaluate(long executorHandle, String queryId) {
        if (closed.get()) {
            return new BackpressureResult(BackpressureController.Action.REJECT, java.time.Duration.ZERO);
        }
        OutputBufferManager buffer = activeOutputBuffer.getForQuery(queryId);
        int pending = buffer.getBufferedCount();
        BackpressureResult result = controller.checkBackpressure(pending);
        if (result.action() == BackpressureController.Action.REJECT) {
            LOG.warn("event=buffer_backpressure_reject pending={} executorHandle={} queryId={}",
                    pending, executorHandle, queryId);
        }
        return result;
    }

    /**
     * Evaluates current buffer pressure using the global buffer (legacy).
     */
    public BackpressureResult evaluate(long executorHandle) {
        return evaluate(executorHandle, null);
    }

    /**
     * Returns true when the buffer has capacity for more batches.
     */
    public boolean hasCapacity() {
        return !closed.get() && !activeOutputBuffer.get().isFull();
    }

    /**
     * Stops backpressure monitoring. Idempotent — safe to call multiple times.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        LOG.info("event=output_buffer_backpressure_closed");
    }

    /**
     * Returns true if this backpressure monitor has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }
}
