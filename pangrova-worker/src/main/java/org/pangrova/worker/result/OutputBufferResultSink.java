package org.pangrova.worker.result;

import org.pangrova.worker.exception.BatchRejectedException;

import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.worker.buffer.BackpressureController.BackpressureResult;
import org.pangrova.worker.buffer.OutputBufferBackpressure;
import org.pangrova.worker.buffer.OutputBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Adapts result batches to OutputBufferManager.
 * Stores raw columnar IPC stream bytes for transport serving.
 *
 * <p>When an {@link OutputBufferBackpressure} is provided, evaluates
 * backpressure before adding each batch. On {@code SLOW_DOWN}, applies
 * a graduated delay to reduce upstream pressure. On {@code REJECT},
 * throws {@link BatchRejectedException} immediately rather than
 * blocking for the full 30-second timeout in
 * {@link OutputBufferManager#addBatch}.</p>
 */
public class OutputBufferResultSink implements ResultBatchSink {

    private static final Logger LOG = LoggerFactory.getLogger(OutputBufferResultSink.class);

    private final OutputBufferManager bufferManager;
    private final OutputBufferBackpressure backpressure;

    public OutputBufferResultSink(OutputBufferManager bufferManager) {
        this(bufferManager, null);
    }

    /**
     * @param bufferManager the output buffer to add batches to (must not be null)
     * @param backpressure  optional backpressure evaluator; when non-null, each
     *                      batch is checked for SLOW_DOWN/REJECT before enqueue
     */
    public OutputBufferResultSink(OutputBufferManager bufferManager,
                                  OutputBufferBackpressure backpressure) {
        this.bufferManager = checkNotNull(bufferManager, "bufferManager must not be null");
        this.backpressure = backpressure;
    }

    @Override
    public void onBatch(byte[] batchData) {
        applyBackpressure(batchData.length);
        try {
            bufferManager.addBatch(batchData);
        } catch (IllegalStateException e) {
            throw new BatchRejectedException(batchData.length,
                    OutputBufferManager.BACKPRESSURE_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchRejectedException(batchData.length,
                    OutputBufferManager.BACKPRESSURE_TIMEOUT_MS);
        }
    }

    /**
     * Evaluates backpressure before enqueuing a batch.
     * <ul>
     *   <li>{@code PROCEED} — no delay, continue to addBatch</li>
     *   <li>{@code SLOW_DOWN} — sleep for the computed delay to reduce
     *       upstream pressure, then continue to addBatch</li>
     *   <li>{@code REJECT} — throw immediately rather than blocking for
     *       the full addBatch timeout</li>
     * </ul>
     */
    private void applyBackpressure(int batchSize) {
        if (backpressure == null) {
            return;
        }
        BackpressureResult result = backpressure.evaluate(0);
        switch (result.action()) {
            case PROCEED -> { /* no-op */ }
            case SLOW_DOWN -> {
                long delayMs = result.delay().toMillis();
                LOG.debug("event=result_sink_backpressure_slow_down delayMs={} batchSize={}",
                        delayMs, batchSize);
                ThreadUtils.sleepQuietly(delayMs);
            }
            case REJECT -> {
                LOG.warn("event=result_sink_backpressure_reject batchSize={}", batchSize);
                throw new BatchRejectedException(batchSize, 0);
            }
            default -> throw new IllegalStateException(
                    "Unhandled backpressure action: " + result.action());
        }
    }

    @Override
    public void complete() {
        LOG.debug("event=result_stream_complete totalRows={} totalBytes={}",
                bufferManager.getTotalOutputBatches(), bufferManager.getTotalOutputBytes());
        bufferManager.close();
    }
}
