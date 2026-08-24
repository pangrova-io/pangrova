package org.pangrova.worker.shuffle;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.execution.BatchResult;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TracingProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Pulls serialized shuffle data from the native engine via TaskExecutor SPI
 * and sends to remote Workers via ShuffleSender.
 *
 * <p>The native engine returns pre-serialized PrestoVectorSerde bytes
 * (including partition_id) from a single queue. The thread polls until
 * EOF or error is returned, then terminates.</p>
 *
 * <p>P0-FPV-11: Uses {@link BatchResult} from the SPI to distinguish
 * EOF / NOT_READY / ERROR states. NOT_READY causes the loop to continue
 * polling; ERROR propagates as {@link ShuffleDrainException}; EOF exits
 * cleanly.</p>
 */
public class ShuffleDrainThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleDrainThread.class);

    /** Maximum backoff in milliseconds for NOT_READY polling. */
    private static final int MAX_BACKOFF_MS = 16;

    private final long executorHandle;
    private final ShuffleSender sender;
    private final TaskExecutor taskExecutor;
    private final TracingProvider tracingProvider;

    /**
     * Primary constructor — 3 required parameters within governance limit.
     */
    public ShuffleDrainThread(long executorHandle,
                              ShuffleSender sender, TaskExecutor taskExecutor) {
        this(executorHandle, sender, taskExecutor, null);
    }

    /**
     * Extended constructor with optional tracing support.
     */
    public ShuffleDrainThread(long executorHandle,
                              ShuffleSender sender, TaskExecutor taskExecutor,
                              TracingProvider tracingProvider) {
        if (executorHandle == 0) {
            throw new IllegalArgumentException("executorHandle must not be zero");
        }
        this.executorHandle = executorHandle;
        this.sender = Objects.requireNonNull(sender, "sender");
        this.taskExecutor = Objects.requireNonNull(taskExecutor, "taskExecutor");
        this.tracingProvider = tracingProvider;
    }

    /**
     * P0-FPV-11: Primary drain loop using discriminated {@link BatchResult}.
     *
     * <p>Loops on NOT_READY (transient backpressure), propagates ERROR as
     * {@link ShuffleDrainException}, exits cleanly on EOF.</p>
     */
    @Override
    public void run() {
        BackoffStrategy backoff = BackoffStrategy.poll(MAX_BACKOFF_MS);
        while (!Thread.currentThread().isInterrupted()) {
            BatchResult result = taskExecutor.getShuffleBatchResult(executorHandle);
            if (handleBatchResult(result, backoff)) {
                return;
            }
        }
    }

    private boolean handleBatchResult(BatchResult result, BackoffStrategy backoff) {
        switch (result.status()) {
            case DATA -> {
                backoff.reset();
                Span span = startShuffleDrainSpan();
                sendOrThrow(result.copyData());
                endSpan(span);
            }
            case EOF -> {
                LOG.info("event=shuffle_drain_eof executorHandle={}", executorHandle);
                return true;
            }
            case NOT_READY -> ThreadUtils.sleepQuietly(backoff.next());
            case ERROR -> {
                LOG.error("event=shuffle_drain_error executorHandle={} error={}",
                        executorHandle, result.errorMessage());
                throw new ShuffleDrainException(result.errorMessage());
            }
            default -> throw new IllegalStateException("Unhandled batch status: " + result.status());
        }
        return false;
    }

    /**
     * Enhanced drain loop using discriminated {@link ShuffleBatchResult}.
     * Retained for backward compatibility with callers that supply a custom
     * result supplier.
     *
     * @param resultSupplier supplier of discriminated shuffle batch results
     */
    public void runWithResult(java.util.function.Supplier<ShuffleBatchResult> resultSupplier) {
        while (!Thread.currentThread().isInterrupted()) {
            ShuffleBatchResult result = resultSupplier.get();
            switch (result.status()) {
                case DATA -> {
                    traceShuffleDrain();
                    sender.send(result.data());
                }
                case EOF -> {
                    LOG.info("event=shuffle_drain_eof executorHandle={}", executorHandle);
                    return;
                }
                case ERROR -> {
                    LOG.error("event=shuffle_drain_error executorHandle={} error={}",
                            executorHandle, result.errorMessage());
                    throw new ShuffleDrainException(result.errorMessage());
                }
                default -> throw new IllegalStateException(
                        "Unhandled batch status: " + result.status());
            }
        }
    }

    private void sendOrThrow(byte[] data) {
        try {
            sender.send(data);
        } catch (Exception e) {
            LOG.error("event=shuffle_send_failed executorHandle={} error={}",
                    executorHandle, e.getMessage(), e);
            throw new ShuffleDrainException("Shuffle send failed: " + e.getMessage());
        }
    }

    private Span startShuffleDrainSpan() {
        if (tracingProvider == null) {
            return null;
        }
        return tracingProvider.startSpan("pangrova.shuffle.drain", SpanKind.INTERNAL);
    }

    private static void endSpan(Span span) {
        if (span != null) {
            span.end();
        }
    }

    private void traceShuffleDrain() {
        if (tracingProvider == null) {
            return;
        }
        Span span = tracingProvider.startSpan("pangrova.shuffle.drain", SpanKind.INTERNAL);
        span.end();
    }

    /**
     * Abstraction for sending serialized shuffle data to remote workers.
     * Data is pre-serialized PrestoVectorSerde bytes from the native engine.
     */
    public interface ShuffleSender {

        void send(byte[] serializedData);
    }
}
