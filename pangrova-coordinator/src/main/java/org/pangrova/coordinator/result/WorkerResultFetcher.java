package org.pangrova.coordinator.result;

import java.util.function.BiConsumer;
import org.pangrova.client.rest.PangrovaConnectionException;
import org.pangrova.client.rest.ResultBatchConsumer;
import org.pangrova.client.rest.ResultStreamFetcher;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

/**
 * Fetches query results from a worker's native result server endpoint.
 *
 * <p>Delegates to {@link ResultStreamFetcher} (transport client) to pull
 * pre-serialized result pages from the worker, then feeds the bytes into
 * {@link CoordinatorResultService#dispatchLocal} for coordinator-side buffering.</p>
 *
 * <p>After a successful fetch, the result bytes are stored in the
 * {@link CoordinatorResultBuffer} so that subsequent client requests and
 * cache writeback are served from the coordinator's local buffer.</p>
 *
 * <p>When the worker endpoint is unreachable, retries with exponential backoff
 * using {@link BackoffStrategy}. Partial results from a disconnected stream
 * are never cached.</p>
 */
public final class WorkerResultFetcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerResultFetcher.class);

    private static final int STREAM_BUFFER_INITIAL_SIZE = 64 * 1024;
    private static final ThreadLocal<java.io.ByteArrayOutputStream> REUSABLE_STREAM_BAOS =
            ThreadLocal.withInitial(() -> new java.io.ByteArrayOutputStream(STREAM_BUFFER_INITIAL_SIZE));

    static final int DEFAULT_MAX_RETRIES = 10;
    static final long DEFAULT_INITIAL_DELAY_MS = 500;
    static final long DEFAULT_MAX_DELAY_MS = 10_000;
    private static final long NANOS_PER_MILLISECOND = 1_000_000;

    private final Function<ResultEndpoint, byte[]> fetchFunction;
    private final BiConsumer<ResultEndpoint, ResultBatchConsumer> streamFunction;
    private final AutoCloseable closeDelegate;
    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final Sleeper sleeper;

    /** Creates a fetcher backed by the default {@link ResultStreamFetcher} transport client. */
    public WorkerResultFetcher() {
        ResultStreamFetcher fetcher = new ResultStreamFetcher();
        this.fetchFunction = fetcher::fetch;
        this.streamFunction = fetcher::fetchStream;
        this.closeDelegate = fetcher;
        this.maxRetries = DEFAULT_MAX_RETRIES;
        this.initialDelayMs = DEFAULT_INITIAL_DELAY_MS;
        this.maxDelayMs = DEFAULT_MAX_DELAY_MS;
        this.sleeper = Thread::sleep;
    }

    /**
     * Constructor for testing: accepts a fetch function and a closeable.
     */
    WorkerResultFetcher(Function<ResultEndpoint, byte[]> fetchFunction,
                        AutoCloseable closeDelegate) {
        this(fetchFunction, null, closeDelegate, DEFAULT_MAX_RETRIES,
                DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_DELAY_MS, Thread::sleep);
    }

    WorkerResultFetcher(Function<ResultEndpoint, byte[]> fetchFunction,
                        AutoCloseable closeDelegate,
                        int maxRetries,
                        long initialDelayMs,
                        long maxDelayMs,
                        Sleeper sleeper) {
        this(fetchFunction, null, closeDelegate, maxRetries, initialDelayMs, maxDelayMs, sleeper);
    }

    WorkerResultFetcher(Function<ResultEndpoint, byte[]> fetchFunction,
                        BiConsumer<ResultEndpoint, ResultBatchConsumer> streamFunction,
                        AutoCloseable closeDelegate,
                        int maxRetries,
                        long initialDelayMs,
                        long maxDelayMs,
                        Sleeper sleeper) {
        this.fetchFunction = Objects.requireNonNull(fetchFunction, "fetchFunction");
        this.streamFunction = streamFunction;
        this.closeDelegate = closeDelegate;
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /**
     * Fetches result bytes from the worker's native result server and dispatches
     * them through the coordinator's local result pipeline.
     *
     * <p>Retries with exponential backoff when the endpoint is unreachable.
     * At max retries exceeded, fails with {@code TRANSPORT_CONNECTION_FAILED}.</p>
     *
     * @param workerHost     the worker host address
     * @param resultPort     the worker's native result server port
     * @param taskId         the native task ID used as the transport ticket
     * @param queryId        the coordinator query ID for buffering
     * @param resultService  the coordinator result service for local dispatch
     * @param finisher       callback to transition query state to FINISHED
     * @return the local dispatch result with coordinator-side endpoint
     */
    public CoordinatorResultService.LocalDispatchResult fetchAndDispatch(
            String workerHost,
            int resultPort,
            String taskId,
            String queryId,
            CoordinatorResultService resultService,
            CoordinatorResultService.QueryFinisher finisher) {

        ResultEndpoint workerEndpoint = new ResultEndpoint(
                workerHost, resultPort, taskId, "grpc");

        LOG.info("event=worker_result_fetch_start queryId={} taskId={} endpoint={}:{}",
                queryId, taskId, workerHost, resultPort);

        long startNanos = System.nanoTime();
        byte[] resultBytes = fetchWithRetry(workerEndpoint, queryId, taskId);
        long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLISECOND;

        LOG.info("event=worker_result_fetch_complete queryId={} taskId={} bytes={} durationMs={}",
                queryId, taskId, resultBytes.length, durationMs);

        return resultService.dispatchLocal(queryId, resultBytes, finisher);
    }

    private byte[] fetchWithRetry(ResultEndpoint endpoint, String queryId, String taskId) {
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelayMs);
        PangrovaConnectionException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return fetchFunction.apply(endpoint);
            } catch (PangrovaConnectionException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long delayMs = backoff.next();
                    LOG.warn("event=worker_result_fetch_retry queryId={} taskId={} attempt={} "
                                    + "maxRetries={} delayMs={} error={}",
                            queryId, taskId, attempt + 1, maxRetries, delayMs, e.getMessage());
                    sleep(delayMs);
                }
            }
        }

        LOG.error("event=worker_result_fetch_exhausted queryId={} taskId={} maxRetries={} endpoint={}:{}",
                queryId, taskId, maxRetries, endpoint.host(), endpoint.port());
        throw new PangrovaException(
                PangrovaErrorCode.TRANSPORT_CONNECTION_FAILED,
                "Result fetch failed after " + maxRetries + " retries for queryId=" + queryId
                        + " taskId=" + taskId + " endpoint=" + endpoint.host() + ":" + endpoint.port(),
                lastException);
    }

    private void sleep(long millis) {
        try {
            sleeper.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(
                    PangrovaErrorCode.TRANSPORT_CONNECTION_FAILED,
                    "Retry interrupted", e);
        }
    }

    /**
     * Streams result batches from the worker, invoking the consumer for each batch.
     * Retries with exponential backoff on connection failures.
     * Reduces coordinator memory pressure compared to full-pull fetch.
     */
    void streamWithRetry(ResultEndpoint endpoint, String queryId, String taskId,
                         java.util.function.Consumer<byte[]> batchConsumer) {
        if (streamFunction == null) {
            byte[] fullResult = fetchWithRetry(endpoint, queryId, taskId);
            batchConsumer.accept(fullResult);
            return;
        }
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelayMs);
        PangrovaConnectionException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                streamFunction.accept(endpoint, batchConsumer::accept);
                return;
            } catch (RuntimeException e) {
                lastException = handleStreamRetryException(e, backoff, queryId, taskId, attempt);
            }
        }

        throw new PangrovaException(
                PangrovaErrorCode.TRANSPORT_CONNECTION_FAILED,
                "Streaming fetch failed after " + maxRetries + " retries for queryId=" + queryId,
                lastException);
    }

    private PangrovaConnectionException handleStreamRetryException(
            RuntimeException e, BackoffStrategy backoff,
            String queryId, String taskId, int attempt) {
        PangrovaConnectionException ace = (e instanceof PangrovaConnectionException direct)
                ? direct
                : (e.getCause() instanceof PangrovaConnectionException wrapped) ? wrapped : null;
        if (ace == null) {
            throw e;
        }
        logAndSleepIfRetryable(backoff, queryId, taskId, attempt);
        return ace;
    }

    private void logAndSleepIfRetryable(BackoffStrategy backoff, String queryId,
                                        String taskId, int attempt) {
        if (attempt < maxRetries) {
            long delayMs = backoff.next();
            LOG.warn("event=worker_result_stream_retry queryId={} taskId={} attempt={} delayMs={}",
                    queryId, taskId, attempt + 1, delayMs);
            sleep(delayMs);
        }
    }

    /**
     * Fetches result bytes using streaming to limit coordinator memory pressure.
     * Each batch is accumulated incrementally rather than loading the entire
     * result set at once. Falls back to full-pull if streaming is not available.
     */
    public CoordinatorResultService.LocalDispatchResult fetchAndDispatchStreaming(
            String workerHost, int resultPort, String taskId, String queryId,
            CoordinatorResultService resultService,
            CoordinatorResultService.QueryFinisher finisher) {

        ResultEndpoint workerEndpoint = new ResultEndpoint(workerHost, resultPort, taskId, "grpc");
        LOG.info("event=worker_result_stream_start queryId={} taskId={} endpoint={}:{}",
                queryId, taskId, workerHost, resultPort);

        long startNanos = System.nanoTime();
        java.io.ByteArrayOutputStream accumulator = REUSABLE_STREAM_BAOS.get();
        accumulator.reset();
        streamWithRetry(workerEndpoint, queryId, taskId,
                batch -> accumulator.write(batch, 0, batch.length));

        long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLISECOND;
        byte[] resultBytes = accumulator.toByteArray();
        LOG.info("event=worker_result_stream_complete queryId={} taskId={} bytes={} durationMs={}",
                queryId, taskId, resultBytes.length, durationMs);

        return resultService.dispatchLocal(queryId, resultBytes, finisher);
    }

    /**
     * Abstraction for sleep to allow deterministic testing without real delays.
     */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    @Override
    public void close() {
        if (closeDelegate != null) {
            try {
                closeDelegate.close();
            } catch (Exception e) {
                LOG.warn("event=worker_result_fetcher_close_error error={}", e.getMessage());
            }
        }
        LOG.debug("event=worker_result_fetcher_closed");
    }
}
