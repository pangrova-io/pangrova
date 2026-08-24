package org.pangrova.coordinator.scheduler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches tasks to Worker via real HTTP POST /v1/task.
 * Uses java.net.http.HttpClient (JDK 11+), Jackson for JSON serialization.
 * Wraps dispatch with CircuitBreaker for fail-fast on repeated Worker failures.
 * Injects W3C trace context into outgoing HTTP headers for distributed tracing.
 * Client side only — no Jetty dependency needed.
 */
public class RemoteTaskDispatcher implements TaskDispatcher, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskDispatcher.class);

    /** Circuit breaker service name used as a prefix for per-host breaker keys. */
    private static final String BREAKER_SERVICE = "coordinator-to-worker";

    /** Maximum retry attempts for commit notifications (BUG-D28). */
    static final int COMMIT_NOTIFY_MAX_ATTEMPTS = 3;
    /** Initial backoff for commit notification retry: 1 second. */
    static final long COMMIT_NOTIFY_INITIAL_BACKOFF_MS = 1_000;
    /** Maximum backoff for commit notification retry: 4 seconds. */
    static final long COMMIT_NOTIFY_MAX_BACKOFF_MS = 4_000;

    private static final String INTERNAL_AUTH_HEADER = "X-Internal-Auth";

    private final HttpClient httpClient;
    private final org.pangrova.common.http.ManagedHttpClient managedClient;
    private final ObjectMapper mapper;
    private final CircuitBreakerPolicy circuitBreakerPolicy;
    private final TracingProvider tracingProvider;
    private final String internalAuthSecret;
    // F-COORD-HA-04: Epoch supplier for zombie fencing. When set, every dispatched
    // task request is stamped with the current coordinator epoch so workers can
    // reject stale-epoch submissions during split-brain windows.
    private volatile java.util.function.LongSupplier epochSupplier;

    public RemoteTaskDispatcher(CircuitBreakerPolicy circuitBreakerPolicy, TracingProvider tracingProvider) {
        this(circuitBreakerPolicy, tracingProvider, null);
    }

    public RemoteTaskDispatcher(CircuitBreakerPolicy circuitBreakerPolicy, TracingProvider tracingProvider,
                                String internalAuthSecret) {
        org.pangrova.common.http.ManagedHttpClient managed = HttpClientFactory.createDefault();
        this.managedClient = managed;
        this.httpClient = managed.httpClient();
        this.mapper = JsonMapperFactory.shared();
        this.circuitBreakerPolicy = checkNotNull(circuitBreakerPolicy, "circuitBreakerPolicy must not be null");
        this.tracingProvider = checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.internalAuthSecret = internalAuthSecret;
    }

    public RemoteTaskDispatcher(HttpClient httpClient, CircuitBreakerPolicy circuitBreakerPolicy,
                              TracingProvider tracingProvider) {
        this(httpClient, circuitBreakerPolicy, tracingProvider, null);
    }

    public RemoteTaskDispatcher(HttpClient httpClient, CircuitBreakerPolicy circuitBreakerPolicy,
                              TracingProvider tracingProvider, String internalAuthSecret) {
        this.httpClient = checkNotNull(httpClient, "httpClient must not be null");
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.mapper = JsonMapperFactory.shared();
        this.circuitBreakerPolicy = checkNotNull(circuitBreakerPolicy, "circuitBreakerPolicy must not be null");
        this.tracingProvider = checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.internalAuthSecret = internalAuthSecret;
    }

    /**
     * Shuts down the managed HTTP client thread pool if this dispatcher owns it.
     * No-op if the HttpClient was externally provided.
     *
     * <p>R10.1: Ensures the {@link HttpClientFactory}-created executor is shut down,
     * preventing lingering daemon threads after dispatcher close.</p>
     */
    @Override
    public void close() {
        if (managedClient != null) {
            managedClient.close();
            LOG.info("event=remote_task_dispatcher_closed reason=managed_client_shutdown");
        } else {
            LOG.debug("event=remote_task_dispatcher_close_skip reason=externally_provided_client");
        }
    }

    /**
     * F-COORD-HA-04: Stamps the request with the current coordinator epoch for
     * zombie fencing. Workers can reject submissions from stale-epoch coordinators.
     */
    private SubmitTaskRequest stampEpochIfNeeded(SubmitTaskRequest request) {
        if (epochSupplier == null || request.coordinatorEpoch() != null) {
            return request;
        }
        return new SubmitTaskRequest(
                request.taskId(), request.queryId(), request.stageId(), request.partitionId(),
                request.taskType(), request.planFragment(), request.splits(),
                request.exchangeSources(), request.resourceLimit(),
                request.sessionProperties(), request.outputConfig(), request.writeConfig(),
                request.dynamicFilters(), request.shuffleConfig(), request.splitMode(),
                request.tenantId(), request.principal(), request.vectorSearch(),
                epochSupplier.getAsLong());
    }

    @Override
    public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
        checkNotNull(request, "request must not be null");
        checkNotNull(worker, "worker must not be null");

        String breakerKey = breakerKey(worker.host());
        if (!circuitBreakerPolicy.allowRequest(breakerKey)) {
            throw new CircuitBreakerOpenException(worker.host());
        }

        try {
            DispatchResult result = doDispatch(request, worker);
            circuitBreakerPolicy.recordSuccess(breakerKey);
            return result;
        } catch (Exception e) {
            TaskDispatchException classified = classifyAndWrap(e, worker.host());
            settleCircuitBreaker(breakerKey, classified);
            throw classified;
        }
    }

    private DispatchResult doDispatch(SubmitTaskRequest request, WorkerInfo worker) {
        SubmitTaskRequest stamped = stampEpochIfNeeded(request);
        byte[] body = serializeRequest(stamped);
        HttpResponse<String> response = sendPost(worker, body);
        return handleResponse(worker, response);
    }

    /**
     * F-COORD-HA-04: Sets the epoch supplier for coordinator zombie fencing.
     * Called during HA bootstrap to inject the HA service's epoch accessor.
     */
    public void setEpochSupplier(java.util.function.LongSupplier supplier) {
        this.epochSupplier = supplier;
    }

    private byte[] serializeRequest(SubmitTaskRequest request) {
        try {
            return mapper.writeValueAsBytes(request);
        } catch (IOException e) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_SERIALIZATION_FAILED,
                    "Failed to serialize SubmitTaskRequest", e);
        }
    }

    private HttpResponse<String> sendPost(WorkerInfo worker, byte[] body) {
        String url = "http://" + worker.host() + ":" + worker.httpPort() + "/v1/task";
        LOG.debug("event=task_dispatch url={}", url);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));

        injectTraceContext(builder);
        injectInternalAuth(builder);

        try {
            return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_INTERRUPTED,
                    "Task dispatch interrupted for worker " + worker.host(), e);
        } catch (IOException e) {
            throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED,
                    "Task dispatch failed for worker " + worker.host(), e);
        }
    }

    private void injectTraceContext(HttpRequest.Builder builder) {
        tracingProvider.inject(null, builder, HttpRequest.Builder::header);
    }

    private void injectInternalAuth(HttpRequest.Builder builder) {
        if (internalAuthSecret != null && !internalAuthSecret.isEmpty()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
    }

    private DispatchResult handleResponse(WorkerInfo worker, HttpResponse<String> response) {
        int statusCode = response.statusCode();
        if (statusCode != 200) {
            PangrovaErrorCode errorCode = (statusCode == 503)
                    ? PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED
                    : PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
            throw new PangrovaException(errorCode,
                    "Worker returned HTTP " + statusCode + ": " + response.body());
        }

        SubmitTaskResponse taskResponse = deserializeResponse(response.body());
        if (!taskResponse.accepted()) {
            throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED,
                    "Worker rejected task: " + taskResponse.rejectReason());
        }

        return new DispatchResult(worker.host(), taskResponse.taskId(), taskResponse.resultEndpoint());
    }

    private SubmitTaskResponse deserializeResponse(String body) {
        try {
            return mapper.readValue(body, SubmitTaskResponse.class);
        } catch (IOException e) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Failed to deserialize SubmitTaskResponse", e);
        }
    }

    /**
     * Asynchronously dispatches a task using HttpClient.sendAsync.
     * Circuit breaker is checked before dispatch; success/failure recorded on completion.
     */
    @Override
    public CompletableFuture<DispatchResult> dispatchAsync(SubmitTaskRequest request, WorkerInfo worker) {
        checkNotNull(request, "request must not be null");
        checkNotNull(worker, "worker must not be null");

        String breakerKey = breakerKey(worker.host());
        if (!circuitBreakerPolicy.allowRequest(breakerKey)) {
            return CompletableFuture.failedFuture(new CircuitBreakerOpenException(worker.host()));
        }

        // F-COORD-HA-04: Stamp epoch before async dispatch (same as sync path).
        SubmitTaskRequest stamped = stampEpochIfNeeded(request);
        byte[] body = serializeRequest(stamped);
        HttpRequest httpRequest = buildAsyncRequest(worker, body);

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    DispatchResult result = handleResponse(worker, response);
                    circuitBreakerPolicy.recordSuccess(breakerKey);
                    return result;
                })
                .exceptionally(ex -> {
                    TaskDispatchException classified = classifyAndWrap(ex, worker.host());
                    settleCircuitBreaker(breakerKey, classified);
                    throw classified;
                });
    }

    private HttpRequest buildAsyncRequest(WorkerInfo worker, byte[] body) {
        String url = "http://" + worker.host() + ":" + worker.httpPort() + "/v1/task";
        LOG.debug("event=async_task_dispatch url={}", url);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));

        injectTraceContext(builder);
        injectInternalAuth(builder);
        return builder.build();
    }

    /**
     * Builds the compound breaker key for a given worker host.
     * The key has the form "coordinator-to-worker#host", which allows
     * ResilienceRegistry to look up the coordinator-to-worker config template
     * while maintaining per-host state.
     */
    private static String breakerKey(String workerHost) {
        return BREAKER_SERVICE + "#" + workerHost;
    }

    /**
     * Settles the circuit breaker after a dispatch failure.
     * F-RESIL-02: NETWORK and RPC_TIMEOUT errors record failure (worker is down).
     * F-RESIL-05: WORKER_BUSY (503 backpressure) records success to settle the permit
     * (worker is alive but temporarily overloaded).
     * WORKER_FATAL records failure (unknown/fatal error).
     */
    private void settleCircuitBreaker(String breakerTarget, TaskDispatchException classified) {
        switch (classified.getCategory()) {
            case NETWORK, RPC_TIMEOUT, WORKER_FATAL ->
                    circuitBreakerPolicy.recordFailure(breakerTarget, classified);
            case WORKER_BUSY ->
                    circuitBreakerPolicy.recordSuccess(breakerTarget);
            default -> throw new IllegalStateException(
                    "Unhandled ErrorCategory: " + classified.getCategory());
        }
    }

    /**
     * Classifies a dispatch exception by inspecting its type and wraps it in a
     * {@link TaskDispatchException} carrying the appropriate {@link ErrorCategory}.
     *
     * <p>Classification rules:
     * <ul>
     *   <li>{@link ConnectException} / general {@link IOException} → {@link ErrorCategory#NETWORK}</li>
     *   <li>{@link HttpTimeoutException} / {@link HttpConnectTimeoutException} /
     *       {@link TimeoutException} → {@link ErrorCategory#RPC_TIMEOUT}</li>
     *   <li>{@link PangrovaException} with {@code COORDINATOR_DISPATCH_REJECTED} → {@link ErrorCategory#WORKER_BUSY}</li>
     *   <li>All other exceptions → {@link ErrorCategory#WORKER_FATAL}</li>
     * </ul>
     */
    TaskDispatchException classifyAndWrap(Throwable ex, String workerHost) {
        Throwable cause = (ex instanceof java.util.concurrent.CompletionException) ? ex.getCause() : ex;
        if (cause == null) {
            cause = ex;
        }

        ErrorCategory category = classifyException(cause);
        LOG.debug("event=dispatch_exception_classified workerHost={} category={} exceptionType={}",
                workerHost, category, cause.getClass().getSimpleName());

        if (cause instanceof TaskDispatchException existing) {
            return existing;
        }
        return new TaskDispatchException(category, workerHost,
                "Task dispatch failed for worker " + workerHost + " [" + category + "]", cause);
    }

    private ErrorCategory classifyException(Throwable cause) {
        if (cause instanceof HttpTimeoutException || cause instanceof HttpConnectTimeoutException) {
            return ErrorCategory.RPC_TIMEOUT;
        }
        if (cause instanceof TimeoutException) {
            return ErrorCategory.RPC_TIMEOUT;
        }
        if (cause instanceof ConnectException) {
            return ErrorCategory.NETWORK;
        }
        if (cause instanceof IOException) {
            return ErrorCategory.NETWORK;
        }
        if (cause instanceof PangrovaException ae
                && ae.getErrorCode() == PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED) {
            return ErrorCategory.WORKER_BUSY;
        }
        return ErrorCategory.WORKER_FATAL;
    }

    @Override
    public void injectDynamicFilter(String taskId, String planNodeId,
                                    byte[] filterProto, String workerId) {
        String[] parts = workerId.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8080;
        String url = "http://" + host + ":" + port + "/v1/task/" + taskId + "/dynamic-filter";
        LOG.debug("event=dynamic_filter_inject url={} taskId={} planNodeId={}", url, taskId, planNodeId);

        try {
            byte[] body = mapper.writeValueAsBytes(
                    new org.pangrova.protocol.rest.task.InjectDynamicFilterRequest(planNodeId, filterProto));
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            injectTraceContext(builder);
            injectInternalAuth(builder);
            HttpResponse<String> response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                LOG.warn("event=dynamic_filter_inject_failed taskId={} workerId={} statusCode={}",
                        taskId, workerId, response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=dynamic_filter_inject_interrupted taskId={} workerId={}", taskId, workerId);
        } catch (IOException e) {
            LOG.warn("event=dynamic_filter_inject_io_error taskId={} workerId={} error={}",
                    taskId, workerId, e.getMessage());
        }
    }

    @Override
    public SchedulerCancelResult cancelTask(String taskId, String workerHost, int httpPort) {
        String url = "http://" + workerHost + ":" + httpPort + "/v1/task/" + taskId;
        LOG.debug("event=task_cancel url={}", url);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .DELETE();
        injectTraceContext(builder);
        injectInternalAuth(builder);

        return sendCancelRequest(builder.build(), taskId);
    }
    @Override
    public void notifyUpstreamCommit(String workerHost, int httpPort,
                                     String queryId, int upstreamStageId) {
        notifyUpstreamCommit(workerHost, httpPort, queryId, upstreamStageId, null);
    }

    /**
     * Notifies a downstream Worker that an upstream stage has committed, with retry.
     *
     * <p>Retries up to {@link #COMMIT_NOTIFY_MAX_ATTEMPTS} times with exponential backoff
     * (1s/2s/4s). On final failure, invokes {@code stageFailureCallback} so the caller
     * can fail the downstream stage explicitly (BUG-D28 fix).
     */
    @Override
    public void notifyUpstreamCommit(String workerHost, int httpPort,
                                     String queryId, int upstreamStageId,
                                     Runnable stageFailureCallback) {
        String url = "http://" + workerHost + ":" + httpPort + "/v1/task/upstream-commit";
        LOG.debug("event=upstream_commit_notify url={} queryId={} upstreamStageId={}",
                url, queryId, upstreamStageId);

        Exception lastException = retryCommitNotify(
                url, queryId, upstreamStageId, workerHost, stageFailureCallback);
        if (lastException != null) {
            LOG.error("event=upstream_commit_notify_exhausted queryId={} upstreamStageId={} "
                    + "workerHost={} attempts={} lastError={}",
                    queryId, upstreamStageId, workerHost, COMMIT_NOTIFY_MAX_ATTEMPTS,
                    lastException.getMessage());
            invokeFailureCallback(stageFailureCallback, queryId, upstreamStageId, workerHost);
        }
    }

    private Exception retryCommitNotify(String url, String queryId, int upstreamStageId,
                                        String workerHost, Runnable stageFailureCallback) {
        BackoffStrategy backoff = BackoffStrategy.retry(
                COMMIT_NOTIFY_INITIAL_BACKOFF_MS, COMMIT_NOTIFY_MAX_BACKOFF_MS);
        Exception lastException = null;
        for (int attempt = 1; attempt <= COMMIT_NOTIFY_MAX_ATTEMPTS; attempt++) {
            lastException = attemptCommitNotify(url, queryId, upstreamStageId, workerHost,
                    stageFailureCallback, attempt);
            if (lastException == null) {
                return null;
            }
            if (Thread.currentThread().isInterrupted()) {
                invokeFailureCallback(stageFailureCallback, queryId, upstreamStageId, workerHost);
                return null;
            }
            if (attempt < COMMIT_NOTIFY_MAX_ATTEMPTS) {
                sleepForRetry(backoff.next(), queryId, upstreamStageId);
            }
        }
        return lastException;
    }

    /** Returns null on success, or the exception on failure. */
    private Exception attemptCommitNotify(String url, String queryId, int upstreamStageId,
                                          String workerHost, Runnable stageFailureCallback, int attempt) {
        try {
            HttpRequest request = buildCommitRequest(url, queryId, upstreamStageId);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return null;
            }
            return logAndReturnCommitFailure(queryId, upstreamStageId, workerHost,
                    response.statusCode(), attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=upstream_commit_notify_interrupted queryId={} upstreamStageId={}",
                    queryId, upstreamStageId);
            invokeFailureCallback(stageFailureCallback, queryId, upstreamStageId, workerHost);
            return e;
        } catch (IOException e) {
            return logCommitIoError(queryId, upstreamStageId, workerHost, e, attempt);
        }
    }

    private Exception logAndReturnCommitFailure(String queryId, int upstreamStageId,
                                                String workerHost, int statusCode, int attempt) {
        LOG.warn("event=upstream_commit_notify_failed queryId={} upstreamStageId={} "
                + "workerHost={} statusCode={} attempt={} maxAttempts={}",
                queryId, upstreamStageId, workerHost, statusCode,
                attempt, COMMIT_NOTIFY_MAX_ATTEMPTS);
        return new IOException("Worker returned HTTP " + statusCode);
    }

    private Exception logCommitIoError(String queryId, int upstreamStageId,
                                       String workerHost, IOException e, int attempt) {
        LOG.warn("event=upstream_commit_notify_io_error queryId={} upstreamStageId={} "
                + "workerHost={} error={} attempt={} maxAttempts={}",
                queryId, upstreamStageId, workerHost, e.getMessage(),
                attempt, COMMIT_NOTIFY_MAX_ATTEMPTS);
        return e;
    }

    private void invokeFailureCallback(Runnable callback, String queryId,
                                       int upstreamStageId, String workerHost) {
        if (callback != null) {
            try {
                callback.run();
            } catch (Exception e) {
                LOG.warn("event=commit_notify_failure_callback_error queryId={} "
                        + "upstreamStageId={} workerHost={} error={}",
                        queryId, upstreamStageId, workerHost, e.getMessage());
            }
        }
    }

    private static void sleepForRetry(long delayMs, String queryId, int upstreamStageId) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("event=commit_notify_retry_sleep_interrupted queryId={} upstreamStageId={}",
                    queryId, upstreamStageId);
        }
    }

    private HttpRequest buildCommitRequest(String url, String queryId,
                                           int upstreamStageId) throws IOException {
        var notification = new org.pangrova.protocol.rest.task.UpstreamCommitNotification(
                queryId, upstreamStageId);
        byte[] body = mapper.writeValueAsBytes(notification);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        injectTraceContext(builder);
        injectInternalAuth(builder);
        return builder.build();
    }




    private SchedulerCancelResult sendCancelRequest(HttpRequest request, String taskId) {
        try {
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());
            return mapCancelResponse(response, taskId);
        } catch (IOException e) {
            LOG.warn("event=task_cancel_io_error taskId={} error={}", taskId, e.getMessage());
            return SchedulerCancelResult.failed("IO error: " + e.getMessage(), true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=task_cancel_interrupted taskId={}", taskId);
            return SchedulerCancelResult.failed("Interrupted", false);
        }
    }

    private SchedulerCancelResult mapCancelResponse(HttpResponse<String> response, String taskId) {
        if (response.statusCode() == 200) {
            return SchedulerCancelResult.cancelled();
        }
        if (response.statusCode() == 404) {
            return SchedulerCancelResult.notFound(taskId);
        }
        LOG.warn("event=task_cancel_failed taskId={} statusCode={}", taskId, response.statusCode());
        return SchedulerCancelResult.failed(
                "Worker returned HTTP " + response.statusCode(), true);
    }
}
