package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.api.exception.CircuitBreakerOpenException;
import org.pangrova.scheduler.api.exception.TaskDispatchException;
import org.pangrova.scheduler.api.enums.ErrorCategory;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkPort;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.LongSupplier;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.transport.http.client.HttpClientFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;
import org.pangrova.protocol.http.TaskAdmitted;
import org.pangrova.protocol.http.TaskShuffleEndpoint;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.execution.binding.TaskControlBinding;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches tasks to Worker via real HTTP POST /v1/task.
 * Task submissions and acknowledgements use the internal Protobuf transport.
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
    private static final int HTTP_OK = 200;
    private static final int HTTP_EPOCH_AHEAD = 425;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final long EPOCH_CATCHUP_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(15);
    private static final long EPOCH_CATCHUP_RETRY_MS = 250;

    private final HttpClient httpClient;
    private volatile HttpClient copyTaskTlsClient;
    private final org.pangrova.transport.http.client.ManagedHttpClient managedClient;
    private final CircuitBreakerPolicy circuitBreakerPolicy;
    private final TracingProvider tracingProvider;
    private final String internalAuthSecret;
    private final WorkerTaskControlClient taskControlClient;
    // F-COORD-HA-04: Epoch supplier for zombie fencing. When set, every dispatched
    // task request is stamped with the current coordinator epoch so workers can
    // reject stale-epoch submissions during split-brain windows.
    private volatile java.util.function.UnaryOperator<SubmitTaskRequest> copySourceBinder;
    private volatile LongSupplier controlEpochSupplier;
    private volatile Function<String, ExecutionOwnerIdentity> executionOwnerResolver;

    /** Installs explicit mutual-TLS identity and CA for private COPY task submissions. */
    public void configureCopyTaskTls(ShuffleTlsConfig config) {
        checkNotNull(config, "config must not be null");
        this.copyTaskTlsClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .sslContext(org.pangrova.transport.http.tls.PemMutualTls.context(
                        config.certificateChain(), config.privateKey(), config.rootCa()))
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public RemoteTaskDispatcher(CircuitBreakerPolicy circuitBreakerPolicy, TracingProvider tracingProvider) {
        this(circuitBreakerPolicy, tracingProvider, null);
    }

    /** Creates a dispatcher that owns its HTTP client and applies internal authentication. */
    public RemoteTaskDispatcher(
            CircuitBreakerPolicy circuitBreakerPolicy,
            TracingProvider tracingProvider,
            String internalAuthSecret) {
        org.pangrova.transport.http.client.ManagedHttpClient managed = HttpClientFactory.createDefault();
        this.managedClient = managed;
        this.httpClient = managed.httpClient();
        this.circuitBreakerPolicy = checkNotNull(circuitBreakerPolicy, "circuitBreakerPolicy must not be null");
        this.tracingProvider = checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.internalAuthSecret = internalAuthSecret;
        this.taskControlClient = new WorkerTaskControlClient(
                httpClient, JsonMapperFactory.shared(), tracingProvider, internalAuthSecret);
    }

    public RemoteTaskDispatcher(HttpClient httpClient, CircuitBreakerPolicy circuitBreakerPolicy,
                              TracingProvider tracingProvider) {
        this(httpClient, circuitBreakerPolicy, tracingProvider, null);
    }

    /** Creates a dispatcher around a caller-owned HTTP client. */
    public RemoteTaskDispatcher(
            HttpClient httpClient,
            CircuitBreakerPolicy circuitBreakerPolicy,
            TracingProvider tracingProvider,
            String internalAuthSecret) {
        this.httpClient = checkNotNull(httpClient, "httpClient must not be null");
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.circuitBreakerPolicy = checkNotNull(circuitBreakerPolicy, "circuitBreakerPolicy must not be null");
        this.tracingProvider = checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.internalAuthSecret = internalAuthSecret;
        this.taskControlClient = new WorkerTaskControlClient(
                httpClient, JsonMapperFactory.shared(), tracingProvider, internalAuthSecret);
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
    @Override
    public SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
        return TaskControlBinding.bind(request, controlEpochSupplier, executionOwnerResolver);
    }

    private volatile java.util.function.Supplier<java.util.List<WorkerInfo>> recoveryWorkers;

    /** Supplies current physical Worker destinations for recovering an original admission domain. */
    public void setRecoveryWorkers(java.util.function.Supplier<java.util.List<WorkerInfo>> workers) {
        recoveryWorkers = checkNotNull(workers, "recoveryWorkers");
    }

    /** Composition-root wiring for durable frozen COPY source authority. */
    public void setCopySourceBinder(java.util.function.UnaryOperator<SubmitTaskRequest> binder) {
        copySourceBinder = checkNotNull(binder, "copySourceBinder");
    }

    @Override
    public SubmitTaskRequest bindSourceInput(SubmitTaskRequest request) {
        var binder = copySourceBinder;
        boolean needsInput = request.copySourceRead() == null && request.taskRequest().getSplitsList().stream()
                .anyMatch(split -> "copy".equals(split.getConnectorId()));
        if (needsInput && binder == null) {
            throw new IllegalStateException("COPY frozen source admission is not wired");
        }
        return needsInput ? binder.apply(request) : request;
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
        SubmitTaskRequest stamped = bindSourceInput(bindControlIdentity(bindWorkerIdentity(request, worker)));
        requireEncryptedCopyTaskTransport(stamped, worker);
        byte[] body = stamped.encode();
        long deadline = System.nanoTime() + EPOCH_CATCHUP_TIMEOUT_NANOS;
        for (boolean epochRetry = false; ; epochRetry = true) {
            verifyControlIdentityStillCurrent(stamped, worker);
            HttpResponse<byte[]> response = sendPost(stamped, worker, body, deadline, epochRetry);
            if (response.statusCode() != HTTP_EPOCH_AHEAD) {
                return handleResponse(worker, response, stamped);
            }
            if (System.nanoTime() >= deadline) {
                throw epochCatchupExpired(worker);
            }
            try {
                CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(
                        EPOCH_CATCHUP_RETRY_MS, TimeUnit.MILLISECONDS)).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_INTERRUPTED,
                        "Epoch catch-up interrupted for worker " + worker.host(), e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Epoch catch-up retry scheduling failed", e.getCause());
            }
        }
    }

    private void verifyControlIdentityStillCurrent(
            SubmitTaskRequest stamped, WorkerInfo worker) {
        var attempt = stamped.taskRequest().getAttempt();
        LongSupplier epochSupplier = controlEpochSupplier;
        if (epochSupplier != null && epochSupplier.getAsLong() != attempt.getControlEpoch()) {
            throw new TaskDispatchException(ErrorCategory.CONTROL_FENCED, worker.host(),
                    "Control epoch changed while dispatching task " + attempt.getTaskId());
        }
        Function<String, ExecutionOwnerIdentity> ownerResolver = executionOwnerResolver;
        if (ownerResolver != null) {
            ExecutionOwnerIdentity current = ownerResolver.apply(attempt.getQueryOrJobId());
            ExecutionOwnerIdentity stampedOwner = attempt.hasExecutionOwner()
                    ? ExecutionOwnerIdentity.from(attempt.getExecutionOwner()) : null;
            if (!java.util.Objects.equals(current, stampedOwner)) {
                throw new TaskDispatchException(ErrorCategory.CONTROL_FENCED, worker.host(),
                        "Execution owner changed while dispatching task " + attempt.getTaskId());
            }
        }
    }

    private static TaskDispatchException epochCatchupExpired(WorkerInfo worker) {
        return new TaskDispatchException(ErrorCategory.CONTROL_FENCED, worker.host(),
                "Worker did not observe the coordinator epoch before dispatch deadline");
    }

    static Duration dispatchRequestTimeout(long remainingNanos, boolean epochRetry, WorkerInfo worker) {
        // Keep one retry interval for the HTTP exchange after an explicit pre-admission rejection.
        long minimum = epochRetry ? TimeUnit.MILLISECONDS.toNanos(EPOCH_CATCHUP_RETRY_MS) : 0L;
        if (remainingNanos <= minimum) {
            throw epochCatchupExpired(worker);
        }
        return Duration.ofNanos(remainingNanos);
    }

    /**
     * F-COORD-HA-04: Sets the epoch supplier for coordinator zombie fencing.
     * Called during HA bootstrap to inject the HA service's epoch accessor.
     */
    public void setControlEpochSupplier(LongSupplier supplier) {
        this.controlEpochSupplier = checkNotNull(supplier, "supplier must not be null");
    }

    /** Installs the query/job execution-anchor resolver used at task admission. */
    public void setExecutionOwnerResolver(
            Function<String, ExecutionOwnerIdentity> resolver) {
        this.executionOwnerResolver = checkNotNull(resolver, "resolver must not be null");
    }

    private HttpResponse<byte[]> sendPost(SubmitTaskRequest request, WorkerInfo worker,
                                          byte[] body, long deadline, boolean epochRetry) {
        try {
            return taskClient(request).send(buildSubmitRequest(request, worker, body, deadline, epochRetry),
                    HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_INTERRUPTED,
                    "Task dispatch interrupted for worker " + worker.host(), e);
        } catch (HttpTimeoutException e) {
            throw new TaskDispatchException(ErrorCategory.RPC_TIMEOUT, worker.host(),
                    "Task dispatch timed out for worker " + worker.host(), e);
        } catch (IOException e) {
            throw new TaskDispatchException(ErrorCategory.NETWORK, worker.host(),
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

    private DispatchResult handleResponse(WorkerInfo worker, HttpResponse<byte[]> response,
                                          SubmitTaskRequest submitted) {
        int statusCode = response.statusCode();
        if (statusCode != HTTP_OK) {
            PangrovaErrorCode errorCode = (statusCode == HTTP_SERVICE_UNAVAILABLE)
                    ? PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED
                    : PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
            String message = "Worker returned HTTP " + statusCode;
            if (hasBinaryAck(response)) {
                var ack = deserializeResponse(response.body());
                if (!ack.hasRejected()) {
                    throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                            "Worker submission error must carry a rejected acknowledgement");
                }
                message += ": " + ack.getRejected().getReason();
            }
            throw new PangrovaException(errorCode, message);
        }

        if (!hasBinaryAck(response)) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Worker did not return a binary task acknowledgement");
        }
        var ack = deserializeResponse(response.body());
        var admitted = ack.getAdmitted();
        if (!ack.hasAdmitted() || !admitted.getTaskId().equals(submitted.taskRequest().getAttempt().getTaskId())) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Worker acknowledgement does not match the submitted task identity");
        }
        boolean resultTask = submitted.taskRequest().getExecutionOptions().getDirectResultOutput();
        if (resultTask != admitted.hasResultEndpoint()) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Worker result endpoint does not match the admitted result task scope");
        }

        try {
            return new DispatchResult(worker.host(), admitted.getTaskId(), resultEndpoint(admitted),
                    worker.workerId(), shuffleEndpoint(admitted));
        } catch (IllegalArgumentException failure) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Worker returned an invalid task endpoint", failure);
        }
    }

    private static boolean hasBinaryAck(HttpResponse<byte[]> response) {
        return response.headers().firstValue("Content-Type")
                .filter(SubmitTaskRequest.MEDIA_TYPE::equalsIgnoreCase).isPresent();
    }

    private static ResultEndpoint resultEndpoint(TaskAdmitted admitted) {
        if (!admitted.hasResultEndpoint()) return null;
        var endpoint = admitted.getResultEndpoint();
        int port = checkPort(endpoint.getPort(), "Invalid result endpoint");
        if (endpoint.getHost().isBlank() || endpoint.getTicket().isBlank() || endpoint.getProtocol().isBlank()) {
            throw new IllegalArgumentException("Invalid result endpoint");
        }
        return new ResultEndpoint(endpoint.getHost(), port, endpoint.getTicket(), endpoint.getProtocol());
    }

    private static TaskShuffleEndpoint shuffleEndpoint(TaskAdmitted admitted) {
        if (!admitted.hasShuffleEndpoint()) return null;
        var endpoint = admitted.getShuffleEndpoint();
        checkPort(endpoint.getPort(), "Invalid shuffle endpoint");
        if (endpoint.getHost().isBlank() || endpoint.getPartitionTicketsCount() == 0) {
            throw new IllegalArgumentException("Invalid shuffle endpoint");
        }
        for (int partition = 0; partition < endpoint.getPartitionTicketsCount(); partition++) {
            var ticket = endpoint.getPartitionTickets(partition);
            if (ticket.getPartitionId() != partition || ticket.getSignedTicket().isEmpty()
                    || ticket.getExpiresAtEpochMs() <= 0) {
                throw new IllegalArgumentException("Invalid shuffle ticket for partition " + partition);
            }
        }
        return endpoint;
    }

    private static TaskSubmitAck deserializeResponse(byte[] body) {
        try {
            var ack = TaskSubmitAck.parseFrom(body);
            if (!ack.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Unexpected fields in task acknowledgement");
            }
            return ack;
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new PangrovaException(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Failed to decode task acknowledgement", e);
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
        SubmitTaskRequest stamped = bindSourceInput(bindControlIdentity(bindWorkerIdentity(request, worker)));
        requireEncryptedCopyTaskTransport(stamped, worker);
        byte[] body = stamped.encode();
        long deadline = System.nanoTime() + EPOCH_CATCHUP_TIMEOUT_NANOS;

        return sendAsyncWithEpochCatchup(stamped, worker, body, deadline, false)
                .thenApply(result -> {
                    circuitBreakerPolicy.recordSuccess(breakerKey);
                    return result;
                })
                .exceptionally(ex -> {
                    TaskDispatchException classified = classifyAndWrap(ex, worker.host());
                    settleCircuitBreaker(breakerKey, classified);
                    throw classified;
                });
    }

    private void requireEncryptedCopyTaskTransport(SubmitTaskRequest request, WorkerInfo worker) {
        if (request.copySourceRead() != null) {
            if (copyTaskTlsClient == null || worker.copyTaskTlsPort() == 0
                    || internalAuthSecret == null || internalAuthSecret.isBlank()) {
                throw new IllegalStateException(
                        "COPY private task dispatch requires registered mTLS port and service identity");
            }
        }
    }

    private HttpClient taskClient(SubmitTaskRequest request) {
        return request.copySourceRead() == null ? httpClient : copyTaskTlsClient;
    }

    private static String taskUrl(SubmitTaskRequest request, WorkerInfo worker) {
        boolean privateCopy = request.copySourceRead() != null;
        return (privateCopy ? "https://" : "http://") + worker.host() + ":"
                + (privateCopy ? worker.copyTaskTlsPort() : worker.httpPort()) + "/v1/task";
    }

    private CompletableFuture<DispatchResult> sendAsyncWithEpochCatchup(
            SubmitTaskRequest stamped, WorkerInfo worker, byte[] body, long deadline, boolean epochRetry) {
        HttpRequest submitRequest;
        try {
            verifyControlIdentityStillCurrent(stamped, worker);
            submitRequest = buildSubmitRequest(stamped, worker, body, deadline, epochRetry);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return taskClient(stamped).sendAsync(submitRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenCompose(response -> {
                    if (response.statusCode() != HTTP_EPOCH_AHEAD) {
                        try {
                            return CompletableFuture.completedFuture(handleResponse(worker, response, stamped));
                        } catch (RuntimeException e) {
                            return CompletableFuture.failedFuture(e);
                        }
                    }
                    if (System.nanoTime() >= deadline) {
                        return CompletableFuture.failedFuture(epochCatchupExpired(worker));
                    }
                    // HTTP 425 is emitted before admission. Retry the identical attempt only
                    // after the Worker learns this epoch from its authenticated control channel.
                    return CompletableFuture.supplyAsync(() -> Boolean.TRUE,
                                    CompletableFuture.delayedExecutor(
                                            EPOCH_CATCHUP_RETRY_MS, TimeUnit.MILLISECONDS))
                            .thenCompose(ignored -> sendAsyncWithEpochCatchup(
                                    stamped, worker, body, deadline, true));
                });
    }

    private HttpRequest buildSubmitRequest(SubmitTaskRequest request, WorkerInfo worker,
                                          byte[] body, long deadline, boolean epochRetry) {
        String url = taskUrl(request, worker);
        LOG.debug("event=task_dispatch url={}", url);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                .header("Accept", SubmitTaskRequest.MEDIA_TYPE)
                .timeout(dispatchRequestTimeout(deadline - System.nanoTime(), epochRetry, worker))
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
     * CONTROL_FENCED also settles the permit without blaming the Worker.
     * WORKER_FATAL records failure (unknown/fatal error).
     */
    private void settleCircuitBreaker(String breakerTarget, TaskDispatchException classified) {
        switch (classified.getCategory()) {
            case NETWORK, RPC_TIMEOUT, WORKER_FATAL ->
                    circuitBreakerPolicy.recordFailure(breakerTarget, classified);
            case WORKER_BUSY, CONTROL_FENCED ->
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
     *   <li>{@link PangrovaException} with {@code COORDINATOR_DISPATCH_REJECTED}
     *       maps to {@link ErrorCategory#WORKER_BUSY}</li>
     *   <li>All other exceptions → {@link ErrorCategory#WORKER_FATAL}</li>
     * </ul>
     */
    TaskDispatchException classifyAndWrap(Throwable ex, String workerHost) {
        Throwable cause = (ex instanceof java.util.concurrent.CompletionException) ? ex.getCause() : ex;
        if (cause == null) {
            cause = ex;
        }

        if (cause instanceof TaskDispatchException existing) {
            return existing;
        }
        ErrorCategory category = classifyException(cause);
        LOG.debug("event=dispatch_exception_classified workerHost={} category={} exceptionType={}",
                workerHost, category, cause.getClass().getSimpleName());
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
        taskControlClient.injectDynamicFilter(
                taskId, planNodeId, filterProto, workerId);
    }

    @Override
    public SchedulerCancelResult cancelTask(SubmitTaskRequest request, String workerHost, int httpPort) {
        var attempt = request.taskRequest().getAttempt();
        if (!attempt.hasWorkerExecutionIdentity()) {
            return SchedulerCancelResult.failed("Issued task has no physical Worker execution identity", false);
        }
        var inventory = recoveryWorkers;
        if (inventory != null) {
            String domain = attempt.getWorkerExecutionIdentity().getAdmissionDomainId();
            var destinations = inventory.get().stream()
                    .filter(worker -> domain.equals(worker.workerExecutionIdentity().admissionDomainId()))
                    .map(worker -> new RecoveryDestination(worker.host(), worker.httpPort(),
                            worker.workerExecutionIdentity()))
                    .distinct().toList();
            if (destinations.size() > 1) {
                return SchedulerCancelResult.failed("Ambiguous current Worker admission domain", true);
            }
            if (!destinations.isEmpty()) {
                var destination = destinations.get(0);
                // Route to the exclusive successor, retaining the ORIGINAL sealed attempt bytes.
                return taskControlClient.cancelTask(request, destination.host(), destination.port());
            }
        }
        // The original endpoint may still own the task even before re-registration completes.
        // Its exact seal remains authoritative; an unknown endpoint never implies successful drain.
        return taskControlClient.cancelTask(request, workerHost, httpPort);
    }

    private record RecoveryDestination(String host, int port,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity identity) { }

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
        taskControlClient.notifyUpstreamCommit(
                workerHost, httpPort, queryId, upstreamStageId,
                stageFailureCallback);
    }
}
