package org.pangrova.worker.task.report;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pangrova.common.concurrency.BackoffStrategy;
import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSleeper;
import org.pangrova.protocol.constants.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.http.TaskStatusAck;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusWire;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports task status to Coordinator via real HTTP POST /v1/task/status-report. Uses
 * java.net.http.HttpClient and binary Protobuf. Injects W3C trace context
 * into outgoing HTTP headers for distributed tracing. Client side only — no Jetty dependency
 * needed.
 *
 * <p>Final-state reports (FAILED/FINISHED/CANCELLED) use bounded exponential backoff retry.
 * Delivery may still fail after the retry budget is exhausted; non-final-state reports (RUNNING
 * etc.) remain best-effort — loss is compensated by the next heartbeat cycle.
 *
 * <p>Each status report carries a {@code reportSequence} field that serves as an idempotency key.
 * The Coordinator deduplicates by {@code taskId + reportSequence}, making retried deliveries safe.
 *
 * <p>HTTP 409 means the Coordinator has not registered this Worker. A terminal report remains
 * undelivered and is retried while registration can recover.
 */
public class RemoteTaskStatusReporter implements TaskStatusReporter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskStatusReporter.class);

    static final int MAX_RETRY_ATTEMPTS =
            TerminalReportDeliveryPolicy.ATTEMPTS_PER_ROUND;
    static final long INITIAL_BACKOFF_MS =
            org.pangrova.protocol.constants.TerminalReportDeliveryPolicy.INITIAL_BACKOFF.toMillis();
    static final long MAX_BACKOFF_MS =
            org.pangrova.protocol.constants.TerminalReportDeliveryPolicy.MAX_BACKOFF.toMillis();
    private static final long ROUND_TIMEOUT_MS = terminalRoundTimeoutMillis();
    private static final String FINAL_STATE_FAILED = "FAILED";
    private static final String FINAL_STATE_FINISHED = "FINISHED";
    private static final String FINAL_STATE_CANCELLED = "CANCELLED";

    private final HttpClient httpClient;
    private final org.pangrova.transport.http.client.ManagedHttpClient managedClient;
    private volatile String baseUrl;
    private final TracingProvider tracingProvider;
    private final LongCounter reportFailedCounter;
    private final String internalAuthSecret;
    private final TerminalAttempts terminalAttempts = new TerminalAttempts();

    /**
     * Creates and owns the HTTP client and registers the terminal delivery failure counter.
     *
     * @param coordinatorBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     * @param tracingProvider tracing context propagation
     */
    public RemoteTaskStatusReporter(
            String coordinatorBaseUrl, TracingProvider tracingProvider,
            MetricsProvider metricsProvider, String internalAuthSecret) {
        checkNotNull(coordinatorBaseUrl, "coordinatorBaseUrl must not be null");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.baseUrl = coordinatorBaseUrl;
        this.tracingProvider = tracingProvider;
        this.internalAuthSecret = normalizeSecret(internalAuthSecret);
        this.reportFailedCounter = deliveryFailures(metricsProvider);
        this.managedClient = org.pangrova.transport.http.client.HttpClientFactory.createDefault();
        this.httpClient = managedClient.httpClient();
    }

    /**
     * Constructs reporter with custom HTTP client and metrics provider.
     *
     * @param coordinatorBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     * @param httpClient JDK HttpClient for sending requests
     * @param tracingProvider tracing context propagation
     * @param metricsProvider metrics recording for terminal delivery failures
     */
    public RemoteTaskStatusReporter(
            String coordinatorBaseUrl,
            HttpClient httpClient,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            String internalAuthSecret) {
        checkNotNull(coordinatorBaseUrl, "coordinatorBaseUrl must not be null");
        checkNotNull(httpClient, "httpClient must not be null");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.baseUrl = coordinatorBaseUrl;
        this.httpClient = httpClient;
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.tracingProvider = tracingProvider;
        this.internalAuthSecret = normalizeSecret(internalAuthSecret);
        this.reportFailedCounter = deliveryFailures(metricsProvider);
    }

    /**
     * Updates the Coordinator target address for status reporting. Thread-safe: baseUrl is
     * volatile, so subsequent report() calls will use the new target without synchronization.
     *
     * @param newBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     */
    public void updateTarget(String newBaseUrl) {
        checkNotNull(newBaseUrl, "newBaseUrl must not be null");
        this.baseUrl = newBaseUrl;
        LOG.info("event=status_reporter_target_update baseUrl={}", this.baseUrl);
    }

    /**
     * Shuts down the managed HTTP client thread pool if this reporter owns it. No-op if the
     * HttpClient was externally provided.
     */
    @Override
    public void close() {
        if (managedClient != null) {
            managedClient.close();
        }
    }

    @Override
    public void report(TaskStatusReport report) {
        checkNotNull(report, "report must not be null");
        if (isFinalState(report.state())) {
            awaitTerminalReport(report);
            return;
        }
        byte[] body = serializeReport(report);
        if (body == null) return;
        if ("RUNNING".equals(report.state())) {
            // P1-16: RUNNING is the most critical non-final state — coordinator
            // uses it to confirm the task started. One retry prevents re-dispatch
            // on transient network blip.
            if (!sendPost(body, report)) {
                sleep(INITIAL_BACKOFF_MS);
                sendPost(body, report);
            }
        } else {
            sendPost(body, report);
        }
    }

    private void awaitTerminalReport(TaskStatusReport report) {
        var delivery = reportTerminal(report).toCompletableFuture();
        try {
            delivery.get();
        } catch (InterruptedException interrupted) {
            delivery.cancel(false);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Terminal task report was interrupted: " + report.taskId(), interrupted);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new IllegalStateException("Terminal task report failed: " + report.taskId(), failure.getCause());
        }
    }

    /**
     * Sends a final-state report with bounded exponential backoff retry. Initial backoff: 1s, max
     * backoff: 30s, max attempts: 5. On exhaustion, records {@code task_status_report_failed}
     * metric.
     */
    @Override
    public CompletionStage<Void> reportTerminal(TaskStatusReport report) {
        checkNotNull(report, "report must not be null");
        if (!isFinalState(report.state())) {
            throw new IllegalArgumentException("Report is not terminal: " + report.taskId());
        }
        byte[] body = serializeReport(report);
        if (body == null) {
            reportFailedCounter.add(1);
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Cannot serialize terminal task report: " + report.taskId()));
        }
        var traceHeaders = new HashMap<String, String>();
        tracingProvider.inject(null, traceHeaders, Map::put);
        var round = new TerminalRound(report, HttpRequest.BodyPublishers.ofByteArray(body), Map.copyOf(traceHeaders),
                BackoffStrategy.retry(INITIAL_BACKOFF_MS, MAX_BACKOFF_MS),
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ROUND_TIMEOUT_MS), new CompletableFuture<>());
        // Including time spent waiting for a transport slot, one round fits the
        // same delivery horizon and ACK retention budget used by the Coordinator.
        round.delivery().orTimeout(ROUND_TIMEOUT_MS, TimeUnit.MILLISECONDS).whenComplete((ignored, failure) -> {
            if (failure instanceof TimeoutException) {
                reportFailedCounter.add(1);
                LOG.warn("event=terminal_report_round_timeout taskId={} reportSequence={}",
                        report.taskId(), report.reportSequence());
            }
        });
        enqueueTerminalAttempt(round, 1);
        return round.delivery();
    }

    private void enqueueTerminalAttempt(TerminalRound round, int attempt) {
        var delivery = round.delivery();
        if (delivery.isDone()) return;
        try {
            terminalAttempts.startOrQueue(() -> sendTerminalAttempt(round, attempt), delivery);
        } catch (RejectedExecutionException failure) {
            reportFailedCounter.add(1);
            delivery.completeExceptionally(failure);
        }
    }

    private void sendTerminalAttempt(TerminalRound round, int attempt) {
        var delivery = round.delivery();
        long remainingNanos = round.deadlineNanos() - System.nanoTime();
        if (delivery.isDone() || remainingNanos <= 0) {
            terminalAttempts.release();
            return;
        }
        try {
            Duration timeout = Duration.ofNanos(Math.min(
                    TerminalReportDeliveryPolicy.REQUEST_TIMEOUT.toNanos(), remainingNanos));
            var builder = postRequest(round.body()).timeout(timeout);
            round.traceHeaders().forEach(builder::header);
            var response = httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            response.whenComplete((reply, failure) -> {
                terminalAttempts.release();
                if (delivery.isDone()) return;
                try {
                    onTerminalAttempt(round, attempt, reply, failure);
                } catch (RuntimeException | Error unexpected) {
                    delivery.completeExceptionally(unexpected);
                }
            });
        } catch (RuntimeException | Error failure) {
            terminalAttempts.release();
            delivery.completeExceptionally(failure);
        }
    }

    private void onTerminalAttempt(TerminalRound round, int attempt,
                                   HttpResponse<byte[]> response, Throwable failure) {
        var report = round.report();
        var delivery = round.delivery();
        if (failure == null && handleResponseStatus(response, report)) {
            if (attempt > 1) {
                LOG.info("event=final_state_report_retry_succeeded taskId={} reportSequence={} attempt={}",
                        report.taskId(), report.reportSequence(), attempt);
            }
            delivery.complete(null);
            return;
        }
        if (failure != null) {
            LOG.warn("event=status_report_failed taskId={} reportSequence={} error={}",
                    report.taskId(), report.reportSequence(), failure.toString());
        }
        if (attempt < MAX_RETRY_ATTEMPTS) {
            long backoffMs = round.backoff().next();
            LOG.warn("event=final_state_report_retry taskId={} reportSequence={} attempt={} nextBackoffMs={}",
                    report.taskId(), report.reportSequence(), attempt, backoffMs);
            CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS)
                    .execute(() -> enqueueTerminalAttempt(round, attempt + 1));
            return;
        }
        reportFailedCounter.add(1);
        LOG.error("event=final_state_report_exhausted taskId={} queryId={} state={} "
                        + "reportSequence={} maxAttempts={}",
                report.taskId(),
                report.queryId(),
                report.state(),
                report.reportSequence(),
                MAX_RETRY_ATTEMPTS);
        delivery.completeExceptionally(new IllegalStateException(
                "Terminal task report was not acknowledged: " + report.taskId(), failure));
    }

    private static long terminalRoundTimeoutMillis() {
        long timeout = MAX_RETRY_ATTEMPTS * TerminalReportDeliveryPolicy.REQUEST_TIMEOUT.toMillis();
        var backoff = BackoffStrategy.retry(INITIAL_BACKOFF_MS, MAX_BACKOFF_MS);
        for (int attempt = 1; attempt < MAX_RETRY_ATTEMPTS; attempt++) timeout += backoff.next();
        return timeout;
    }

    private record TerminalRound(TaskStatusReport report, HttpRequest.BodyPublisher body,
                                 Map<String, String> traceHeaders,
                                 BackoffStrategy backoff, long deadlineNanos, CompletableFuture<Void> delivery) { }

    private byte[] serializeReport(TaskStatusReport report) {
        try {
            return TaskStatusWire.encode(report);
        } catch (IllegalArgumentException e) {
            LOG.warn("event=status_report_serialize_failed taskId={} error={}",
                    report.taskId(),
                    e.getMessage());
            return null;
        }
    }

    private HttpRequest.Builder postRequest(HttpRequest.BodyPublisher body) {
        String url = baseUrl + "/v1/task/status-report";
        HttpRequest.Builder builder =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", TaskStatusWire.MEDIA_TYPE)
                        .header("Accept", TaskStatusWire.MEDIA_TYPE)
                        .POST(body);

        if (internalAuthSecret != null) {
            builder.header("Authorization", "Bearer " + internalAuthSecret);
        }

        return builder;
    }

    /** Sends a single progress POST. Returns true only when the Coordinator accepts it. */
    private boolean sendPost(byte[] body, TaskStatusReport report) {
        var builder = postRequest(HttpRequest.BodyPublishers.ofByteArray(body))
                .timeout(TerminalReportDeliveryPolicy.REQUEST_TIMEOUT);
        injectTraceContext(builder);
        try {
            HttpResponse<byte[]> response =
                    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            return handleResponseStatus(response, report);
        } catch (IOException e) {
            LOG.warn("event=status_report_failed taskId={} url={} error={}",
                    report.taskId(),
                    baseUrl,
                    e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=status_report_interrupted taskId={} url={}", report.taskId(), baseUrl);
            return false;
        }
    }

    /** HTTP status code indicating the report was accepted. */
    private static final int HTTP_OK = 200;

    /** HTTP status code indicating the worker is not registered. */
    private static final int HTTP_CONFLICT = 409;

    /** Returns true if the response indicates the report was accepted. */
    private boolean handleResponseStatus(HttpResponse<byte[]> response, TaskStatusReport report) {
        int statusCode = response.statusCode();
        if (statusCode == HTTP_OK) {
            try {
                if (response.headers().firstValue("Content-Type")
                        .filter(TaskStatusWire.MEDIA_TYPE::equalsIgnoreCase).isEmpty()) {
                    return false;
                }
                var ack = TaskStatusAck.parseFrom(response.body());
                return ack.getUnknownFields().asMap().isEmpty()
                        && report.taskId().equals(ack.getTaskId()) && report.reportSequence() == ack.getSequence()
                        && (ack.getDirective() == TaskStatusAck.Directive.CONTINUE
                            || retiredResultNotice(ack.getDirective(), report));
            } catch (IOException malformed) {
                LOG.warn("event=status_report_ack_malformed taskId={}", report.taskId(), malformed);
                return false;
            }
        }
        if (statusCode == HTTP_CONFLICT) {
            LOG.warn("event=status_report_rejected reason=worker_not_registered "
                            + "taskId={} statusCode=409", report.taskId());
            return false;
        }
        LOG.warn("event=status_report_error taskId={} statusCode={}", report.taskId(), statusCode);
        return false;
    }

    private static boolean retiredResultNotice(TaskStatusAck.Directive directive, TaskStatusReport report) {
        if (directive != TaskStatusAck.Directive.RETIRED_RESULT_NOTICE
                || !"OUTPUT".equals(report.taskType()) || !"FINISHED".equals(report.state())
                || report.writeResult() != null || report.canonicalEvidence() == null) return false;
        var actual = report.canonicalStatus();
        return actual.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                && actual.getReportOrigin() == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                && actual.getExecutionDrainState() == org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_UNSPECIFIED
                && !actual.hasError() && !actual.hasArtifactWork();
    }

    private void injectTraceContext(HttpRequest.Builder builder) {
        tracingProvider.inject(null, builder, HttpRequest.Builder::header);
    }

    private static String normalizeSecret(String secret) {
        return secret == null || secret.isBlank() ? null : secret;
    }

    static boolean isFinalState(String state) {
        return FINAL_STATE_FAILED.equals(state)
                || FINAL_STATE_FINISHED.equals(state)
                || FINAL_STATE_CANCELLED.equals(state);
    }

    /** Sleeps for the given duration. Extracted for testability. */
    void sleep(long millis) {
        ThreadSleeper.sleepQuietly(millis);
    }

    private static LongCounter deliveryFailures(MetricsProvider metrics) {
        return checkNotNull(metrics, "metricsProvider must not be null").createCounter(
                "task_status_report_failed",
                "Number of final-state task status reports that failed after all retries",
                "reports");
    }

    /** Bounds live HTTP requests independently of the number of retained terminal owners. */
    static final class TerminalAttempts {
        static final int MAX_ACTIVE =
                org.pangrova.transport.http.client.HttpClientConfig.DEFAULT.maxConnections();
        @GuardedBy("this")
        private final LinkedHashSet<Runnable> waiting = new LinkedHashSet<>();
        @GuardedBy("this")
        private int active;

        void startOrQueue(Runnable attempt, CompletableFuture<Void> round) {
            boolean start;
            synchronized (this) {
                start = active < MAX_ACTIVE;
                if (start) {
                    active++;
                } else {
                    if (waiting.size() >= TerminalReportDeliveryPolicy.MAX_RETAINED_IDENTITIES) {
                        throw new RejectedExecutionException("Terminal report transport queue is full");
                    }
                    waiting.add(attempt);
                }
            }
            if (start) attempt.run();
            else round.whenComplete((ignored, failure) -> removeWaiting(attempt));
        }

        synchronized void removeWaiting(Runnable attempt) {
            waiting.remove(attempt);
        }

        void release() {
            Runnable next;
            synchronized (this) {
                if (waiting.isEmpty()) {
                    active--;
                    return;
                }
                var iterator = waiting.iterator();
                next = iterator.next();
                iterator.remove();
            }
            // A synchronously failed client request can release a slot on entry.
            // Schedule queued work rather than recursing through the queue.
            ForkJoinPool.commonPool().execute(next);
        }
    }
}
