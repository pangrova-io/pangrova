package org.pangrova.worker.task;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports task status to Coordinator via real HTTP POST /v1/task/status-report.
 * Uses java.net.http.HttpClient (JDK 11+), Jackson for JSON serialization.
 * Injects W3C trace context into outgoing HTTP headers for distributed tracing.
 * Client side only — no Jetty dependency needed.
 *
 * <p>Final-state reports (FAILED/FINISHED) use exponential backoff retry to
 * guarantee at-least-once delivery. Non-final-state reports (RUNNING etc.)
 * remain best-effort — loss is compensated by the next heartbeat cycle.</p>
 *
 * <p>Each status report carries a {@code reportSequence} field that serves as
 * an idempotency key. The Coordinator deduplicates by {@code taskId + reportSequence},
 * making retried deliveries safe.</p>
 *
 * <p>On HTTP 409 (Conflict), the report is silently dropped — re-registration
 * is handled by WorkerBootstrap, not by the reporter.</p>
 */
public class RemoteTaskStatusReporter implements TaskStatusReporter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskStatusReporter.class);

    static final int MAX_RETRY_ATTEMPTS = 5;
    static final long INITIAL_BACKOFF_MS = 1_000L;
    static final long MAX_BACKOFF_MS = 30_000L;
    private static final String FINAL_STATE_FAILED = "FAILED";
    private static final String FINAL_STATE_FINISHED = "FINISHED";

    private final HttpClient httpClient;
    private final org.pangrova.common.http.ManagedHttpClient managedClient;
    private final ObjectMapper mapper;
    private volatile String baseUrl;
    private final TracingProvider tracingProvider;
    private final LongCounter reportFailedCounter;

    /**
     * Constructs reporter with default HTTP client and NoOp metrics.
     *
     * @param coordinatorBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     * @param tracingProvider    tracing context propagation
     */
    public RemoteTaskStatusReporter(String coordinatorBaseUrl,
                                  TracingProvider tracingProvider) {
        org.pangrova.common.http.ManagedHttpClient managed =
                org.pangrova.common.http.HttpClientFactory.createDefault();
        this.managedClient = managed;
        this.httpClient = managed.httpClient();
        checkNotNull(coordinatorBaseUrl, "coordinatorBaseUrl must not be null");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.baseUrl = coordinatorBaseUrl;
        this.mapper = JsonMapperFactory.shared();
        this.tracingProvider = tracingProvider;
        this.reportFailedCounter = new org.pangrova.observability.noop.NoOpMetricsProvider()
                .createCounter("noop", "noop", "1");
    }

    /**
     * Constructs reporter with custom HTTP client and metrics provider.
     *
     * @param coordinatorBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     * @param httpClient         JDK HttpClient for sending requests
     * @param tracingProvider    tracing context propagation
     * @param metricsProvider    metrics recording (nullable — uses no-op counter if null)
     */
    public RemoteTaskStatusReporter(String coordinatorBaseUrl,
                                  HttpClient httpClient, TracingProvider tracingProvider,
                                  MetricsProvider metricsProvider) {
        checkNotNull(coordinatorBaseUrl, "coordinatorBaseUrl must not be null");
        checkNotNull(httpClient, "httpClient must not be null");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        this.baseUrl = coordinatorBaseUrl;
        this.httpClient = httpClient;
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.mapper = JsonMapperFactory.shared();
        this.tracingProvider = tracingProvider;
        this.reportFailedCounter = metricsProvider != null
                ? metricsProvider.createCounter(
                        "task_status_report_failed",
                        "Number of final-state task status reports that failed after all retries",
                        "reports")
                : NoOpCounter.INSTANCE;
    }

    /**
     * Updates the Coordinator target address for status reporting.
     * Thread-safe: baseUrl is volatile, so subsequent report() calls
     * will use the new target without synchronization.
     *
     * @param newBaseUrl full base URL including scheme (e.g. "https://coordinator:8443")
     */
    public void updateTarget(String newBaseUrl) {
        checkNotNull(newBaseUrl, "newBaseUrl must not be null");
        this.baseUrl = newBaseUrl;
        LOG.info("event=status_reporter_target_update baseUrl={}", this.baseUrl);
    }

    /**
     * Shuts down the managed HTTP client thread pool if this reporter owns it.
     * No-op if the HttpClient was externally provided.
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
        byte[] body = serializeReport(report);
        if (body == null) {
            if (isFinalState(report.state())) {
                reportFailedCounter.add(1);
                LOG.error("event=final_state_report_serialize_failed taskId={} queryId={} state={} "
                        + "reportSequence={}", report.taskId(), report.queryId(),
                        report.state(), report.reportSequence());
            }
            return;
        }

        if (isFinalState(report.state())) {
            sendWithRetry(body, report);
        } else if ("RUNNING".equals(report.state())) {
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

    /**
     * Sends a final-state report with exponential backoff retry.
     * Initial backoff: 1s, max backoff: 30s, max attempts: 5.
     * On exhaustion, records {@code task_status_report_failed} metric.
     */
    private void sendWithRetry(byte[] body, TaskStatusReport report) {
        BackoffStrategy backoff = BackoffStrategy.retry(INITIAL_BACKOFF_MS, MAX_BACKOFF_MS);
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            boolean success = sendPost(body, report);
            if (success) {
                if (attempt > 1) {
                    LOG.info("event=final_state_report_retry_succeeded taskId={} queryId={} "
                            + "state={} reportSequence={} attempt={}",
                            report.taskId(), report.queryId(), report.state(),
                            report.reportSequence(), attempt);
                }
                return;
            }
            if (attempt < MAX_RETRY_ATTEMPTS) {
                long backoffMs = backoff.next();
                LOG.warn("event=final_state_report_retry taskId={} queryId={} state={} "
                        + "reportSequence={} attempt={} nextBackoffMs={}",
                        report.taskId(), report.queryId(), report.state(),
                        report.reportSequence(), attempt, backoffMs);
                sleep(backoffMs);
            }
        }
        reportFailedCounter.add(1);
        LOG.error("event=final_state_report_exhausted taskId={} queryId={} state={} "
                + "reportSequence={} maxAttempts={}",
                report.taskId(), report.queryId(), report.state(),
                report.reportSequence(), MAX_RETRY_ATTEMPTS);
    }

    private byte[] serializeReport(TaskStatusReport report) {
        try {
            return mapper.writeValueAsBytes(report);
        } catch (IOException e) {
            LOG.warn("event=status_report_serialize_failed taskId={} error={}",
                    report.taskId(), e.getMessage());
            return null;
        }
    }

    /**
     * Sends a single HTTP POST. Returns true on success (HTTP 200 or 409).
     */
    private boolean sendPost(byte[] body, TaskStatusReport report) {
        String url = baseUrl + "/v1/task/status-report";
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));

        injectTraceContext(builder);

        try {
            HttpResponse<String> response = httpClient.send(
                    builder.build(), HttpResponse.BodyHandlers.ofString());
            return handleResponseStatus(response, report);
        } catch (IOException e) {
            LOG.warn("event=status_report_failed taskId={} url={} error={}",
                    report.taskId(), url, e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=status_report_interrupted taskId={} url={}",
                    report.taskId(), url);
            return false;
        }
    }

    /** HTTP status code indicating the report was accepted. */
    private static final int HTTP_OK = 200;

    /** HTTP status code indicating the worker is not registered. */
    private static final int HTTP_CONFLICT = 409;

    /**
     * Returns true if the response indicates the report was accepted or already known.
     */
    private boolean handleResponseStatus(HttpResponse<String> response, TaskStatusReport report) {
        int statusCode = response.statusCode();
        if (statusCode == HTTP_OK) {
            return true;
        }
        if (statusCode == HTTP_CONFLICT) {
            LOG.warn("event=status_report_rejected reason=worker_not_registered "
                    + "taskId={} statusCode=409 body={}",
                    report.taskId(), response.body());
            return true;
        }
        LOG.warn("event=status_report_error taskId={} statusCode={} body={}",
                report.taskId(), statusCode, response.body());
        return false;
    }

    private void injectTraceContext(HttpRequest.Builder builder) {
        tracingProvider.inject(null, builder, HttpRequest.Builder::header);
    }

    static boolean isFinalState(String state) {
        return FINAL_STATE_FAILED.equals(state) || FINAL_STATE_FINISHED.equals(state);
    }

    /**
     * Sleeps for the given duration. Extracted for testability.
     */
    void sleep(long millis) {
        ThreadUtils.sleepQuietly(millis);
    }

    /**
     * No-op counter for when no MetricsProvider is supplied.
     */
    private enum NoOpCounter implements LongCounter {
        INSTANCE;

        @Override
        public void add(long value) {
            // intentionally empty
        }
    }
}
