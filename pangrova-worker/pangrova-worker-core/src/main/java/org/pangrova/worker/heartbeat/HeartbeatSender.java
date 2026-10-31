package org.pangrova.worker.heartbeat;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.LongConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Periodically sends heartbeat HTTP POST to Coordinator /v1/worker/heartbeat.
 * Uses self-rescheduling with ±10% jitter to avoid thundering herd.
 * On HTTP failure: logs warning, retries on next interval — never crashes.
 *
 * <p>Concurrency strategy: {@code AtomicBoolean running} guards lifecycle
 * transitions (started→stopped). {@code volatile} for scheduled task reference;
 * self-rescheduling on ScheduledExecutorService thread. Stopping cancels future
 * sends, but every stop attempt retries owned HTTP resource cleanup. The injected
 * scheduler owner must separately join its actual callbacks.</p>
 *
 * <p>Memory reporting: the {@code payloadSupplier} is invoked at send time
 * on the scheduler thread. Because {@link org.pangrova.worker.resource.memory.MemoryArbitrator}
 * uses {@link java.util.concurrent.atomic.AtomicLong} for all counters,
 * any memory released by {@code TaskLifecycleManager.releaseMemoryIfNeeded()}
 * on a task-completion thread is immediately visible to the heartbeat thread
 * without additional synchronization. This ensures the Coordinator receives
 * up-to-date {@code availableMemoryBytes} on the next heartbeat after a task
 * completes.</p>
 */
@ThreadSafe
public class HeartbeatSender {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatSender.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);
    private static final String HEARTBEAT_PATH = "/v1/worker/heartbeat";

    /** HTTP status indicating the heartbeat was accepted. */
    private static final int HTTP_OK = 200;

    /** HTTP status indicating a registration conflict. */
    private static final int HTTP_CONFLICT = 409;

    /** Max consecutive failures before logging at ERROR level. */
    private static final int FAILURE_ESCALATION_THRESHOLD = 5;
    /** Max backoff multiplier applied to base interval on sustained failures. */
    private static final int MAX_BACKOFF_MULTIPLIER = 4;
    /** Consecutive failures before entering degraded mode (stop accepting tasks). */
    static final int DEGRADATION_THRESHOLD = 10;

    private final HeartbeatConfig config;
    private final Supplier<HeartbeatPayload> payloadSupplier;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final org.pangrova.common.http.ManagedHttpClient managedClient;
    private final ObjectMapper mapper;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> scheduledTask;
    private volatile int consecutiveFailures;
    private volatile Runnable reRegistrationCallback;
    private volatile Runnable degradationCallback;
    private volatile LongConsumer coordinatorEpochObserver;
    private final AtomicBoolean degraded = new AtomicBoolean(false);
    private final String internalAuthSecret;

    public HeartbeatSender(HeartbeatConfig config,
                           Supplier<HeartbeatPayload> payloadSupplier,
                           ScheduledExecutorService scheduler) {
        this(config, payloadSupplier, scheduler, (String) null);
    }

    /** Creates a sender with the given auth secret and a default HTTP client. */
    public HeartbeatSender(HeartbeatConfig config,
                           Supplier<HeartbeatPayload> payloadSupplier,
                           ScheduledExecutorService scheduler,
                           String internalAuthSecret) {
        this(config, payloadSupplier, scheduler, createManagedClient(config, payloadSupplier, scheduler),
                internalAuthSecret);
    }

    HeartbeatSender(HeartbeatConfig config, Supplier<HeartbeatPayload> payloadSupplier,
                    ScheduledExecutorService scheduler, org.pangrova.common.http.ManagedHttpClient managed,
                    String internalAuthSecret) {
        this.config = checkNotNull(config, "config must not be null");
        this.payloadSupplier = checkNotNull(payloadSupplier, "payloadSupplier must not be null");
        this.scheduler = checkNotNull(scheduler, "scheduler must not be null");
        this.managedClient = checkNotNull(managed, "managed client must not be null");
        this.httpClient = managed.httpClient();
        this.mapper = JsonMapperFactory.shared();
        this.internalAuthSecret = internalAuthSecret;
    }

    private static org.pangrova.common.http.ManagedHttpClient createManagedClient(
            HeartbeatConfig config, Supplier<HeartbeatPayload> payloadSupplier, ScheduledExecutorService scheduler) {
        checkNotNull(config, "config must not be null");
        checkNotNull(payloadSupplier, "payloadSupplier must not be null");
        checkNotNull(scheduler, "scheduler must not be null");
        return HttpClientFactory.createDefault();
    }

    public HeartbeatSender(HeartbeatConfig config,
                           Supplier<HeartbeatPayload> payloadSupplier,
                           ScheduledExecutorService scheduler,
                           HttpClient httpClient) {
        this(config, payloadSupplier, scheduler, httpClient, null);
    }

    /** Creates a sender with an externally provided HTTP client. */
    public HeartbeatSender(HeartbeatConfig config,
                           Supplier<HeartbeatPayload> payloadSupplier,
                           ScheduledExecutorService scheduler,
                           HttpClient httpClient,
                           String internalAuthSecret) {
        this.config = checkNotNull(config, "config must not be null");
        this.payloadSupplier = checkNotNull(payloadSupplier, "payloadSupplier must not be null");
        this.scheduler = checkNotNull(scheduler, "scheduler must not be null");
        this.httpClient = checkNotNull(httpClient, "httpClient must not be null");
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.mapper = JsonMapperFactory.shared();
        this.internalAuthSecret = internalAuthSecret;
    }

    /** Starts periodic heartbeat sending. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("event=heartbeat_sender_already_started workerId={}", config.workerId());
            return;
        }
        // Registration has completed before this sender starts. Publish the
        // initialized resource snapshot now so the Coordinator can admit tasks
        // without waiting for the first periodic interval.
        scheduledTask = scheduler.schedule(this::sendAndReschedule, 0, TimeUnit.MILLISECONDS);
        LOG.info("event=heartbeat_sender_started workerId={} baseIntervalMs={} target={}",
                config.workerId(), config.intervalMs(), config.coordinatorUrl());
    }

    /** Stops heartbeat sending. */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            ScheduledFuture<?> task = scheduledTask;
            if (task != null) task.cancel(false);
        }
        // BUG-W10: Do NOT call scheduler.shutdownNow() — the executor is injected
        // and may be shared with other components (health check, metrics).
        // Cancellation is not callback-exit evidence. The executor owner must
        // shutdown and await its actual threads during lifecycle teardown.
        if (managedClient != null) {
            managedClient.close();
        }
        LOG.info("event=heartbeat_sender_stopped workerId={}", config.workerId());
    }

    /**
     * Returns whether the heartbeat sender is currently running.
     * Package-private for testability.
     */
    boolean isRunning() {
        return running.get();
    }

    /**
     * Sets a callback invoked when the coordinator responds with 409 and
     * {@code action: re-register}. The worker bootstrap should set this
     * to trigger a full re-registration cycle (C-WK-01/C-WK-03).
     */
    public void setReRegistrationCallback(Runnable callback) {
        this.reRegistrationCallback = callback;
    }

    /**
     * Sets a callback invoked when the worker enters degraded mode after
     * {@link #DEGRADATION_THRESHOLD} consecutive heartbeat failures.
     * The callback should stop accepting new tasks and drain existing ones.
     */
    public void setDegradationCallback(Runnable callback) {
        this.degradationCallback = callback;
    }

    /**
     * Sets the sink for authenticated Coordinator epochs returned by heartbeat.
     * The sink is invoked before a response is counted as successful.
     */
    public void setCoordinatorEpochObserver(LongConsumer observer) {
        this.coordinatorEpochObserver = observer;
    }

    /**
     * Returns true if the worker has entered degraded mode due to
     * sustained heartbeat failures.
     */
    public boolean isDegraded() {
        return degraded.get();
    }

    private void sendAndReschedule() {
        if (!running.get()) return;
        sendHeartbeat();
        if (running.get()) {
            long nextMs = nextIntervalMs();
            // C-WK-07: Apply backoff multiplier on sustained failures
            int failures = consecutiveFailures;
            if (failures > 0) {
                int multiplier = Math.min(1 + failures, MAX_BACKOFF_MULTIPLIER);
                nextMs = nextMs * multiplier;
            }
            LOG.debug("event=heartbeat_interval_actual intervalMs={} failures={} workerId={}",
                    nextMs, failures, config.workerId());
            scheduledTask = scheduler.schedule(this::sendAndReschedule, nextMs, TimeUnit.MILLISECONDS);
        }
    }

    void sendHeartbeat() {
        HeartbeatPayload payload = collectPayload();
        if (payload == null) {
            return;
        }
        byte[] body = serialize(payload);
        if (body == null) {
            return;
        }
        postHeartbeat(body);
    }

    /**
     * Calculates the next heartbeat interval with ±10% random jitter.
     * Package-private for testability.
     */
    long nextIntervalMs() {
        long base = config.intervalMs();
        long jitter = base / 10;
        return ThreadLocalRandom.current().nextLong(base - jitter, base + jitter + 1);
    }

    private HeartbeatPayload collectPayload() {
        try {
            return payloadSupplier.get();
        } catch (RuntimeException e) {
            LOG.warn("event=heartbeat_payload_failed error={}", e.getMessage());
            return null;
        }
    }

    private byte[] serialize(HeartbeatPayload payload) {
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            LOG.warn("event=heartbeat_serialize_failed error={}", e.getMessage());
            return null;
        }
    }

    private void postHeartbeat(byte[] body) {
        String url = config.coordinatorUrl() + HEARTBEAT_PATH;
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(HTTP_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            builder.header("Authorization", "Bearer " + internalAuthSecret);
        }
        HttpRequest request = builder.build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            handleResponse(response);
        } catch (java.io.IOException e) {
            recordFailureAndLog(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=heartbeat_interrupted workerId={}", config.workerId());
        }
    }

    private void handleResponse(HttpResponse<String> response) {
        if (response.statusCode() == HTTP_OK) {
            handleSuccessResponse(response.body());
        } else if (isReRegistrationRequired(response)) {
            handleReRegistrationResponse();
        } else {
            handleErrorResponse(response.statusCode());
        }
    }

    private void handleSuccessResponse(String responseBody) {
        try {
            org.pangrova.protocol.rest.worker.WorkerControlResponse controlResponse =
                    mapper.readValue(responseBody,
                            org.pangrova.protocol.rest.worker.WorkerControlResponse.class);
            controlResponse.requireStatus("ok");
            long epoch = controlResponse.requireCoordinatorEpoch();
            LongConsumer observer = coordinatorEpochObserver;
            if (observer != null) {
                observer.accept(epoch);
            }
        } catch (Exception e) {
            int failures = ++consecutiveFailures;
            LOG.warn("event=heartbeat_control_response_invalid workerId={} "
                            + "consecutiveFailures={} error={}",
                    config.workerId(), failures, e.getMessage());
            checkDegradationThreshold(failures);
            return;
        }
        if (consecutiveFailures > 0) {
            LOG.info("event=heartbeat_recovered workerId={} previousFailures={}",
                    config.workerId(), consecutiveFailures);
        }
        consecutiveFailures = 0;
        if (degraded.compareAndSet(true, false)) {
            LOG.info("event=heartbeat_degradation_cleared workerId={}", config.workerId());
            invokeCallbackSafely(reRegistrationCallback, "reregistration_after_recovery_failed");
        }
    }

    private boolean isReRegistrationRequired(HttpResponse<String> response) {
        return response.statusCode() == HTTP_CONFLICT
                && response.body() != null
                && response.body().contains("re-register");
    }

    private void handleReRegistrationResponse() {
        // C-WK-01/C-WK-03: Coordinator doesn't know this worker.
        // Trigger re-registration so the worker becomes schedulable.
        LOG.warn("event=heartbeat_reregister_required workerId={}", config.workerId());
        consecutiveFailures = 0;
        invokeCallbackSafely(reRegistrationCallback, "reregistration_callback_failed");
    }

    private void handleErrorResponse(int statusCode) {
        int failures = ++consecutiveFailures;
        if (failures >= FAILURE_ESCALATION_THRESHOLD) {
            LOG.error("event=heartbeat_sustained_failure statusCode={} workerId={} consecutiveFailures={}",
                    statusCode, config.workerId(), failures);
        } else {
            LOG.warn("event=heartbeat_error statusCode={} workerId={} consecutiveFailures={}",
                    statusCode, config.workerId(), failures);
        }
        checkDegradationThreshold(failures);
    }

    private void recordFailureAndLog(java.io.IOException e) {
        int failures = ++consecutiveFailures;
        if (failures >= FAILURE_ESCALATION_THRESHOLD) {
            LOG.error("event=heartbeat_sustained_failure workerId={} consecutiveFailures={} error={}",
                    config.workerId(), failures, e.getMessage());
        } else {
            LOG.warn("event=heartbeat_failed workerId={} consecutiveFailures={} error={}",
                    config.workerId(), failures, e.getMessage());
        }
        checkDegradationThreshold(failures);
    }

    private void invokeCallbackSafely(Runnable callback, String failureEvent) {
        if (callback == null) {
            return;
        }
        try {
            callback.run();
        } catch (Exception e) {
            LOG.error("event={} workerId={} error={}",
                    failureEvent, config.workerId(), e.getMessage(), e);
        }
    }

    /**
     * Enters degraded mode when consecutive failures reach the threshold.
     * In degraded mode the worker stops accepting new tasks and drains existing ones.
     * Recovery occurs when a heartbeat succeeds, triggering re-registration.
     */
    private void checkDegradationThreshold(int failures) {
        if (failures >= DEGRADATION_THRESHOLD && degraded.compareAndSet(false, true)) {
            LOG.error("event=heartbeat_degradation_entered workerId={} consecutiveFailures={} threshold={}",
                    config.workerId(), failures, DEGRADATION_THRESHOLD);
            Runnable callback = degradationCallback;
            if (callback != null) {
                try {
                    callback.run();
                } catch (Exception e) {
                    LOG.error("event=degradation_callback_failed workerId={} error={}",
                            config.workerId(), e.getMessage(), e);
                }
            }
        }
    }
}
