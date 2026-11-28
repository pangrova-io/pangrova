package org.pangrova.worker.bootstrap.registration;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrency.ThreadSleeper;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.SerializationException;
import org.pangrova.common.http.HttpResponseCheck;
import org.pangrova.transport.http.client.HttpClientFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.worker.WorkerControlResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP client for Worker registration/deregistration with Coordinator.
 * First POST uses /v1/workers/register; retries and repair use conditional /v1/workers/refresh.
 * DELETE /v1/workers/{workerId} withdraws only the original process on shutdown.
 * One client owns one logical ID and execution incarnation, retained before sending the
 * first registration attempt. DELETE carries that original identity as JSON, including
 * retries after an unknown response; the client cannot switch to another process.
 *
 * <p>Serialization failures throw {@link SerializationException} with error code
 * {@link PangrovaErrorCode#WORKER_REGISTRATION_SERIALIZATION_FAILED} instead of
 * returning null, ensuring the caller can detect and retry registration.
 */
public final class WorkerRegistrationClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerRegistrationClient.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);
    private static final String REGISTER_PATH = "/v1/workers/register";
    private static final int MAX_DEREGISTER_ATTEMPTS = 3;
    private static final long DEREGISTER_BACKOFF_MS = 1_000;

    /** Inclusive lower bound of the HTTP success status range. */

    /** Exclusive upper bound of the HTTP success status range. */

    private final String coordinatorUrl;
    private final HttpClient httpClient;
    private final org.pangrova.transport.http.client.ManagedHttpClient managedClient;
    private final ObjectMapper mapper;
    private final String internalAuthSecret;
    // This client owns one registration attempt domain, including an unknown HTTP outcome.
    private final java.util.concurrent.atomic.AtomicReference<RegistrationTarget> registration =
            new java.util.concurrent.atomic.AtomicReference<>();

    private record RegistrationTarget(String workerId,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity identity) {}


    public WorkerRegistrationClient(String coordinatorUrl) {
        this(coordinatorUrl, (String) null);
    }

    /** Creates a client with the given coordinator URL and auth secret. */
    public WorkerRegistrationClient(String coordinatorUrl, String internalAuthSecret) {
        org.pangrova.transport.http.client.ManagedHttpClient managed = HttpClientFactory.createDefault();
        this.coordinatorUrl = checkNotNull(coordinatorUrl, "coordinatorUrl must not be null");
        this.managedClient = managed;
        this.httpClient = managed.httpClient();
        this.mapper = JsonMapperFactory.shared();
        this.internalAuthSecret = internalAuthSecret;
    }

    public WorkerRegistrationClient(String coordinatorUrl, HttpClient httpClient) {
        this(coordinatorUrl, httpClient, null);
    }

    /** Creates a client over an externally provided HTTP client. */
    public WorkerRegistrationClient(String coordinatorUrl, HttpClient httpClient, String internalAuthSecret) {
        this.coordinatorUrl = checkNotNull(coordinatorUrl, "coordinatorUrl must not be null");
        this.httpClient = checkNotNull(httpClient, "httpClient must not be null");
        this.managedClient = null; // externally provided — caller owns lifecycle
        this.mapper = JsonMapperFactory.shared();
        this.internalAuthSecret = internalAuthSecret;
    }

    /**
     * Shuts down the managed HTTP client thread pool if this client owns it.
     * No-op if the HttpClient was externally provided.
     */
    @Override
    public void close() {
        if (managedClient != null) {
            managedClient.close();
        }
    }

    /** Registers this worker with the coordinator. */
    public WorkerControlResponse register(WorkerRegistrationRequest request) {
        byte[] body = serialize(request);
        var target = new RegistrationTarget(request.workerId(), request.workerExecutionIdentity());
        RegistrationTarget previous = registration.getAndUpdate(existing -> {
            if (existing != null && !existing.equals(target)) {
                throw new IllegalStateException("Registration client cannot change its Worker incarnation");
            }
            return target;
        });
        String url = coordinatorUrl + (previous == null ? REGISTER_PATH : "/v1/workers/refresh");
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(HTTP_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            builder.header("Authorization", "Bearer " + internalAuthSecret);
        }
        return sendControlRequest(builder.build(), "register");
    }

    /** Withdraws this client's exact attempted registration, never a later process using its ID. */
    public void deregister(String workerId) {
        RegistrationTarget target = registration.get();
        if (target == null || !target.workerId().equals(workerId)) {
            throw new IllegalStateException("No matching Worker registration belongs to this client");
        }
        byte[] identity;
        try {
            identity = mapper.writeValueAsBytes(target.identity());
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("Cannot serialize Worker withdrawal identity", failure);
        }
        String url = coordinatorUrl + "/v1/workers/" + workerId;
        HttpRequest httpReq = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("Content-Type", "application/json")
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(identity))
                .build();
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            httpReq = HttpRequest.newBuilder(httpReq, (name, value) -> true)
                    .header("Authorization", "Bearer " + internalAuthSecret)
                    .build();
        }
        sendDeregister(httpReq, workerId);
    }

    private void sendDeregister(HttpRequest request, String workerId) {
        for (int attempt = 1; attempt <= MAX_DEREGISTER_ATTEMPTS; attempt++) {
            if (trySendDeregister(request, workerId, attempt)) return;
            if (attempt < MAX_DEREGISTER_ATTEMPTS) ThreadSleeper.sleepQuietly(DEREGISTER_BACKOFF_MS);
        }
        LOG.error("event=worker_deregistration_failed maxAttempts={} workerId={}",
                MAX_DEREGISTER_ATTEMPTS, workerId);
        throw new IllegalStateException("Worker deregistration was not acknowledged: " + workerId);
    }

    private boolean trySendDeregister(HttpRequest request, String workerId, int attempt) {
        try {
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());
            if (HttpResponseCheck.isSuccess(response.statusCode())) {
                mapper.readValue(response.body(), WorkerControlResponse.class).requireStatus("deregistered");
                LOG.info("event=worker_deregistration_ack workerId={} body={}",
                        workerId, response.body());
                return true;
            }
            LOG.warn("event=worker_deregister_http_error attempt={} maxAttempts={} statusCode={} body={}",
                    attempt, MAX_DEREGISTER_ATTEMPTS, response.statusCode(), response.body());
        } catch (java.io.IOException e) {
            LOG.warn("event=worker_deregister_io_error attempt={} maxAttempts={} error={}",
                    attempt, MAX_DEREGISTER_ATTEMPTS, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=worker_deregister_interrupted workerId={}", workerId);
            throw new IllegalStateException("Worker deregistration interrupted: " + workerId, e);
        }
        return false;
    }

    private WorkerControlResponse sendControlRequest(HttpRequest request, String operation) {
        try {
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());
            if (HttpResponseCheck.isSuccess(response.statusCode())) {
                LOG.info("event=worker_{}_succeeded statusCode={}", operation, response.statusCode());
                WorkerControlResponse controlResponse = mapper.readValue(
                        response.body(), WorkerControlResponse.class);
                controlResponse.requireStatus("registered");
                controlResponse.requireCoordinatorEpoch();
                return controlResponse;
            }
            LOG.warn("event=worker_{}_http_error statusCode={} body={}",
                    operation, response.statusCode(), response.body());
            throw new java.io.IOException(
                    "Worker " + operation + " failed: HTTP " + response.statusCode()
                            + " — " + response.body());
        } catch (java.io.IOException e) {
            LOG.warn("event=worker_{}_io_error error={}", operation, e.getMessage());
            throw new RuntimeException("Worker " + operation + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Worker " + operation + " interrupted", e);
        }
    }

    /**
     * Serializes the registration request to JSON bytes.
     *
     * @throws SerializationException with {@link PangrovaErrorCode#WORKER_REGISTRATION_SERIALIZATION_FAILED}
     *         if serialization fails, instead of returning null
     */
    public byte[] serialize(WorkerRegistrationRequest request) {
        try {
            request.requireBoundedMetadata(mapper);
            byte[] body = mapper.writeValueAsBytes(request);
            if (body.length > WorkerRegistrationRequest.MAX_REGISTRATION_BODY_BYTES) {
                throw new IllegalArgumentException("Worker registration exceeds maximum JSON body size");
            }
            return body;
        } catch (Exception e) {
            LOG.error("event=registration_serialize_failed errorCode={} workerId={} error={}",
                    PangrovaErrorCode.WORKER_REGISTRATION_SERIALIZATION_FAILED.getCode(),
                    request.workerId(),
                    e.getMessage(), e);
            throw new SerializationException(
                    PangrovaErrorCode.WORKER_REGISTRATION_SERIALIZATION_FAILED,
                    "Failed to serialize worker registration request for workerId=" + request.workerId(),
                    e);
        }
    }
}
