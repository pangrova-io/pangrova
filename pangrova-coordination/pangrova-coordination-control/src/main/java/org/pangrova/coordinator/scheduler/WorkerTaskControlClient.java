package org.pangrova.coordinator.scheduler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.task.TaskRuntimeFilterWire;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.task.CanonicalExecutionDrainReports;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sends task control-plane requests after a task has been dispatched. */
final class WorkerTaskControlClient {
    private static final int HTTP_TIMEOUT_SECONDS = 30;
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(3);

    private static final Logger LOG = LoggerFactory.getLogger(
            WorkerTaskControlClient.class);
    private static final String INTERNAL_AUTH_HEADER = "X-Internal-Auth";
    private static final int HTTP_OK = 200;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NO_CONTENT = 204;
    private static final int HTTP_CONFLICT = 409;

    private final HttpClient httpClient;
    private final TracingProvider tracingProvider;
    private final String internalAuthSecret;

    WorkerTaskControlClient(
            HttpClient httpClient,
            TracingProvider tracingProvider,
            String internalAuthSecret) {
        this.httpClient = httpClient;
        this.tracingProvider = tracingProvider;
        this.internalAuthSecret = internalAuthSecret;
    }

    CompletableFuture<Boolean> injectDynamicFilter(TaskAttemptId attempt,
            RuntimeFilterBinding update, WorkerInfo worker) {
        if (!attempt.getWorkerExecutionIdentity().equals(worker.workerExecutionIdentity().toProto())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Runtime filter Worker target differs"));
        }
        final URI endpoint;
        try {
            endpoint = new URI("http", null, worker.host(), worker.httpPort(),
                    "/v1/task/" + attempt.getTaskId() + "/dynamic-filter", null, null);
        } catch (URISyntaxException malformed) {
            return CompletableFuture.failedFuture(malformed);
        }
        var request = decorate(HttpRequest.newBuilder()).uri(endpoint).timeout(RECOVERY_TIMEOUT)
                .header("Content-Type", TaskRuntimeFilterWire.MEDIA_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(TaskRuntimeFilterWire.encode(attempt, update))).build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding()).thenApply(response -> {
            if (response.statusCode() == HTTP_NO_CONTENT) return true;
            if (response.statusCode() == HTTP_CONFLICT) return false;
            throw new IllegalStateException("Worker runtime filter rejected with HTTP " + response.statusCode());
        });
    }

    SchedulerCancelResult cancelTask(TaskAdmissionSealRequest seal, String workerHost, int httpPort) {
        String taskId = seal.getAttempt().getTaskId();
        try {
            HttpResponse<byte[]> response = httpClient.send(
                    cancelRequest(seal, workerHost, httpPort, Duration.ofSeconds(HTTP_TIMEOUT_SECONDS)),
                    HttpResponse.BodyHandlers.ofByteArray());
            return mapCancelResponse(response, seal.getAttempt());
        } catch (IOException failure) {
            LOG.warn("event=task_cancel_io_error taskId={} error={}", taskId, failure.getMessage());
            return SchedulerCancelResult.failed("IO error: " + failure.getMessage(), true);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            LOG.warn("event=task_cancel_interrupted taskId={}", taskId);
            return SchedulerCancelResult.failed("Interrupted", false);
        }
    }

    CompletableFuture<SchedulerCancelResult> recoverTask(
            TaskAdmissionSealRequest seal, String workerHost, int httpPort) {
        var response = httpClient.sendAsync(cancelRequest(seal, workerHost, httpPort, RECOVERY_TIMEOUT),
                HttpResponse.BodyHandlers.ofByteArray());
        return response.copy().orTimeout(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .handle((received, failure) -> {
                    if (failure == null) return mapCancelResponse(received, seal.getAttempt());
                    response.cancel(true);
                    return SchedulerCancelResult.failed(failure.getMessage(), true);
                });
    }

    private HttpRequest cancelRequest(TaskAdmissionSealRequest seal, String workerHost, int httpPort,
            Duration timeout) {
        URI endpoint;
        try {
            endpoint = new URI("http", null, workerHost, httpPort,
                    "/v1/task/" + seal.getAttempt().getTaskId(), null, null);
        } catch (URISyntaxException malformed) {
            throw new IllegalArgumentException("Invalid Worker task control address", malformed);
        }
        return decorate(HttpRequest.newBuilder()).uri(endpoint)
                .header("Content-Type", "application/x-protobuf")
                .header("Accept", "application/x-protobuf")
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(seal.toByteArray()))
                .timeout(timeout).build();
    }

    private HttpRequest.Builder decorate(HttpRequest.Builder builder) {
        tracingProvider.inject(null, builder, HttpRequest.Builder::header);
        if (internalAuthSecret != null && !internalAuthSecret.isEmpty()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
        return builder;
    }

    private static SchedulerCancelResult mapCancelResponse(
            HttpResponse<byte[]> response, TaskAttemptId issued) {
        String taskId = issued.getTaskId();
        if (response.statusCode() == HTTP_OK) {
            String contentType = response.headers().firstValue("Content-Type").orElse("");
            if (!contentType.split(";", 2)[0].equalsIgnoreCase("application/x-protobuf")) {
                return SchedulerCancelResult.failed(
                    "Worker did not return a canonical drain receipt media type", false);
            }
            try {
                var actual = CanonicalExecutionDrainReports.decode(response.body(), issued);
                return SchedulerCancelResult.drained(actual);
            } catch (IllegalArgumentException malformed) {
                return SchedulerCancelResult.failed(
                    "Worker execution drain receipt rejected: " + malformed.getMessage(), false);
            }
        }
        if (response.statusCode() == HTTP_ACCEPTED) return SchedulerCancelResult.pending();
        LOG.warn("event=task_cancel_failed taskId={} statusCode={}",
                taskId, response.statusCode());
        return SchedulerCancelResult.failed(
                "Worker returned HTTP " + response.statusCode(), true);
    }
}
