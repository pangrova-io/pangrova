package org.pangrova.coordinator.scheduler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.protocol.rest.task.InjectDynamicFilterRequest;
import org.pangrova.protocol.rest.task.UpstreamCommitNotification;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sends task control-plane requests after a task has been dispatched. */
final class WorkerTaskControlClient {
    private static final int HTTP_TIMEOUT_SECONDS = 30;

    private static final Logger LOG = LoggerFactory.getLogger(
            WorkerTaskControlClient.class);
    private static final String INTERNAL_AUTH_HEADER = "X-Internal-Auth";
    private static final int HTTP_OK = 200;
    private static final int HTTP_ACCEPTED = 202;
    private static final int DEFAULT_WORKER_HTTP_PORT = 8080;

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final TracingProvider tracingProvider;
    private final String internalAuthSecret;

    WorkerTaskControlClient(
            HttpClient httpClient,
            ObjectMapper mapper,
            TracingProvider tracingProvider,
            String internalAuthSecret) {
        this.httpClient = httpClient;
        this.mapper = mapper;
        this.tracingProvider = tracingProvider;
        this.internalAuthSecret = internalAuthSecret;
    }

    void injectDynamicFilter(
            String taskId, String planNodeId, byte[] filterProto, String workerId) {
        String[] parts = workerId.split(":");
        String host = parts[0];
        int port = parts.length > 1
                ? Integer.parseInt(parts[1]) : DEFAULT_WORKER_HTTP_PORT;
        String url = "http://" + host + ":" + port
                + "/v1/task/" + taskId + "/dynamic-filter";
        LOG.debug("event=dynamic_filter_inject url={} taskId={} planNodeId={}",
                url, taskId, planNodeId);
        try {
            HttpResponse<String> response = httpClient.send(
                    dynamicFilterRequest(url, planNodeId, filterProto),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HTTP_OK) {
                LOG.warn("event=dynamic_filter_inject_failed taskId={}"
                                + " workerId={} statusCode={}",
                        taskId, workerId, response.statusCode());
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            LOG.warn("event=dynamic_filter_inject_interrupted taskId={} workerId={}",
                    taskId, workerId);
        } catch (IOException failure) {
            LOG.warn("event=dynamic_filter_inject_io_error taskId={} workerId={} error={}",
                    taskId, workerId, failure.getMessage());
        }
    }

    SchedulerCancelResult cancelTask(org.pangrova.protocol.rest.task.SubmitTaskRequest issued,
            String workerHost, int httpPort) {
        var seal = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(issued.taskRequest());
        String taskId = seal.getAttempt().getTaskId();
        String url = "http://" + workerHost + ":" + httpPort + "/v1/task/" + taskId;
        LOG.debug("event=task_cancel url={}", url);
        HttpRequest request = decorate(HttpRequest.newBuilder())
                .uri(URI.create(url)).header("Content-Type", "application/x-protobuf")
                .header("Accept", "application/x-protobuf")
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(seal.toByteArray()))
                .timeout(java.time.Duration.ofSeconds(HTTP_TIMEOUT_SECONDS)).build();
        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            return mapCancelResponse(response, issued);
        } catch (IOException failure) {
            LOG.warn("event=task_cancel_io_error taskId={} error={}",
                    taskId, failure.getMessage());
            return SchedulerCancelResult.failed(
                    "IO error: " + failure.getMessage(), true);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            LOG.warn("event=task_cancel_interrupted taskId={}", taskId);
            return SchedulerCancelResult.failed("Interrupted", false);
        }
    }

    void notifyUpstreamCommit(
            String workerHost,
            int httpPort,
            String queryId,
            int upstreamStageId,
            Runnable failureCallback) {
        String url = "http://" + workerHost + ":" + httpPort
                + "/v1/task/upstream-commit";
        Exception failure = retryCommitNotify(
                url, queryId, upstreamStageId, workerHost, failureCallback);
        if (failure != null) {
            LOG.error("event=upstream_commit_notify_exhausted queryId={}"
                            + " upstreamStageId={} workerHost={} attempts={} lastError={}",
                    queryId, upstreamStageId, workerHost,
                    RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS,
                    failure.getMessage());
            invokeFailureCallback(
                    failureCallback, queryId, upstreamStageId, workerHost);
        }
    }

    private Exception retryCommitNotify(
            String url,
            String queryId,
            int upstreamStageId,
            String workerHost,
            Runnable failureCallback) {
        BackoffStrategy backoff = BackoffStrategy.retry(
                RemoteTaskDispatcher.COMMIT_NOTIFY_INITIAL_BACKOFF_MS,
                RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_BACKOFF_MS);
        Exception failure = null;
        for (int attempt = 1;
                attempt <= RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS;
                attempt++) {
            failure = attemptCommitNotify(
                    url, queryId, upstreamStageId, workerHost, attempt);
            if (failure == null || Thread.currentThread().isInterrupted()) {
                break;
            }
            if (attempt < RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS) {
                awaitRetry(backoff.next(), queryId, upstreamStageId);
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            invokeFailureCallback(
                    failureCallback, queryId, upstreamStageId, workerHost);
            return null;
        }
        return failure;
    }

    private Exception attemptCommitNotify(
            String url,
            String queryId,
            int upstreamStageId,
            String workerHost,
            int attempt) {
        try {
            HttpResponse<String> response = httpClient.send(
                    commitRequest(url, queryId, upstreamStageId),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == HTTP_OK) {
                return null;
            }
            LOG.warn("event=upstream_commit_notify_failed queryId={}"
                            + " upstreamStageId={} workerHost={} statusCode={}"
                            + " attempt={} maxAttempts={}",
                    queryId, upstreamStageId, workerHost, response.statusCode(),
                    attempt, RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS);
            return new IOException("Worker returned HTTP " + response.statusCode());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            LOG.warn("event=upstream_commit_notify_interrupted queryId={}"
                            + " upstreamStageId={}", queryId, upstreamStageId);
            return failure;
        } catch (IOException failure) {
            LOG.warn("event=upstream_commit_notify_io_error queryId={}"
                            + " upstreamStageId={} workerHost={} error={}"
                            + " attempt={} maxAttempts={}",
                    queryId, upstreamStageId, workerHost, failure.getMessage(),
                    attempt, RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS);
            return failure;
        }
    }

    private HttpRequest dynamicFilterRequest(
            String url, String planNodeId, byte[] filterProto) throws IOException {
        byte[] body = mapper.writeValueAsBytes(
                new InjectDynamicFilterRequest(planNodeId, filterProto));
        return postRequest(url, body);
    }

    private HttpRequest commitRequest(
            String url, String queryId, int upstreamStageId) throws IOException {
        byte[] body = mapper.writeValueAsBytes(
                new UpstreamCommitNotification(queryId, upstreamStageId));
        return postRequest(url, body);
    }

    private HttpRequest postRequest(String url, byte[] body) {
        return decorate(HttpRequest.newBuilder())
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private HttpRequest.Builder decorate(HttpRequest.Builder builder) {
        tracingProvider.inject(null, builder, HttpRequest.Builder::header);
        if (internalAuthSecret != null && !internalAuthSecret.isEmpty()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
        return builder;
    }

    private static SchedulerCancelResult mapCancelResponse(
            HttpResponse<byte[]> response, org.pangrova.protocol.rest.task.SubmitTaskRequest issued) {
        String taskId = issued.taskRequest().getAttempt().getTaskId();
        if (response.statusCode() == HTTP_OK) {
            String contentType = response.headers().firstValue("Content-Type").orElse("");
            if (!contentType.split(";", 2)[0].equalsIgnoreCase("application/x-protobuf")) {
                return SchedulerCancelResult.failed(
                    "Worker did not return a canonical drain receipt media type", false);
            }
            try {
                var actual = new org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator()
                        .validateExecutionDrainReceipt(response.body(), issued);
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

    private static void awaitRetry(
            long delayMs, String queryId, int upstreamStageId) {
        try {
            new CountDownLatch(1).await(delayMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            LOG.debug("event=commit_notify_retry_wait_interrupted queryId={}"
                            + " upstreamStageId={}", queryId, upstreamStageId);
        }
    }

    private static void invokeFailureCallback(
            Runnable callback,
            String queryId,
            int upstreamStageId,
            String workerHost) {
        if (callback == null) {
            return;
        }
        try {
            callback.run();
        } catch (RuntimeException failure) {
            LOG.warn("event=commit_notify_failure_callback_error queryId={}"
                            + " upstreamStageId={} workerHost={} error={}",
                    queryId, upstreamStageId, workerHost, failure.getMessage());
        }
    }
}
