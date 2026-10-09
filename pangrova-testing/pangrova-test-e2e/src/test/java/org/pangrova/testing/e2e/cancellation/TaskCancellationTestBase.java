package org.pangrova.testing.e2e.cancellation;

import org.junit.jupiter.api.BeforeEach;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Owns actual SQL, authenticated registration and transparent single-Worker transport. */
abstract class TaskCancellationTestBase extends SqlE2ETestBase {
    private static final String CATALOG = "task_admission_cancellation";
    protected static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = JsonMapperFactory.create();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30)).build();
    private String table;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default",
                icebergWarehouse().resolve("task-admission-cancellation").toString()));
    }

    @BeforeEach
    void createActualSource() throws Exception {
        registerTestConnectors(connectorConfigs());
        table = CATALOG + ".default.admission_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        var inserted = submitQueryViaHttp("INSERT INTO " + table + " VALUES (7), (9)");
        assertThat(pollUntilTerminal(inserted.queryId())).contains("\"FINISHED\"");
    }

    protected Scenario scenario() throws Exception { return new Scenario("SELECT id FROM " + table); }

    protected static HttpResponse<byte[]> cancelQuery(String queryId) throws Exception {
        return CLIENT.send(authenticatedHttpRequest(URI.create("http://localhost:"
                        + coordinatorHttpServer.getPort() + "/v1/query/" + queryId + "/cancel"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    protected static HttpResponse<byte[]> replay(byte[] body, java.util.Map<String, List<String>> headers)
            throws Exception {
        var request = HttpRequest.newBuilder(workerUri().resolve("/v1/task")).timeout(Duration.ofSeconds(30));
        headers.forEach((name, values) -> {
            if (name.equalsIgnoreCase("X-Internal-Auth") || name.equalsIgnoreCase("Content-Type")) {
                values.forEach(value -> request.header(name, value));
            }
        });
        return CLIENT.send(request.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    protected static HttpResponse<byte[]> seal(SubmitTaskRequest request) throws Exception {
        return sealAt(request, workerUri());
    }

    protected static HttpResponse<byte[]> sealAt(SubmitTaskRequest request, URI endpoint) throws Exception {
        byte[] body = CanonicalTaskAdmissionSeals.create(request.taskRequest()).toByteArray();
        return CLIENT.send(HttpRequest.newBuilder(endpoint.resolve(
                        "/v1/task/" + request.taskRequest().getAttempt().getTaskId()))
                .timeout(Duration.ofSeconds(30)).header("X-Internal-Auth", E2E_INTERNAL_AUTH_SECRET)
                .header("Authorization", "Bearer " + E2E_INTERNAL_AUTH_SECRET)
                .header("Content-Type", "application/x-protobuf").header("Accept", "application/x-protobuf")
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    protected static TaskReport receipt(HttpResponse<byte[]> response, SubmitTaskRequest expected) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/x-protobuf");
        return new CanonicalTaskReportValidator().validateExecutionDrainReceipt(response.body(), expected);
    }

    protected static URI workerUri() {
        return URI.create("http://" + workerAdvertisedHost() + ":" + workerHttpServer.getPort());
    }

    private static void registerActualWorker(WorkerInfo actual, int httpPort) throws Exception {
        var request = new WorkerRegistrationRequest(actual.workerId(), actual.host(), httpPort,
                actual.transportPort(), actual.resultServerPort(), actual.protocolVersion(), actual.rackId(),
                actual.dataLocality(), null, CapabilityInventoryCodec.encode(actual.capabilityInventory()));
        postInternal("/v1/workers/register", MAPPER.writeValueAsBytes(request));
        postInternal("/v1/worker/heartbeat", MAPPER.writeValueAsBytes(new HeartbeatRequest(actual.workerId(),
                actual.memoryPressure().name(), actual.cpuUtilization(), actual.runningTaskCount(),
                actual.availableMemoryBytes())));
    }

    private static void postInternal(String path, byte[] body) throws Exception {
        var response = CLIENT.send(HttpRequest.newBuilder(URI.create("http://localhost:"
                        + coordinatorHttpServer.getPort() + path)).timeout(Duration.ofSeconds(30))
                .header("X-Internal-Auth", E2E_INTERNAL_AUTH_SECRET).header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + E2E_INTERNAL_AUTH_SECRET)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).as("real authenticated endpoint %s", path).isEqualTo(200);
    }

    protected final class Scenario implements AutoCloseable {
        private final WorkerInfo original = java.util.Objects.requireNonNull(workerManager.getWorker(WORKER_ID));
        final TaskHttpTransport transport = new TaskHttpTransport(workerUri());
        private CompletableFuture<SubmitQueryResponse> querySubmission;
        String queryId;
        TaskHttpTransport.Submission submission;

        private Scenario(String sql) throws Exception {
            try {
                registerActualWorker(original, transport.port());
                // The relay intentionally holds the actual Worker POST. Coordinator query
                // submission waits for that response, so start submission asynchronously before
                // exercising cancellation/admission behavior against the captured request.
                querySubmission = CompletableFuture.supplyAsync(() -> {
                    try {
                        return submitQueryViaHttp(sql);
                    } catch (Exception failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                });
                submission = transport.awaitSubmission();
                queryId = submission.request().taskRequest().getAttempt().getQueryOrJobId();
            } catch (Exception | Error failure) {
                try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        SubmitQueryResponse awaitQuery() throws Exception {
            try {
                return querySubmission.get(30, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause() instanceof java.util.concurrent.CompletionException
                        && failure.getCause().getCause() != null
                        ? failure.getCause().getCause() : failure.getCause();
                if (cause instanceof Exception exception) throw exception;
                throw new IllegalStateException("SQL query submission failed", cause);
            }
        }

        @Override
        public void close() throws Exception {
            transport.reconnectAllDeletes();
            Throwable failure = null;
            if (queryId != null) {
                try {
                    assertThat(cancelQuery(queryId).statusCode()).isIn(200, 202);
                } catch (Throwable cleanup) { failure = cleanup; }
            }
            transport.releasePost();
            if (queryId != null) {
                try {
                    if (querySubmission != null) {
                        try {
                            querySubmission.get(30, TimeUnit.SECONDS);
                        } catch (ExecutionException expectedWhenCancellationWinsDispatch) {
                            // A real 409 from the held Worker POST can make the SQL submit itself
                            // fail; tests assert the cancellation and admission receipt directly.
                        }
                    }
                    pollUntilTerminal(queryId);
                }
                catch (Throwable cleanup) {
                    if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
                }
            }
            try { registerActualWorker(original, original.httpPort()); }
            catch (Throwable cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            try { transport.close(); }
            catch (Throwable cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (failure instanceof Error error) throw error;
            if (failure instanceof Exception exception) throw exception;
        }
    }
}
