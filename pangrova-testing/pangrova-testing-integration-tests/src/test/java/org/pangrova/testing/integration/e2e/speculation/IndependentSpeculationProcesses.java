package org.pangrova.testing.integration.e2e.speculation;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.client.protocol.ResultProtocol;
import org.pangrova.client.protocol.QueryStatusResponse;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Same production JVM launcher with transparent HTTP fault boundaries and real Native Flight. */
final class IndependentSpeculationProcesses implements AutoCloseable {
    private static final long MIN_DISPATCH_MEMORY_BYTES = 64L * 1024 * 1024;
    private final IndependentLocalProcesses local;
    private final ExecutorService clientCommands = ManagedExecutorFactory.newSingleThreadExecutor("process-sql-client");
    private final List<WorkerNetwork> networks = new ArrayList<>();
    private ProcessInputSnapshot input;

    IndependentSpeculationProcesses() throws Exception {
        local = new IndependentLocalProcesses(true);
        try {
            for (int number = 1; number <= 2; number++) {
                var target = new AtomicReference<URI>();
                var http = new ProcessWorkerHttpRelay(() -> Objects.requireNonNull(target.get(), "Worker registration"));
                networks.add(new WorkerNetwork("local-worker-" + number, http, target));
            }
            local.reports().mapWorkerHttpPorts(actual -> {
                var network = networks.stream().filter(worker -> worker.workerId().equals(actual.workerId()))
                        .findFirst().orElseThrow();
                network.target().set(URI.create("http://" + actual.httpHost() + ":" + actual.httpPort()));
                return network.http().port();
            });
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    void start() throws Exception {
        try {
            local.startCoordinatorAndFirstWorker();
            input = new ProcessInputSnapshot(local.warehouse(), local.hostJdbcUrl());
            local.createSourceCatalog();
            local.startSecondWorker();
            awaitSchedulableWorkers();
        } catch (Exception | Error failure) {
            failure.addSuppressed(new IllegalStateException("Independent speculation diagnostics retained at "
                    + local.retainDiagnostics()));
            throw failure;
        }
    }

    private void awaitSchedulableWorkers() {
        var client = HttpClient.newHttpClient();
        String credentials = ProcessSecurityAuthority.USER + ":" + ProcessSecurityAuthority.PASSWORD;
        var request = HttpRequest.newBuilder(coordinator().resolve("/api/workers"))
                .header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(5)).GET().build();
        await("both independent Workers have dispatch capacity").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                    assertThat(twoWorkersSchedulable(JsonMapperFactory.shared().readTree(response.body())))
                            .as(response.body()).isTrue();
                });
    }

    static boolean twoWorkersSchedulable(JsonNode workers) {
        if (workers == null || !workers.isArray()) return false;
        boolean first = false;
        boolean second = false;
        for (JsonNode worker : workers) {
            boolean schedulable = "ACTIVE".equals(worker.path("healthState").asText())
                    && !"CRITICAL".equals(worker.path("memoryPressure").asText())
                    && worker.path("availableMemoryBytes").asLong() >= MIN_DISPATCH_MEMORY_BYTES;
            if (schedulable && "local-worker-1".equals(worker.path("workerId").asText())) first = true;
            if (schedulable && "local-worker-2".equals(worker.path("workerId").asText())) second = true;
        }
        return first && second;
    }

    ProcessStatusReportRelay reports() { return local.reports(); }
    ProcessInputSnapshot input() { return Objects.requireNonNull(input, "Processes have not started"); }
    List<WorkerNetwork> workers() { return List.copyOf(networks); }
    URI coordinator() { return URI.create("http://127.0.0.1:" + local.coordinatorPort()); }
    long pid(String workerId) { return local.pid(workerId); }
    java.nio.file.Path retainDiagnostics() { return local.retainDiagnostics(); }
    int activeTenantReservations() { return local.activeTenantReservations(); }
    QueryStatusResponse queryStatus(String queryId) { return local.status(queryId); }
    HttpResponse<byte[]> replayReport(ProcessStatusReportRelay.Delivery actual) throws Exception {
        return local.replayReport(actual);
    }

    CompletableFuture<SqlOutput> query(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var result = local.executeSql(sql, ResultProtocol.GRPC_TLS);
                if (result.exitCode() == 0) local.requireSuccessfulQuery(result, ResultProtocol.GRPC_TLS);
                return new SqlOutput(result.exitCode(), Objects.requireNonNullElse(result.output(), ""),
                        result.exitCode() == 0 ? "" : result.errors());
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CompletionException(failure);
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        }, clientCommands);
    }

    List<IssuedTask> issuedTasks() throws IOException {
        var tasks = new ArrayList<IssuedTask>();
        for (WorkerNetwork worker : networks) {
            for (var transfer : worker.http().transfers()) {
                if (transfer.isTaskSubmission()) tasks.add(new IssuedTask(worker.workerId(), transfer.submittedTask(), transfer));
            }
        }
        return List.copyOf(tasks);
    }

    @Override public void close() throws Exception {
        local.reports().releaseReports();
        clientCommands.shutdown();
        if (!clientCommands.awaitTermination(40, TimeUnit.SECONDS)) {
            clientCommands.shutdownNow();
            if (!clientCommands.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Independent CLI owner is still live: " + local.retainDiagnostics());
            }
        }
        Exception failure = null;
        try { local.close(); } catch (Exception cleanup) { failure = cleanup; }
        for (var network : networks) {
            try { network.http().close(); }
            catch (Exception cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        if (input != null) {
            try { input.close(); }
            catch (Exception cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) throw failure;
    }

    record WorkerNetwork(String workerId, ProcessWorkerHttpRelay http, AtomicReference<URI> target) { }
    record IssuedTask(String workerId, SubmitTaskRequest request, ProcessWorkerHttpRelay.Transfer transfer) { }
    record SqlOutput(int exitCode, String output, String errors) {
        boolean succeeded() { return exitCode == 0 && !output.contains("Error") && !output.contains("Query failed"); }
    }
}
