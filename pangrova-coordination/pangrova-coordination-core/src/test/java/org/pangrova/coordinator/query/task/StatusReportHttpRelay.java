package org.pangrova.coordinator.query.task;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Only forwards actual HTTP responses or closes the actual socket; never manufactures an ACK. */
@ThreadSafe
final class StatusReportHttpRelay implements AutoCloseable {
    private static final Set<String> HOP_HEADERS = Set.of("connection", "content-length", "expect", "host",
            "upgrade", "transfer-encoding", "http2-settings", "te", "trailer", "keep-alive");
    private final URI coordinator;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final java.util.concurrent.ExecutorService threads = Executors.newFixedThreadPool(4,
            action -> new Thread(action, "status-report-http-relay"));
    private final HttpServer server;
    private final CopyOnWriteArrayList<Attempt> attempts = new CopyOnWriteArrayList<>();
    private final AtomicInteger matched = new AtomicInteger();
    private volatile boolean stopped;
    private volatile String selectedState;
    private volatile boolean onlyDrained;
    private volatile int disconnectBefore;
    private volatile int loseAckAfter;
    private volatile Runnable beforeFirstSelected = () -> { };

    StatusReportHttpRelay(URI coordinator) throws IOException {
        this.coordinator = coordinator;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 4);
        server.createContext("/v1/task/status-report", this::forward);
        server.setExecutor(threads);
        server.start();
    }

    URI uri() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }

    void select(String state, boolean drained, int before, int after) {
        if (!attempts.isEmpty()) throw new IllegalStateException("Cannot change faults after delivery begins");
        selectedState = state;
        onlyDrained = drained;
        disconnectBefore = before;
        loseAckAfter = after;
    }

    List<Attempt> matchingAttempts() { return attempts.stream().filter(Attempt::selected).toList(); }
    void beforeFirstSelected(Runnable action) { beforeFirstSelected = java.util.Objects.requireNonNull(action); }

    private void forward(HttpExchange exchange) throws IOException {
        try (exchange) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            TaskStatusReport report = JsonMapperFactory.shared().readValue(body, TaskStatusReport.class);
            boolean selected = selectedState.equals(report.state()) && (!onlyDrained || drained(report));
            int count = selected ? matched.incrementAndGet() : 0;
            var attempt = new Attempt(body, exchange.getRequestHeaders().getFirst("Authorization"), selected,
                    new java.util.concurrent.atomic.AtomicReference<>());
            attempts.add(attempt);
            if (selected && count == 1) beforeFirstSelected.run();
            if (selected && count <= disconnectBefore) return;
            var request = HttpRequest.newBuilder(coordinator.resolve("/v1/task/status-report"))
                    .timeout(Duration.ofSeconds(10));
            exchange.getRequestHeaders().forEach((name, values) -> {
                if (!HOP_HEADERS.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                    values.forEach(value -> request.header(name, value));
                }
            });
            var response = client.send(request.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            attempt.response().set(response);
            if (selected && count <= loseAckAfter) return;
            response.headers().firstValue("Content-Type")
                    .ifPresent(value -> exchange.getResponseHeaders().set("Content-Type", value));
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Actual status-report forwarding interrupted", interrupted);
        }
    }

    private static boolean drained(TaskStatusReport report) throws IOException {
        if (report.canonicalTaskReport() == null || report.canonicalTaskReport().length == 0) return false;
        ExecutionDrainState state = TaskReport.parseFrom(report.canonicalTaskReport()).getExecutionDrainState();
        return state == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                || state == ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER;
    }

    @Override
    public void close() throws InterruptedException {
        if (stopped) return;
        server.stop(0);
        threads.shutdown();
        try {
            if (!threads.awaitTermination(15, TimeUnit.SECONDS)) {
                threads.shutdownNow();
                throw new IllegalStateException("Actual status-report forwarding did not stop");
            }
            stopped = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    record Attempt(byte[] body, String authorization, boolean selected,
                   java.util.concurrent.atomic.AtomicReference<HttpResponse<byte[]>> response) { }
}
