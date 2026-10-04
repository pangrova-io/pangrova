package org.pangrova.testing.e2e.speculation;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.task.TaskStatusReport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/** Network observation and delay only: all registration, reports and responses come from real processes. */
@ThreadSafe
final class ProcessStatusReportRelay implements AutoCloseable {
    private static final Set<String> HOP_HEADERS = Set.of("connection", "content-length", "expect", "host",
            "upgrade", "transfer-encoding", "http2-settings", "te", "trailer", "keep-alive");
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService threads = ManagedExecutorFactory.newFixedThreadPool("process-report-relay", 8);
    private final HttpServer server;
    private final CopyOnWriteArrayList<Delivery> deliveries = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Throwable> forwardingFailures = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ReportHold> holds = new CopyOnWriteArrayList<>();
    private volatile URI coordinator;
    private volatile boolean closed;

    ProcessStatusReportRelay() throws IOException {
        server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 8);
        server.createContext("/", this::forward);
        server.setExecutor(threads);
        server.start();
    }

    int port() { return server.getAddress().getPort(); }

    void connect(URI actualCoordinator) {
        if (coordinator != null) throw new IllegalStateException("Coordinator endpoint is already bound");
        coordinator = Objects.requireNonNull(actualCoordinator);
    }

    ReportHold holdReports(Predicate<TaskStatusReport> select) {
        ReportHold retained = new ReportHold(Objects.requireNonNull(select), new CompletableFuture<>());
        holds.add(retained);
        return retained;
    }

    void releaseReports() {
        holds.forEach(ReportHold::close);
        holds.clear();
    }

    List<Delivery> deliveries() { return List.copyOf(deliveries); }
    List<Throwable> forwardingFailures() { return List.copyOf(forwardingFailures); }

    private void forward(HttpExchange exchange) throws IOException {
        try (exchange) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Delivery delivery = observe(exchange, body);
            delaySelected(delivery);
            HttpResponse<byte[]> response = send(exchange, body);
            if (delivery != null) delivery.response().set(response);
            response.headers().map().forEach((name, values) -> {
                if (!HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                    exchange.getResponseHeaders().put(name, values);
                }
            });
            exchange.sendResponseHeaders(response.statusCode(), response.body().length == 0 ? -1 : response.body().length);
            if (response.body().length != 0) exchange.getResponseBody().write(response.body());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            forwardingFailures.add(failure);
            throw new IOException("Actual process report forwarding interrupted", failure);
        } catch (IOException | RuntimeException failure) {
            forwardingFailures.add(failure);
            throw failure;
        }
    }

    private Delivery observe(HttpExchange exchange, byte[] body) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/v1/task/status-report")) return null;
        var report = JsonMapperFactory.shared().readValue(body, TaskStatusReport.class);
        var delivery = new Delivery(report, body.clone(), System.nanoTime(), new AtomicReference<>());
        deliveries.add(delivery);
        return delivery;
    }

    private void delaySelected(Delivery delivery) throws InterruptedException {
        if (delivery == null) return;
        for (ReportHold retained : holds) {
            if (!retained.select().test(delivery.report())) continue;
            try {
                retained.released().get();
            } catch (java.util.concurrent.ExecutionException failure) {
                throw new IllegalStateException("Report hold failed", failure.getCause());
            }
        }
    }

    private HttpResponse<byte[]> send(HttpExchange exchange, byte[] body) throws IOException, InterruptedException {
        URI target = Objects.requireNonNull(coordinator, "Actual Coordinator endpoint is not connected");
        var request = HttpRequest.newBuilder(target.resolve(exchange.getRequestURI()))
                .timeout(Duration.ofSeconds(30));
        exchange.getRequestHeaders().forEach((name, values) -> {
            if (!HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                values.forEach(value -> request.header(name, value));
            }
        });
        return client.send(request.method(exchange.getRequestMethod(),
                body.length == 0 ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body))
                .build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    @Override
    public void close() throws InterruptedException {
        if (closed) return;
        releaseReports();
        server.stop(0);
        threads.shutdown();
        try {
            if (!threads.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual process report forwarding has not exited");
            }
            closed = true;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    record Delivery(TaskStatusReport report, byte[] body, long receivedNanos,
            AtomicReference<HttpResponse<byte[]>> response) {
        @Override public byte[] body() { return body.clone(); }
    }

    record ReportHold(Predicate<TaskStatusReport> select, CompletableFuture<Void> released) implements AutoCloseable {
        @Override public void close() { released.complete(null); }
    }
}
