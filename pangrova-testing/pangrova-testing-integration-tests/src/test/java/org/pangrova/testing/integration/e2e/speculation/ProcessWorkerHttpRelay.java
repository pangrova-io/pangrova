package org.pangrova.testing.integration.e2e.speculation;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Observes real control traffic and can disconnect a selected request before delivery. */
@ThreadSafe
final class ProcessWorkerHttpRelay implements AutoCloseable {
    private static final Set<String> HOP_HEADERS = Set.of("connection", "content-length", "expect", "host",
            "upgrade", "transfer-encoding", "http2-settings", "te", "trailer", "keep-alive");
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService threads = ManagedExecutorFactory.newFixedThreadPool("process-worker-control", 8);
    private final CopyOnWriteArrayList<Transfer> transfers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
    private final AtomicReference<Predicate<Transfer>> disconnected = new AtomicReference<>(ignored -> false);
    private final AtomicReference<Predicate<Transfer>> disconnectedAfter = new AtomicReference<>(ignored -> false);
    private final AtomicReference<Consumer<Transfer>> beforeDelivery = new AtomicReference<>(ignored -> { });
    private final HttpServer server;
    private final Supplier<URI> target;

    ProcessWorkerHttpRelay(Supplier<URI> target) throws IOException {
        this.target = Objects.requireNonNull(target);
        server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 8);
        server.createContext("/", this::forward);
        server.setExecutor(threads);
        server.start();
    }

    int port() { return server.getAddress().getPort(); }
    List<Transfer> transfers() { return List.copyOf(transfers); }
    List<Throwable> forwardingFailures() { return List.copyOf(failures); }
    void disconnectBeforeDelivery(Predicate<Transfer> select) { disconnected.set(Objects.requireNonNull(select)); }
    void disconnectAfterDelivery(Predicate<Transfer> select) { disconnectedAfter.set(Objects.requireNonNull(select)); }
    void beforeDelivery(Consumer<Transfer> action) { beforeDelivery.set(Objects.requireNonNull(action)); }

    private void forward(HttpExchange exchange) throws IOException {
        try (exchange) {
            var transfer = new Transfer(exchange.getRequestMethod(), exchange.getRequestURI(),
                    exchange.getRequestBody().readAllBytes(), System.nanoTime(), new AtomicReference<>());
            transfers.add(transfer);
            if (disconnected.get().test(transfer)) return;
            beforeDelivery.get().accept(transfer);
            HttpResponse<byte[]> response = send(exchange, transfer);
            transfer.response().set(response);
            if (disconnectedAfter.get().test(transfer)) return;
            response.headers().map().forEach((name, values) -> {
                if (!HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                    exchange.getResponseHeaders().put(name, values);
                }
            });
            exchange.sendResponseHeaders(response.statusCode(), response.body().length == 0 ? -1 : response.body().length);
            if (response.body().length != 0) exchange.getResponseBody().write(response.body());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            failures.add(failure);
            throw new IOException("Actual Worker control forwarding interrupted", failure);
        } catch (IOException | RuntimeException failure) {
            failures.add(failure);
            throw failure;
        }
    }

    private HttpResponse<byte[]> send(HttpExchange exchange, Transfer transfer)
            throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(Objects.requireNonNull(target.get()).resolve(transfer.path()))
                .timeout(Duration.ofSeconds(30));
        exchange.getRequestHeaders().forEach((name, values) -> {
            if (!HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                values.forEach(value -> request.header(name, value));
            }
        });
        return client.send(request.method(transfer.method(), HttpRequest.BodyPublishers.ofByteArray(transfer.body()))
                .build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    @Override public void close() throws InterruptedException {
        server.stop(0);
        threads.shutdown();
        try {
            if (!threads.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual Worker control forwarding is still live");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    record Transfer(String method, URI path, byte[] body, long receivedNanos,
            AtomicReference<HttpResponse<byte[]>> response) {
        Transfer { body = body.clone(); }
        @Override public byte[] body() { return body.clone(); }
        boolean isTaskSubmission() { return method.equals("POST") && path.getPath().equals("/v1/task"); }
        SubmitTaskRequest submittedTask() throws IOException {
            if (!isTaskSubmission()) throw new IllegalStateException("Not a task submission");
            return JsonMapperFactory.shared().readValue(body, SubmitTaskRequest.class);
        }
    }
}
