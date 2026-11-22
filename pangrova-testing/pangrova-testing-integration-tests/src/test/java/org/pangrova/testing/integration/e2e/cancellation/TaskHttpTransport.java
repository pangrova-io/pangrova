package org.pangrova.testing.integration.e2e.cancellation;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Holds one actual Coordinator POST; every forwarded response originates at the real Worker. */
@ThreadSafe
final class TaskHttpTransport implements AutoCloseable {
    private static final Set<String> HOP_HEADERS = Set.of(
            "connection", "content-length", "expect", "host", "upgrade", "transfer-encoding",
            "http2-settings", "te", "trailer", "keep-alive", "proxy-authenticate", "proxy-authorization");
    private final HttpServer server;
    private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(4,
            action -> new Thread(action, "task-admission-http-transport"));
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    private final URI worker;
    private final AtomicBoolean intercepted = new AtomicBoolean();
    private final CountDownLatch released = new CountDownLatch(1);
    private final CompletableFuture<Submission> submission = new CompletableFuture<>();
    private final CompletableFuture<HttpResponse<byte[]>> postResponse = new CompletableFuture<>();
    private final CompletableFuture<HttpResponse<byte[]>> drainResponse = new CompletableFuture<>();
    private final Set<String> disconnectedTasks = ConcurrentHashMap.newKeySet();
    private final AtomicInteger deleteAttempts = new AtomicInteger();

    TaskHttpTransport(URI worker) throws IOException {
        this.worker = worker;
        server = HttpServer.create(new InetSocketAddress(0), 4);
        server.createContext("/v1/task", this::forward);
        server.setExecutor(executor);
        server.start();
    }

    int port() { return server.getAddress().getPort(); }
    void releasePost() { released.countDown(); }
    void disconnectDeletes(String taskId) { disconnectedTasks.add("/v1/task/" + taskId); }
    void reconnectDeletes(String taskId) { disconnectedTasks.remove("/v1/task/" + taskId); }
    void reconnectAllDeletes() { disconnectedTasks.clear(); }
    int deleteAttempts() { return deleteAttempts.get(); }
    Submission awaitSubmission() throws Exception { return submission.get(30, TimeUnit.SECONDS); }
    HttpResponse<byte[]> awaitPostResponse() throws Exception { return postResponse.get(30, TimeUnit.SECONDS); }
    HttpResponse<byte[]> awaitDrainResponse() throws Exception { return drainResponse.get(30, TimeUnit.SECONDS); }

    private void forward(HttpExchange exchange) throws IOException {
        boolean held = false;
        try (exchange) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                deleteAttempts.incrementAndGet();
                if (disconnectedTasks.contains(exchange.getRequestURI().getPath())) {
                    return; // Closing the actual socket without a response is the transport fault.
                }
            }
            byte[] body = exchange.getRequestBody().readAllBytes();
            held = "POST".equals(exchange.getRequestMethod())
                    && "/v1/task".equals(exchange.getRequestURI().getPath())
                    && intercepted.compareAndSet(false, true);
            if (held) {
                submission.complete(new Submission(body, immutableHeaders(exchange)));
                if (!released.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("Actual Coordinator task POST was not released");
                }
            }
            var request = forwardedRequest(exchange, body);
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (held) postResponse.complete(response);
            if ("DELETE".equals(exchange.getRequestMethod())
                    && isCapturedTask(exchange.getRequestURI().getPath())) {
                drainResponse.complete(response);
            }
            response.headers().firstValue("Content-Type")
                    .ifPresent(value -> exchange.getResponseHeaders().set("Content-Type", value));
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            fail(failure, held);
            throw new IOException("Actual task transport interrupted", failure);
        } catch (IOException | RuntimeException failure) {
            fail(failure, held);
            throw failure;
        }
    }

    private boolean isCapturedTask(String path) {
        Submission captured = submission.getNow(null);
        return captured != null && path.equals("/v1/task/" + captured.request().taskRequest().getAttempt().getTaskId());
    }

    private HttpRequest forwardedRequest(HttpExchange exchange, byte[] body) {
        var request = HttpRequest.newBuilder(worker.resolve(exchange.getRequestURI().toString()))
                .timeout(Duration.ofSeconds(30));
        exchange.getRequestHeaders().forEach((name, values) -> {
            if (!HOP_HEADERS.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                values.forEach(value -> request.header(name, value));
            }
        });
        return request.method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private static Map<String, List<String>> immutableHeaders(HttpExchange exchange) {
        return exchange.getRequestHeaders().entrySet().stream().collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    private void fail(Throwable failure, boolean held) {
        submission.completeExceptionally(failure);
        if (held) postResponse.completeExceptionally(failure);
        drainResponse.completeExceptionally(failure);
    }

    @Override
    public void close() throws InterruptedException {
        releasePost();
        server.stop(0);
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Actual task HTTP transport has not drained");
        }
    }

    static final class Submission {
        private final byte[] body;
        private final Map<String, List<String>> headers;
        private final SubmitTaskRequest request;

        Submission(byte[] body, Map<String, List<String>> headers) throws IOException {
            this.body = body.clone();
            this.headers = headers;
            request = SubmitTaskRequest.decode(body);
        }

        byte[] body() { return body.clone(); }
        Map<String, List<String>> headers() { return headers; }
        SubmitTaskRequest request() { return request; }
    }
}
