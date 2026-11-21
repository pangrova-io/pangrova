package org.pangrova.testing.integration.e2e.write;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A bounded real HTTP forwarding barrier; every status and response byte comes from Coordinator. */
final class CatalogDeploymentTransport implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final HttpClient client = HttpClient.newHttpClient();
    private final URI coordinator;
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private final AtomicInteger requests = new AtomicInteger();

    CatalogDeploymentTransport(String coordinatorUrl) throws IOException {
        coordinator = URI.create(coordinatorUrl);
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 2);
        server.createContext("/v1/internal/catalog-deployments/", this::forward);
        server.setExecutor(executor);
        server.start();
    }

    String url() { return "http://localhost:" + server.getAddress().getPort(); }
    int requestCount() { return requests.get(); }
    void release() { released.countDown(); }
    boolean awaitRequest() throws InterruptedException { return entered.await(10, TimeUnit.SECONDS); }

    private void forward(HttpExchange exchange) throws IOException {
        try (exchange) {
            requests.incrementAndGet();
            entered.countDown();
            if (!released.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Actual deployment transport was not released");
            }
            URI target = coordinator.resolve(exchange.getRequestURI().toString());
            HttpRequest request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(10))
                    .header("Authorization", exchange.getRequestHeaders().getFirst("Authorization"))
                    .GET().build();
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            response.headers().firstValue("Content-Type")
                    .ifPresent(value -> exchange.getResponseHeaders().set("Content-Type", value));
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Actual deployment forwarding interrupted", interrupted);
        }
    }

    @Override
    public void close() throws InterruptedException {
        release();
        server.stop(0);
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Actual deployment forwarding has not drained");
        }
    }
}
