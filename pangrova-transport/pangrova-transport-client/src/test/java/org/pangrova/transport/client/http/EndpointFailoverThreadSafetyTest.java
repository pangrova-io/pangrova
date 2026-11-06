package org.pangrova.transport.client.http;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.session.ConnectionPoolConfig;
import org.pangrova.client.session.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that endpoint failover rotation is thread-safe when multiple threads
 * share the same {@link PangrovaRestClient} instance.
 *
 * <p>F-COMMON-07: concurrent threads rotating endpoints must not interfere with
 * each other (no thundering-herd endpoint skipping, no stale endpoint usage
 * across retry sequences).
 */
@DisplayName("Endpoint failover thread safety (F-COMMON-07)")
class EndpointFailoverThreadSafetyTest {

    private static final RetryPolicy FAST_RETRY =
            new RetryPolicy(2, Duration.ofMillis(5), Duration.ofMillis(20), 1.5);

    private HttpServer serverA;
    private HttpServer serverB;
    private HttpServer serverC;
    private int portA;
    private int portB;
    private int portC;

    @BeforeEach
    void startServers() throws IOException {
        serverA = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverB = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverC = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverA.setExecutor(Executors.newFixedThreadPool(4));
        serverB.setExecutor(Executors.newFixedThreadPool(4));
        serverC.setExecutor(Executors.newFixedThreadPool(4));
        serverA.start();
        serverB.start();
        serverC.start();
        portA = serverA.getAddress().getPort();
        portB = serverB.getAddress().getPort();
        portC = serverC.getAddress().getPort();
    }

    @AfterEach
    void stopServers() {
        if (serverA != null) serverA.stop(0);
        if (serverB != null) serverB.stop(0);
        if (serverC != null) serverC.stop(0);
    }

    /**
     * Verifies that concurrent threads failing simultaneously on the same endpoint
     * only rotate the shared index once (CAS prevents double-skip).
     */
    @Test
    @DisplayName("concurrent failures on same endpoint do not double-skip (CAS rotation)")
    void concurrentFailuresDoNotDoubleSkip() throws Exception {
        // Server A always returns 503 (trigger failover)
        AtomicInteger serverAHits = new AtomicInteger();
        serverA.createContext("/v1/query/q-1/state", exchange -> {
            serverAHits.incrementAndGet();
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        // Server B succeeds
        AtomicInteger serverBHits = new AtomicInteger();
        serverB.createContext("/v1/query/q-1/state", exchange -> {
            serverBHits.incrementAndGet();
            byte[] body = "{\"query_id\":\"q-1\",\"state\":\"RUNNING\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        // Server C succeeds
        AtomicInteger serverCHits = new AtomicInteger();
        serverC.createContext("/v1/query/q-1/state", exchange -> {
            serverCHits.incrementAndGet();
            byte[] body = "{\"query_id\":\"q-1\",\"state\":\"RUNNING\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        List<String> endpoints = List.of(
                "127.0.0.1:" + portA,
                "127.0.0.1:" + portB,
                "127.0.0.1:" + portC);
        ConnectionPoolConfig poolConfig = ConnectionPoolConfig.withFailover(endpoints);

        try (PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(portA)
                .retryPolicy(FAST_RETRY)
                .connectionPoolConfig(poolConfig)
                .requestTimeout(Duration.ofSeconds(5))
                .build()) {

            int threadCount = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger();

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        // All threads try to get query status simultaneously
                        client.getQueryStatus("q-1");
                        successCount.incrementAndGet();
                    } catch (Exception ignored) {
                        // Some may still fail if retries exhausted
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Release all threads at once
            startLatch.countDown();
            doneLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            // At least some threads should have succeeded by failing over to B or C
            assertThat(successCount.get())
                    .as("some threads should successfully fail over to endpoint B or C")
                    .isGreaterThan(0);

            // Server B or C should have received requests (failover happened)
            assertThat(serverBHits.get() + serverCHits.get())
                    .as("failover targets should have received requests")
                    .isGreaterThan(0);

            // Server C should NOT have received the bulk of requests if CAS is working correctly.
            // Without CAS, 10 threads each incrementing would skip past B to C and beyond.
            // With CAS, the shared index advances at most once per unique failure, so most
            // threads should land on B (the first failover target).
            assertThat(serverBHits.get())
                    .as("CAS rotation should direct most failover traffic to endpoint B (next after A)")
                    .isGreaterThanOrEqualTo(serverCHits.get());
        }
    }

    /**
     * Verifies that each thread's retry loop independently cycles through endpoints
     * even when the shared index is contested by other threads.
     */
    @Test
    @DisplayName("per-request local failover cycles independently of shared state")
    void perRequestLocalFailoverCyclesIndependently() throws Exception {
        // All three servers return 503 for path /v1/query/q-fail/state
        // to verify that each thread's retry loop tries multiple endpoints
        ConcurrentHashMap<Integer, AtomicInteger> hitsByPort = new ConcurrentHashMap<>();
        hitsByPort.put(portA, new AtomicInteger());
        hitsByPort.put(portB, new AtomicInteger());
        hitsByPort.put(portC, new AtomicInteger());

        for (var entry : Map.of(serverA, portA, serverB, portB, serverC, portC).entrySet()) {
            HttpServer server = entry.getKey();
            int port = entry.getValue();
            server.createContext("/v1/query/q-fail/state", exchange -> {
                hitsByPort.get(port).incrementAndGet();
                byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(503, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
        }

        List<String> endpoints = List.of(
                "127.0.0.1:" + portA,
                "127.0.0.1:" + portB,
                "127.0.0.1:" + portC);
        ConnectionPoolConfig poolConfig = ConnectionPoolConfig.withFailover(endpoints);

        try (PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(portA)
                .retryPolicy(FAST_RETRY)
                .connectionPoolConfig(poolConfig)
                .requestTimeout(Duration.ofSeconds(5))
                .build()) {

            int threadCount = 6;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        client.getQueryStatus("q-fail");
                    } catch (Exception ignored) {
                        // Expected: all endpoints return 503, retries exhaust
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            // Each thread should have tried multiple endpoints in its retry loop.
            // With 2 retries (3 attempts) and 3 endpoints, traffic should spread across all three.
            int totalHits = hitsByPort.values().stream().mapToInt(AtomicInteger::get).sum();
            assertThat(totalHits)
                    .as("total attempts should be threadCount * maxAttempts = %d * %d",
                            threadCount, FAST_RETRY.maxRetries() + 1)
                    .isEqualTo(threadCount * (FAST_RETRY.maxRetries() + 1));

            // All three endpoints should have received at least one request,
            // proving that per-request local failover cycles through all endpoints.
            assertThat(hitsByPort.get(portA).get())
                    .as("endpoint A should have received requests")
                    .isGreaterThan(0);
            assertThat(hitsByPort.get(portB).get())
                    .as("endpoint B should have received requests")
                    .isGreaterThan(0);
            assertThat(hitsByPort.get(portC).get())
                    .as("endpoint C should have received requests")
                    .isGreaterThan(0);
        }
    }

    /**
     * Verifies that a single-thread failover still works correctly after the refactor.
     */
    @Test
    @DisplayName("single-thread failover rotates to next endpoint on 503")
    void singleThreadFailoverRotates() {
        // Server A returns 503
        serverA.createContext("/v1/query/q-1/state", exchange -> {
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        // Server B succeeds
        serverB.createContext("/v1/query/q-1/state", exchange -> {
            byte[] body = "{\"query_id\":\"q-1\",\"state\":\"COMPLETED\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        List<String> endpoints = List.of(
                "127.0.0.1:" + portA,
                "127.0.0.1:" + portB,
                "127.0.0.1:" + portC);
        ConnectionPoolConfig poolConfig = ConnectionPoolConfig.withFailover(endpoints);

        try (PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(portA)
                .retryPolicy(FAST_RETRY)
                .connectionPoolConfig(poolConfig)
                .requestTimeout(Duration.ofSeconds(5))
                .build()) {

            var result = client.getQueryStatus("q-1");
            assertThat(result).isNotNull();
        }
    }
}
