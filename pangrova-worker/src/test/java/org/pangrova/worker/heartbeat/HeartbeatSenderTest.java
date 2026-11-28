package org.pangrova.worker.heartbeat;

import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatSenderTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    private static ScheduledExecutorService newExecutor() {
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.setRemoveOnCancelPolicy(true);
        return exec;
    }

    @Test
    void startAndStopWithoutError() {
        ScheduledExecutorService executor = newExecutor();
        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-1", 1000);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> new HeartbeatRequest("w-1", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY),
                executor);

        sender.start();
        assertTrue(sender.isRunning());
        sender.stop();
        assertFalse(sender.isRunning());
    }

    @Test
    void payloadSupplierCalledOnEachTick() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicInteger callCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(3);

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-2", 50);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                    return new HeartbeatRequest("w-2", "NONE", 0.5, 2, 2048, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.start();
        boolean reached = latch.await(5, TimeUnit.SECONDS);
        sender.stop();

        assertTrue(reached, "Payload supplier should be called at least 3 times");
        assertTrue(callCount.get() >= 3);
    }

    @Test
    void httpFailureDoesNotCrash() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicInteger callCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:1", "w-3", 50);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                    return new HeartbeatRequest("w-3", "LOW", 0.8, 5, 512, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.start();
        boolean reached = latch.await(5, TimeUnit.SECONDS);
        sender.stop();

        assertTrue(reached, "Heartbeat should keep ticking despite HTTP failures");
        assertTrue(callCount.get() >= 2);
    }

    @Test
    void supplierExceptionDoesNotCrash() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicInteger callCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-4", 50);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                    throw new RuntimeException("metrics unavailable");
                },
                executor);

        sender.start();
        boolean reached = latch.await(5, TimeUnit.SECONDS);
        sender.stop();

        assertTrue(reached, "Heartbeat should keep ticking despite supplier exceptions");
        assertTrue(callCount.get() >= 2);
    }

    @Test
    void heartbeatConfigValidation() {
        HeartbeatConfig valid = new HeartbeatConfig("http://localhost:8080", "w-1", 5000);
        assertEquals("w-1", valid.workerId());
        assertEquals(5000, valid.intervalMs());
        assertEquals(5000, HeartbeatConfig.DEFAULT_INTERVAL_MS);
    }

    @Test
    void stopIsIdempotent() {
        ScheduledExecutorService executor = newExecutor();
        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-5", 1000);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> new HeartbeatRequest("w-5", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY),
                executor);

        sender.start();
        sender.stop();
        assertFalse(sender.isRunning());

        // Second stop should be a no-op, no exception
        sender.stop();
        assertFalse(sender.isRunning());
    }

    @Test
    void noHeartbeatSentAfterStopCompletes() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicInteger heartbeatCount = new AtomicInteger();
        CountDownLatch firstBeat = new CountDownLatch(1);

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-6", 30);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    heartbeatCount.incrementAndGet();
                    firstBeat.countDown();
                    return new HeartbeatRequest("w-6", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.start();
        assertTrue(firstBeat.await(5, TimeUnit.SECONDS), "At least one heartbeat should fire");

        sender.stop();
        assertFalse(sender.isRunning());

        int countAtStop = heartbeatCount.get();

        // Wait well beyond several heartbeat intervals
        Thread.sleep(200);

        // No new heartbeats should have been sent after stop
        assertEquals(countAtStop, heartbeatCount.get(),
                "No heartbeat should be sent after stop() completes");
    }

    /**
     * Concurrent test: multiple threads call stop() while heartbeats are firing.
     * Verifies no heartbeat is sent after all stop() calls complete.
     *
     * <p><b>Validates: Requirements 1.29, 1.30, 2.29, 2.30</b></p>
     */
    @Test
    void concurrentStopPreventsGhostScheduling() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicInteger heartbeatCount = new AtomicInteger();
        CountDownLatch firstBeat = new CountDownLatch(1);

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-7", 20);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    heartbeatCount.incrementAndGet();
                    firstBeat.countDown();
                    return new HeartbeatRequest("w-7", "NONE", 0.2, 1, 4096, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.start();
        assertTrue(firstBeat.await(5, TimeUnit.SECONDS), "At least one heartbeat should fire");

        // Launch multiple threads calling stop() concurrently
        int stopThreads = 4;
        CountDownLatch allStopped = new CountDownLatch(stopThreads);
        for (int i = 0; i < stopThreads; i++) {
            new Thread(() -> {
                sender.stop();
                allStopped.countDown();
            }).start();
        }
        assertTrue(allStopped.await(5, TimeUnit.SECONDS), "All stop threads should complete");
        assertFalse(sender.isRunning());

        int countAfterStop = heartbeatCount.get();

        // Wait well beyond several heartbeat intervals
        Thread.sleep(200);

        assertEquals(countAfterStop, heartbeatCount.get(),
                "No ghost heartbeat should be scheduled after concurrent stop()");
    }

    @Test
    void doubleStartIsRejected() {
        ScheduledExecutorService executor = newExecutor();
        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-8", 1000);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> new HeartbeatRequest("w-8", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY),
                executor);

        sender.start();
        assertTrue(sender.isRunning());

        // Second start should be a no-op (logged as warning)
        sender.start();
        assertTrue(sender.isRunning());

        sender.stop();
        assertFalse(sender.isRunning());
    }

    @Test
    void successfulHeartbeatPublishesAuthenticatedControlEpoch() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/worker/heartbeat", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{\"status\":\"ok\",\"coordinator_epoch\":42}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        ScheduledExecutorService executor = newExecutor();
        try {
            HeartbeatConfig config = new HeartbeatConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(), "w-epoch", 1000);
            HeartbeatSender sender = new HeartbeatSender(
                    config,
                    () -> new HeartbeatRequest("w-epoch", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY),
                    executor, HttpClient.newHttpClient(), null);
            AtomicLong observed = new AtomicLong();
            sender.setCoordinatorEpochObserver(observed::set);

            sender.sendHeartbeat();

            assertEquals(42L, observed.get());
        } finally {
            executor.shutdownNow();
            server.stop(0);
        }
    }

    @Test
    void firstHeartbeatPublishesResourceSnapshotBeforePeriodicInterval() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch acknowledged = new CountDownLatch(1);
        AtomicLong availableMemory = new AtomicLong();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/worker/heartbeat", exchange -> {
            byte[] request = exchange.getRequestBody().readAllBytes();
            availableMemory.set(org.pangrova.common.json.JsonMapperFactory.shared()
                    .readTree(request).path("available_memory_bytes").asLong());
            received.countDown();
            byte[] response = "{\"status\":\"ok\",\"coordinator_epoch\":42}".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        server.start();
        ScheduledExecutorService executor = newExecutor();
        HeartbeatSender sender = new HeartbeatSender(
                new HeartbeatConfig("http://127.0.0.1:" + server.getAddress().getPort(),
                        "w-immediate", 10_000),
                () -> new HeartbeatRequest("w-immediate", "NONE", 0.1, 0, 512L * 1024 * 1024, HEARTBEAT_IDENTITY),
                executor);
        sender.setCoordinatorEpochObserver(epoch -> acknowledged.countDown());
        try {
            sender.start();
            assertTrue(received.await(3, TimeUnit.SECONDS),
                    "The initialized resource snapshot must precede the first 10-second interval");
            assertEquals(512L * 1024 * 1024, availableMemory.get());
            assertTrue(acknowledged.await(3, TimeUnit.SECONDS),
                    "The Coordinator must acknowledge the first resource heartbeat");
        } finally {
            sender.stop();
            executor.shutdownNow();
            server.stop(0);
        }
    }
    @Test
    void conflictActionIsParsedExactlyAndFencingNeverReRegistersOrResumes() throws Exception {
        var response = new java.util.concurrent.atomic.AtomicReference<>(
                "{\"status\":\"error\",\"message\":\"do not re-register\"}");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/worker/heartbeat", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            byte[] body = response.get().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(409, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        var executor = newExecutor();
        var sender = new HeartbeatSender(new HeartbeatConfig(
                "http://127.0.0.1:" + server.getAddress().getPort(), "fenced-worker", 1000),
                () -> new HeartbeatRequest("fenced-worker", "NONE", 0, 0, 1024, HEARTBEAT_IDENTITY), executor);
        var repaired = new java.util.concurrent.atomic.AtomicInteger();
        var fenced = new java.util.concurrent.atomic.AtomicInteger();
        sender.setReRegistrationCallback(repaired::incrementAndGet);
        sender.setFencedCallback(fenced::incrementAndGet);
        try {
            sender.sendHeartbeat();
            assertEquals(0, repaired.get());
            response.set("{\"status\":\"error\",\"action\":\"re-register\"}");
            sender.sendHeartbeat();
            assertEquals(1, repaired.get());
            response.set("{\"status\":\"error\",\"action\":\"fenced\"}");
            sender.sendHeartbeat();
            assertTrue(sender.isFenced());
            assertEquals(1, fenced.get());
            response.set("{\"status\":\"error\",\"action\":\"re-register\"}");
            sender.sendHeartbeat();
            assertEquals(3, calls.get());
            assertEquals(1, repaired.get());
        } finally {
            sender.stop();
            executor.shutdownNow();
            server.stop(0);
        }
    }

}
