package org.pangrova.worker.copy;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopySourceReadClientTest {
    @Test
    void productionBearerClientRejectsPlaintextAndTestTransportRejectsRemoteHost() {
        assertThatThrownBy(() -> new CopySourceReadClient(
                "http://127.0.0.1:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
        assertThatThrownBy(() -> new CopySourceReadClient(
                "http://worker.example:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
        assertThatThrownBy(() -> CopySourceReadClient.forLoopbackTest(
                "http://worker.example:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
    }

    @Test
    void coordinatorResponseBodyStallIsBoundedByWholeRequestTimeout() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        var executor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, 100);
                exchange.getResponseBody().write('{');
                exchange.getResponseBody().flush();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        server.start();
        try {
            var client = CopySourceReadClient.forLoopbackTest(
                    "http://127.0.0.1:" + server.getAddress().getPort(),
                    "private-secret", Duration.ofMillis(300));
            long started = System.nanoTime();
            assertThatThrownBy(() -> client.redeem("nonce", scope()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("timed out");
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofSeconds(2));
        } finally {
            release.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    void coordinatorChunkedResponseIsRejectedAtByteLimit() throws Exception {
        var executor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write(new byte[64 * 1024 + 1]);
            }
        });
        server.start();
        try {
            var client = CopySourceReadClient.forLoopbackTest(
                    "http://127.0.0.1:" + server.getAddress().getPort(),
                    "private-secret", Duration.ofSeconds(2));
            assertThatThrownBy(() -> client.redeem("nonce", scope()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("size limit");
        } finally {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    void timeoutCancelsTheStalledHttpExchange() throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1,
                InetAddress.getByName("127.0.0.1"))) {
            var executor = Executors.newSingleThreadExecutor();
            try {
                var observedClose = executor.submit(() -> {
                    try (var socket = listener.accept()) {
                        socket.setSoTimeout(2_000);
                        var input = socket.getInputStream();
                        ByteArrayOutputStream headers = new ByteArrayOutputStream();
                        while (!headers.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) {
                            int next = input.read();
                            if (next < 0) return false;
                            headers.write(next);
                        }
                        String requestHeaders = headers.toString(StandardCharsets.ISO_8859_1);
                        int bodyLength = java.util.regex.Pattern.compile(
                                "(?im)^content-length: ([0-9]+)\\r?$")
                                .matcher(requestHeaders).results()
                                .mapToInt(match -> Integer.parseInt(match.group(1)))
                                .findFirst().orElseThrow();
                        if (input.readNBytes(bodyLength).length != bodyLength) return false;
                        socket.getOutputStream().write(("HTTP/1.1 200 OK\r\n"
                                + "Content-Length: 100\r\n\r\n{")
                                .getBytes(StandardCharsets.ISO_8859_1));
                        socket.getOutputStream().flush();
                        return input.read() == -1;
                    }
                });
                var client = CopySourceReadClient.forLoopbackTest(
                        "http://127.0.0.1:" + listener.getLocalPort(),
                        "private-secret", Duration.ofMillis(300));
                assertThatThrownBy(() -> client.redeem("nonce", scope()))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("timed out");
                assertThat(observedClose.get(2, TimeUnit.SECONDS)).isTrue();
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static CopySourceReadHttp.Scope scope() {
        return new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", "task-42", "coordinator", "test", 1, 1,
                4_102_444_800_000L);
    }
}
