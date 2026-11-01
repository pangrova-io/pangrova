package org.pangrova.client.transport.http;

import org.junit.jupiter.api.DisplayName;
import org.pangrova.client.error.PangrovaClientException;
import org.pangrova.client.error.PangrovaConnectionException;
import org.pangrova.client.error.PangrovaProtocolException;
import org.pangrova.client.error.PangrovaServerException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that different server/network error types are correctly classified
 * into the appropriate client-side exception hierarchy.
 *
 * <p>Tests exercise real HTTP calls against unreachable/invalid endpoints
 * to trigger genuine connection and protocol errors. No mocks or stubs.
 *
 * <p><b>Validates: Requirements 1.57, 2.57</b>
 */
@DisplayName("Client exception classification")
class ClientExceptionClassificationTest {

    // ---- Exception hierarchy tests ----

    @Nested
    @DisplayName("Exception hierarchy")
    class ExceptionHierarchy {

        @Test
        @DisplayName("PangrovaConnectionException is subclass of PangrovaClientException")
        void connectionExceptionIsClientException() {
            PangrovaConnectionException ex = new PangrovaConnectionException(
                    "CONNECTION_REFUSED", "refused", new ConnectException());
            assertThat(ex).isInstanceOf(PangrovaClientException.class);
            assertThat(ex.errorCode()).isEqualTo("CONNECTION_REFUSED");
            assertThat(ex.getCause()).isInstanceOf(ConnectException.class);
        }

        @Test
        @DisplayName("PangrovaProtocolException is subclass of PangrovaClientException")
        void protocolExceptionIsClientException() {
            PangrovaProtocolException ex = new PangrovaProtocolException(
                    "DESERIALIZATION_FAILED", "bad json", 200, new RuntimeException("parse error"));
            assertThat(ex).isInstanceOf(PangrovaClientException.class);
            assertThat(ex.errorCode()).isEqualTo("DESERIALIZATION_FAILED");
            assertThat(ex.httpStatusCode()).isEqualTo(200);
            assertThat(ex.getCause()).hasMessage("parse error");
        }

        @Test
        @DisplayName("PangrovaServerException is subclass of PangrovaClientException")
        void serverExceptionIsClientException() {
            PangrovaServerException ex = new PangrovaServerException(
                    "INTERNAL_ERROR", "server crashed", 500);
            assertThat(ex).isInstanceOf(PangrovaClientException.class);
            assertThat(ex.errorCode()).isEqualTo("INTERNAL_ERROR");
            assertThat(ex.httpStatusCode()).isEqualTo(500);
        }

        @Test
        @DisplayName("All exception types preserve cause chain")
        void causeChainPreserved() {
            IOException root = new IOException("network down");
            RuntimeException mid = new RuntimeException("wrapper", root);
            PangrovaConnectionException ex = new PangrovaConnectionException(
                    "CONNECTION_FAILED", "failed", mid);

            assertThat(ex.getCause()).isSameAs(mid);
            assertThat(ex.getCause().getCause()).isSameAs(root);
        }
    }

    // ---- Connection error classification (real HTTP calls) ----

    @Nested
    @DisplayName("Connection error classification")
    class ConnectionErrors {

        @Test
        @DisplayName("Connection refused throws PangrovaConnectionException")
        void connectionRefusedClassified() {
            // Port 1 is almost certainly not listening
            PangrovaRestClient client = PangrovaRestClient.builder()
                    .coordinatorHost("127.0.0.1")
                    .coordinatorPort(1)
                    .connectTimeout(Duration.ofSeconds(2))
                    .requestTimeout(Duration.ofSeconds(2))
                    .build();

            assertThatThrownBy(() -> client.submitQuery("SELECT 1", java.util.Map.of()))
                    .isInstanceOf(PangrovaConnectionException.class)
                    .satisfies(ex -> {
                        PangrovaConnectionException connEx = (PangrovaConnectionException) ex;
                        assertThat(connEx.errorCode()).isIn(
                                "CONNECTION_REFUSED", "CONNECTION_FAILED",
                                "CONNECTION_TIMEOUT", "REQUEST_TIMEOUT");
                        assertThat(connEx.getCause()).isNotNull();
                    });

            client.close();
        }

        @Test
        @DisplayName("DNS resolution failure throws PangrovaConnectionException")
        void dnsResolutionFailureClassified() {
            // Bypass process-wide HTTP proxies: otherwise a proxy can return a
            // 502 for .invalid before the client's DNS classifier is exercised.
            HttpClient directClient = HttpClient.newBuilder()
                    .proxy(new ProxySelector() {
                        @Override
                        public List<Proxy> select(URI uri) {
                            return List.of(Proxy.NO_PROXY);
                        }

                        @Override
                        public void connectFailed(URI uri, SocketAddress address,
                                                  IOException failure) {
                            throw new AssertionError("Unexpected proxy connection", failure);
                        }
                    })
                    .connectTimeout(Duration.ofSeconds(3))
                    .build();
            PangrovaRestClient client = PangrovaRestClient.builder()
                    .coordinatorHost("this-host-does-not-exist-pangrova-test.invalid")
                    .coordinatorPort(8080)
                    .connectTimeout(Duration.ofSeconds(3))
                    .requestTimeout(Duration.ofSeconds(3))
                    .httpClient(directClient)
                    .build();

            assertThatThrownBy(() -> client.getQueryStatus("q-123"))
                    .isInstanceOf(PangrovaConnectionException.class)
                    .satisfies(ex -> {
                        PangrovaConnectionException connEx = (PangrovaConnectionException) ex;
                        // Exact error code depends on OS/network; all are connection-level
                        assertThat(connEx.errorCode()).isIn(
                                "DNS_RESOLUTION_FAILED", "CONNECTION_FAILED",
                                "CONNECTION_REFUSED", "CONNECTION_TIMEOUT");
                        assertThat(connEx.getCause()).isNotNull();
                    });

            client.close();
        }
    }

    // ---- Server error classification (using a real local HTTP server) ----

    @Nested
    @DisplayName("Server error classification via local HTTP server")
    class ServerErrorClassification {

        @Test
        @DisplayName("HTTP 500 with error body throws PangrovaServerException")
        void http500WithErrorBodyClassified() throws Exception {
            com.sun.net.httpserver.HttpServer server =
                    com.sun.net.httpserver.HttpServer.create(
                            new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/query/submit", exchange -> {
                String body = "{\"error_code\":\"INTERNAL_ERROR\",\"message\":\"out of memory\"}";
                byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            int port = server.getAddress().getPort();

            try {
                PangrovaRestClient client = PangrovaRestClient.builder()
                        .coordinatorHost("127.0.0.1")
                        .coordinatorPort(port)
                        .build();

                assertThatThrownBy(() -> client.submitQuery("SELECT 1", java.util.Map.of()))
                        .isInstanceOf(PangrovaServerException.class)
                        .satisfies(ex -> {
                            PangrovaServerException serverEx = (PangrovaServerException) ex;
                            assertThat(serverEx.errorCode()).isEqualTo("INTERNAL_ERROR");
                            assertThat(serverEx.getMessage()).isEqualTo("out of memory");
                            assertThat(serverEx.httpStatusCode()).isEqualTo(500);
                        });

                client.close();
            } finally {
                server.stop(0);
            }
        }

        @Test
        @DisplayName("HTTP 500 with unparseable body throws PangrovaServerException with generic code")
        void http500WithBadBodyClassified() throws Exception {
            com.sun.net.httpserver.HttpServer server =
                    com.sun.net.httpserver.HttpServer.create(
                            new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/query/submit", exchange -> {
                byte[] bytes = "not json".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(503, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            int port = server.getAddress().getPort();

            try {
                PangrovaRestClient client = PangrovaRestClient.builder()
                        .coordinatorHost("127.0.0.1")
                        .coordinatorPort(port)
                        .build();

                assertThatThrownBy(() -> client.submitQuery("SELECT 1", java.util.Map.of()))
                        .isInstanceOf(PangrovaServerException.class)
                        .satisfies(ex -> {
                            PangrovaServerException serverEx = (PangrovaServerException) ex;
                            assertThat(serverEx.errorCode()).isEqualTo("SERVER_ERROR");
                            assertThat(serverEx.httpStatusCode()).isEqualTo(503);
                        });

                client.close();
            } finally {
                server.stop(0);
            }
        }

        @Test
        @DisplayName("HTTP 400 with error body throws PangrovaClientException (not server/connection)")
        void http400WithErrorBodyClassified() throws Exception {
            com.sun.net.httpserver.HttpServer server =
                    com.sun.net.httpserver.HttpServer.create(
                            new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/query/submit", exchange -> {
                String body = "{\"error_code\":\"INVALID_SQL\",\"message\":\"syntax error\"}";
                byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(400, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            int port = server.getAddress().getPort();

            try {
                PangrovaRestClient client = PangrovaRestClient.builder()
                        .coordinatorHost("127.0.0.1")
                        .coordinatorPort(port)
                        .build();

                assertThatThrownBy(() -> client.submitQuery("SELECT 1", java.util.Map.of()))
                        .isInstanceOf(PangrovaClientException.class)
                        .isNotInstanceOf(PangrovaServerException.class)
                        .isNotInstanceOf(PangrovaConnectionException.class)
                        .isNotInstanceOf(PangrovaProtocolException.class)
                        .satisfies(ex -> {
                            PangrovaClientException clientEx = (PangrovaClientException) ex;
                            assertThat(clientEx.errorCode()).isEqualTo("INVALID_SQL");
                            assertThat(clientEx.getMessage()).isEqualTo("syntax error");
                        });

                client.close();
            } finally {
                server.stop(0);
            }
        }

        @Test
        @DisplayName("HTTP 200 with malformed JSON throws PangrovaProtocolException")
        void malformedResponseClassified() throws Exception {
            com.sun.net.httpserver.HttpServer server =
                    com.sun.net.httpserver.HttpServer.create(
                            new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/query/submit", exchange -> {
                byte[] bytes = "{{invalid json}}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            int port = server.getAddress().getPort();

            try {
                PangrovaRestClient client = PangrovaRestClient.builder()
                        .coordinatorHost("127.0.0.1")
                        .coordinatorPort(port)
                        .build();

                assertThatThrownBy(() -> client.submitQuery("SELECT 1", java.util.Map.of()))
                        .isInstanceOf(PangrovaProtocolException.class)
                        .satisfies(ex -> {
                            PangrovaProtocolException protoEx = (PangrovaProtocolException) ex;
                            assertThat(protoEx.errorCode()).isEqualTo("DESERIALIZATION_FAILED");
                            assertThat(protoEx.getCause()).isNotNull();
                        });

                client.close();
            } finally {
                server.stop(0);
            }
        }
    }

    // JDBC-layer classification tests are in pangrova-client-jdbc module:
    // see org.pangrova.client.jdbc.JdbcExceptionClassificationTest
}
