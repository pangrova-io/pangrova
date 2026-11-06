package org.pangrova.client.jdbc;

import org.pangrova.client.error.PangrovaConnectionException;
import org.pangrova.client.error.PangrovaProtocolException;
import org.pangrova.transport.client.http.PangrovaRestClient;
import org.pangrova.client.error.PangrovaServerException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that JDBC Statement maps classified client exceptions
 * to appropriate SQL states in SQLException.
 *
 * <p><b>Validates: Requirements 1.57, 2.57</b>
 */
@DisplayName("JDBC exception classification")
class JdbcExceptionClassificationTest {

    @Test
    @DisplayName("Connection refused maps to SQLException with SQL state 08001")
    void connectionRefusedMapsTo08001() {
        PangrovaStatement stmt = createStatement("127.0.0.1", 1);

        assertThatThrownBy(() -> stmt.executeQuery("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .satisfies(ex -> {
                    SQLException sqlEx = (SQLException) ex;
                    assertThat(sqlEx.getSQLState()).isEqualTo("08001");
                    assertThat(sqlEx.getCause()).isInstanceOf(PangrovaConnectionException.class);
                    PangrovaConnectionException connEx = (PangrovaConnectionException) sqlEx.getCause();
                    assertThat(connEx.errorCode()).isNotNull();
                    assertThat(connEx.getCause()).isNotNull();
                });
    }

    @Test
    @DisplayName("Server error (HTTP 500) maps to SQLException with SQL state HY000")
    void serverErrorMapsToHY000() throws Exception {
        com.sun.net.httpserver.HttpServer server =
                com.sun.net.httpserver.HttpServer.create(
                        new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> {
            String body = "{\"error_code\":\"INTERNAL_ERROR\",\"message\":\"oom\"}";
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(500, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            PangrovaStatement stmt = createStatement("127.0.0.1", port);

            assertThatThrownBy(() -> stmt.executeQuery("SELECT 1"))
                    .isInstanceOf(SQLException.class)
                    .satisfies(ex -> {
                        SQLException sqlEx = (SQLException) ex;
                        assertThat(sqlEx.getSQLState()).isEqualTo("HY000");
                        assertThat(sqlEx.getCause()).isInstanceOf(PangrovaServerException.class);
                    });
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("Protocol error (malformed JSON) maps to SQLException with SQL state 08P01")
    void protocolErrorMapsTo08P01() throws Exception {
        com.sun.net.httpserver.HttpServer server =
                com.sun.net.httpserver.HttpServer.create(
                        new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> {
            byte[] bytes = "not-json".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            PangrovaStatement stmt = createStatement("127.0.0.1", port);

            assertThatThrownBy(() -> stmt.executeQuery("SELECT 1"))
                    .isInstanceOf(SQLException.class)
                    .satisfies(ex -> {
                        SQLException sqlEx = (SQLException) ex;
                        assertThat(sqlEx.getSQLState()).isEqualTo("08P01");
                        assertThat(sqlEx.getCause()).isInstanceOf(PangrovaProtocolException.class);
                    });
        } finally {
            server.stop(0);
        }
    }

    private PangrovaStatement createStatement(String host, int port) {
        PangrovaRestClient restClient = PangrovaRestClient.builder()
                .coordinatorHost(host)
                .coordinatorPort(port)
                .connectTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
        PangrovaConnection conn = new PangrovaConnection(restClient, null);
        return new PangrovaStatement(conn);
    }
}
