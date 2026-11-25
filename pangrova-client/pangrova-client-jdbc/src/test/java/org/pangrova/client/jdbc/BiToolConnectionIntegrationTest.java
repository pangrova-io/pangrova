package org.pangrova.client.jdbc;

import org.pangrova.testing.testkit.enums.ResultScalarType;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.testing.testkit.result.TestResultEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test simulating DBeaver and IntelliJ JDBC connection patterns.
 *
 * <p>Validates the full BI tool workflow:
 * <ol>
 *   <li>Connection establishment via DriverManager.getConnection</li>
 *   <li>DatabaseMetaData discovery (getCatalogs, getSchemas, getTables, getColumns)</li>
 *   <li>PreparedStatement execution with parameterized queries</li>
 *   <li>ResultSet iteration</li>
 *   <li>Connection close</li>
 * </ol>
 *
 * <p>Uses real HTTP protocol inputs and SDK result transport. This does not prove
 * a deployed Coordinator, authenticated metadata authorization, or Native SQL execution.</p>
 */
@DisplayName("BI Tool Connection Integration (DBeaver/IntelliJ patterns)")
class BiToolConnectionIntegrationTest {

    private HttpServer httpServer;
    private TestResultEndpoint resultServer;
    private MetadataProtocolPeer metadataPeer;
    private int httpPort;

    @BeforeEach
    void setUp() throws Exception {
        metadataPeer = new MetadataProtocolPeer();
        httpServer = metadataPeer.server();
        httpPort = metadataPeer.port();
    }

    @AfterEach
    void tearDown() throws Exception {
        Exception failure = null;
        try { if (metadataPeer != null) metadataPeer.close(); } catch (Exception cleanup) { failure = cleanup; }
        try { if (resultServer != null) resultServer.close(); } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        try { PangrovaDriver.shutdownSharedClient(); } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        if (failure != null) throw failure;
    }

    @Test
    @DisplayName("DBeaver pattern: connect → getMetaData → getCatalogs → getSchemas → getTables → getColumns → close")
    void dbeaverMetadataDiscoveryPattern() throws Exception {
        startCatalogResultServer();
        registerSubmitEndpoint(new AtomicReference<>());
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort)) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();

            DatabaseMetaData metaData = conn.getMetaData();
            assertThat(metaData).isNotNull();
            assertThat(metaData.getDatabaseProductName()).isEqualTo("Pangrova");
            assertThat(metaData.getDriverName()).isEqualTo("Pangrova JDBC Driver");
            assertThat(metaData.getURL()).contains("127.0.0.1:" + httpPort);

            // getCatalogs — DBeaver calls this first to populate the catalog tree
            try (ResultSet catalogs = metaData.getCatalogs()) {
                assertThat(catalogs).isNotNull();
                assertThat(catalogs.next()).isTrue();
                assertThat(catalogs.getString("TABLE_CAT")).isEqualTo("lakehouse");
            }

            // getSchemas — DBeaver expands a catalog node
            try (ResultSet schemas = metaData.getSchemas("lakehouse", null)) {
                assertThat(schemas).isNotNull();
                assertThat(schemas.next()).isTrue();
                assertThat(schemas.getString("TABLE_SCHEM")).isEqualTo("empty");
                assertThat(schemas.next()).isTrue();
                assertThat(schemas.getString("TABLE_SCHEM")).isEqualTo("public");
                assertThat(schemas.next()).isFalse();
            }

            // getTables — DBeaver expands a schema node
            try (ResultSet tables = metaData.getTables("lakehouse", "public", "%", null)) {
                assertThat(tables).isNotNull();
                assertThat(tables.next()).isTrue();
                assertThat(tables.getString("TABLE_SCHEM")).isEqualTo("public");
                assertThat(tables.getString("TABLE_NAME")).isEqualTo("orders");
            }

            // getColumns — DBeaver inspects a table
            try (ResultSet columns = metaData.getColumns("lakehouse", "public", "orders", null)) {
                assertThat(columns).isNotNull();
                assertThat(columns.next()).isTrue();
                assertThat(columns.getString("COLUMN_NAME")).isEqualTo("order_id");
                assertThat(columns.getInt("DATA_TYPE")).isEqualTo(java.sql.Types.BIGINT);
                assertThat(columns.getInt("ORDINAL_POSITION")).isEqualTo(1);
            }
        }
    }

    @Test
    @DisplayName("IntelliJ pattern: connect → prepareStatement → setParameters → executeQuery → iterate ResultSet → close")
    void intellijPreparedStatementPattern() throws Exception {
        AtomicReference<String> capturedSql = new AtomicReference<>();

        startOrdersResultServer();
        registerSubmitEndpoint(capturedSql);
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT order_id, customer_name, amount FROM orders WHERE amount > ? AND customer_name = ?")) {
                assertThat(ps).isNotNull();

                ps.setDouble(1, 100.0);
                ps.setString(2, "Alice");

                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs).isNotNull();
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("order_id")).isEqualTo(1);
                    assertThat(rs.getString("customer_name")).isEqualTo("Alice");
                    assertThat(rs.getInt("amount")).isEqualTo(250);

                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("order_id")).isEqualTo(2);
                    assertThat(rs.getString("customer_name")).isEqualTo("Bob");
                    assertThat(rs.getInt("amount")).isEqualTo(175);
                    assertThat(rs.next()).isFalse();
                }
            }

            // Verify the SQL was sent with inlined parameters
            assertThat(capturedSql.get())
                    .isEqualTo("SELECT order_id, customer_name, amount FROM orders WHERE amount > 100.0 AND customer_name = 'Alice'");
        }
    }

    @Test
    @DisplayName("Connection lifecycle: isValid → getAutoCommit → setReadOnly → close → isClosed")
    void connectionLifecyclePattern() throws Exception {
        startCatalogResultServer();
        registerSubmitEndpoint(new AtomicReference<>());
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort)) {
            assertThat(conn.isValid(5)).isTrue();
            assertThat(conn.getAutoCommit()).isTrue();
            conn.setReadOnly(true);
            assertThat(conn.isReadOnly()).isTrue();
            conn.close();
            assertThat(conn.isClosed()).isTrue();
        }
    }

    @Test
    @DisplayName("PreparedStatement parameter clearing and reuse")
    void preparedStatementReusePattern() throws Exception {
        AtomicReference<String> capturedSql = new AtomicReference<>();

        startOrdersResultServer();
        registerSubmitEndpoint(capturedSql);
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM orders WHERE id = ?")) {

            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
            }
            assertThat(capturedSql.get()).isEqualTo("SELECT * FROM orders WHERE id = 42");

            ps.clearParameters();
            ps.setInt(1, 99);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
            }
            assertThat(capturedSql.get()).isEqualTo("SELECT * FROM orders WHERE id = 99");
        }
    }

    @Test
    void emptySelectReturnsAnEmptyResultSetRatherThanNoResult() throws Exception {
        resultServer = new TestResultEndpoint(
                List.of(new TestResultEndpoint.Column("value", ResultScalarType.INTEGER)), List.of());
        registerSubmitEndpoint(new AtomicReference<>());
        httpServer.start();

        try (var conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
             var statement = conn.createStatement();
             var result = statement.executeQuery("SELECT 1 WHERE false")) {
            assertThat(result.getMetaData().getColumnCount()).isEqualTo(1);
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    void flightEofChecksTerminalFailureAfterRowsHaveBeenDelivered() throws Exception {
        startOrdersResultServer();
        registerSubmitEndpoint(new AtomicReference<>(), "RUNNING");
        var terminalReads = new AtomicInteger();
        var cancels = new AtomicInteger();
        httpServer.createContext("/v1/query/", exchange -> {
            String[] path = exchange.getRequestURI().getPath().split("/");
            if (path[4].equals("state")) {
                terminalReads.incrementAndGet();
                sendJsonResponse(exchange, 200, """
                        {"query_id":"%s","state":"FAILED","version":1,"drained":false,
                         "result_disposition":"STREAM",
                         "result_endpoint":{"host":"127.0.0.1","port":%d,
                                            "ticket":"%s","protocol":"grpc"},
                         "error":{"error_code":"OUTPUT_FAILED","message":"Late output failure"}}
                        """.formatted(path[3], resultServer.endpoint().port(), resultServer.endpoint().ticket()));
            } else {
                cancels.incrementAndGet();
                sendJsonResponse(exchange, 200, """
                        {"query_id":"%s","outcome":"ALREADY_TERMINAL","cancelled":false}
                        """.formatted(path[3]));
            }
        });
        httpServer.start();

        try (var conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
             var statement = conn.createStatement();
             var result = statement.executeQuery("SELECT * FROM orders")) {
            assertThat(result.getMetaData().getColumnCount()).isEqualTo(3);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(terminalReads).hasValue(0);
            assertThatThrownBy(result::next).isInstanceOf(SQLException.class).hasMessageContaining("OUTPUT_FAILED");
            assertThat(result.isClosed()).isTrue();
            assertThat(terminalReads).hasValue(1);
            assertThat(cancels).hasValue(1);
        }
    }

    @Test
    void closingPartialResultCancelsOnlyItsQueryAndStatementCanExecuteAgain() throws Exception {
        startOrdersResultServer();
        registerSubmitEndpoint(new AtomicReference<>(), "RUNNING");
        var cancels = new java.util.concurrent.CopyOnWriteArrayList<String>();
        httpServer.createContext("/v1/query/", exchange -> {
            String[] path = exchange.getRequestURI().getPath().split("/");
            assertThat(path[4]).isEqualTo("cancel");
            cancels.add(path[3]);
            sendJsonResponse(exchange, 202, """
                    {"query_id":"%s","outcome":"CANCEL_ACCEPTED","cancelled":true}
                    """.formatted(path[3]));
        });
        httpServer.start();

        try (var conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
             var statement = conn.createStatement()) {
            for (int execution = 0; execution < 2; execution++) {
                try (var result = statement.executeQuery("SELECT * FROM orders")) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
                    assertThat(cancels).hasSize(execution);
                }
                assertThat(cancels).hasSize(execution + 1);
            }
        }
        assertThat(cancels).hasSize(2).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("DatabaseMetaData capability reporting for BI tool compatibility")
    void metadataCapabilityReporting() throws Exception {
        startCatalogResultServer();
        registerSubmitEndpoint(new AtomicReference<>());
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertThat(meta.supportsGroupBy()).isTrue();
            assertThat(meta.supportsOuterJoins()).isTrue();
            assertThat(meta.supportsSubqueriesInExists()).isTrue();
            assertThat(meta.supportsCorrelatedSubqueries()).isTrue();
            assertThat(meta.supportsUnionAll()).isTrue();
            assertThat(meta.getIdentifierQuoteString()).isEqualTo("\"");
            assertThat(meta.getCatalogSeparator()).isEqualTo(".");
            assertThat(meta.supportsTransactions()).isFalse();
            assertThat(meta.isReadOnly()).isTrue();
        }
    }

    // --- Test infrastructure ---

    private void startCatalogResultServer() throws Exception {
        resultServer = new TestResultEndpoint(List.of(new TestResultEndpoint.Column("TABLE_CAT", ResultScalarType.TEXT)),
                List.of(List.of(new TestResultEndpoint.TextCell("lakehouse"))));
    }

    private void startOrdersResultServer() throws Exception {
        resultServer = new TestResultEndpoint(List.of(
                new TestResultEndpoint.Column("order_id", ResultScalarType.INTEGER),
                new TestResultEndpoint.Column("customer_name", ResultScalarType.TEXT),
                new TestResultEndpoint.Column("amount", ResultScalarType.INTEGER)), List.of(
                List.of(new TestResultEndpoint.IntegerCell(1), new TestResultEndpoint.TextCell("Alice"), new TestResultEndpoint.IntegerCell(250)),
                List.of(new TestResultEndpoint.IntegerCell(2), new TestResultEndpoint.TextCell("Bob"), new TestResultEndpoint.IntegerCell(175))));
    }

    private void registerSubmitEndpoint(AtomicReference<String> capturedSql) {
        registerSubmitEndpoint(capturedSql, "FINISHED");
    }

    private void registerSubmitEndpoint(AtomicReference<String> capturedSql, String state) {
        httpServer.createContext("/v1/query/submit", exchange -> {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String sql = extractSqlFromRequest(requestBody);
            capturedSql.set(sql);

            String queryId = "q-bitest-" + System.nanoTime();
            String response = """
                    {
                      "query_id": "%s",
                      "state": "%s",
                      "result_disposition": "STREAM",
                      "result_endpoint": {
                        "host": "127.0.0.1",
                        "port": %d,
                        "ticket": "%s",
                        "protocol": "grpc"
                      },
                      "column_names": []
                    }
                    """.formatted(queryId, state, resultServer.endpoint().port(), resultServer.endpoint().ticket());
            sendJsonResponse(exchange, 200, response);
        });
    }

    private static String extractSqlFromRequest(String json) throws IOException {
        var request = JsonMapperFactory.create().readValue(json, SubmitQueryRequest.class);
        if (request.query() == null || request.query().isBlank()) {
            throw new IOException("Submission omitted the canonical query field");
        }
        return request.query();
    }

    private static void sendJsonResponse(HttpExchange exchange, int statusCode, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

}
