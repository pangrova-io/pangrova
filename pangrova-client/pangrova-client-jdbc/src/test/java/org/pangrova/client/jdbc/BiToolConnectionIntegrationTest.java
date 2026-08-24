package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
 * <p>Uses an embedded HTTP server as a coordinator REST stub and an embedded
 * gRPC result server for result delivery. No mock frameworks.</p>
 */
@DisplayName("BI Tool Connection Integration (DBeaver/IntelliJ patterns)")
class BiToolConnectionIntegrationTest {

    private HttpServer httpServer;
    private FlightServer resultServer;
    private int httpPort;
    private int resultPort;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator();
        httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpPort = httpServer.getAddress().getPort();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (httpServer != null) {
            httpServer.stop(0);
        }
        if (resultServer != null) {
            resultServer.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    @DisplayName("DBeaver pattern: connect → getMetaData → getCatalogs → getSchemas → getTables → getColumns → close")
    void dbeaverMetadataDiscoveryPattern() throws Exception {
        startResultServer(buildCatalogSchema(), this::populateCatalogData);
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
            }

            // getTables — DBeaver expands a schema node
            try (ResultSet tables = metaData.getTables("lakehouse", "public", "%", null)) {
                assertThat(tables).isNotNull();
                assertThat(tables.next()).isTrue();
            }

            // getColumns — DBeaver inspects a table
            try (ResultSet columns = metaData.getColumns("lakehouse", "public", "orders", null)) {
                assertThat(columns).isNotNull();
                assertThat(columns.next()).isTrue();
            }
        }
    }

    @Test
    @DisplayName("IntelliJ pattern: connect → prepareStatement → setParameters → executeQuery → iterate ResultSet → close")
    void intellijPreparedStatementPattern() throws Exception {
        AtomicReference<String> capturedSql = new AtomicReference<>();

        startResultServer(buildOrdersSchema(), this::populateOrdersData);
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
                }
            }

            // Verify the SQL was sent with inlined parameters
            assertThat(capturedSql.get())
                    .contains("100.0")
                    .contains("'Alice'");
        }
    }

    @Test
    @DisplayName("Connection lifecycle: isValid → getAutoCommit → setReadOnly → close → isClosed")
    void connectionLifecyclePattern() throws Exception {
        startResultServer(buildCatalogSchema(), this::populateCatalogData);
        registerSubmitEndpoint(new AtomicReference<>());
        httpServer.start();

        Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
        assertThat(conn.isValid(5)).isTrue();
        assertThat(conn.getAutoCommit()).isTrue();
        conn.setReadOnly(true);
        assertThat(conn.isReadOnly()).isTrue();

        conn.close();
        assertThat(conn.isClosed()).isTrue();
    }

    @Test
    @DisplayName("PreparedStatement parameter clearing and reuse")
    void preparedStatementReusePattern() throws Exception {
        AtomicReference<String> capturedSql = new AtomicReference<>();

        startResultServer(buildOrdersSchema(), this::populateOrdersData);
        registerSubmitEndpoint(capturedSql);
        httpServer.start();

        try (Connection conn = DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + httpPort);
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM orders WHERE id = ?")) {

            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
            }
            assertThat(capturedSql.get()).contains("42");

            ps.clearParameters();
            ps.setInt(1, 99);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
            }
            assertThat(capturedSql.get()).contains("99");
        }
    }

    @Test
    @DisplayName("DatabaseMetaData capability reporting for BI tool compatibility")
    void metadataCapabilityReporting() throws Exception {
        startResultServer(buildCatalogSchema(), this::populateCatalogData);
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

    private void startResultServer(Schema schema, DataPopulator populator) throws IOException {
        Location location = Location.forGrpcInsecure("127.0.0.1", 0);
        resultServer = FlightServer.builder(allocator, location,
                        new TestResultProducer(schema, populator))
                .build()
                .start();
        resultPort = resultServer.getPort();
    }

    private void registerSubmitEndpoint(AtomicReference<String> capturedSql) {
        httpServer.createContext("/v1/query/submit", exchange -> {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String sql = extractSqlFromRequest(requestBody);
            capturedSql.set(sql);

            String queryId = "q-bitest-" + System.nanoTime();
            String response = """
                    {
                      "query_id": "%s",
                      "state": "FINISHED",
                      "result_endpoint": {
                        "host": "127.0.0.1",
                        "port": %d,
                        "ticket": "%s",
                        "protocol": "grpc"
                      },
                      "column_names": []
                    }
                    """.formatted(queryId, resultPort, queryId);
            sendJsonResponse(exchange, 200, response);
        });
    }

    private Schema buildCatalogSchema() {
        return new Schema(List.of(
                new Field("TABLE_CAT", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    private void populateCatalogData(VectorSchemaRoot root) {
        VarCharVector catVector = (VarCharVector) root.getVector("TABLE_CAT");
        root.allocateNew();
        catVector.setSafe(0, "lakehouse".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
    }

    private Schema buildOrdersSchema() {
        return new Schema(List.of(
                new Field("order_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("customer_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    private void populateOrdersData(VectorSchemaRoot root) {
        IntVector idVector = (IntVector) root.getVector("order_id");
        VarCharVector nameVector = (VarCharVector) root.getVector("customer_name");
        IntVector amountVector = (IntVector) root.getVector("amount");

        root.allocateNew();
        idVector.setSafe(0, 1);
        nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        amountVector.setSafe(0, 250);

        idVector.setSafe(1, 2);
        nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        amountVector.setSafe(1, 175);

        root.setRowCount(2);
    }

    private static String extractSqlFromRequest(String json) {
        int sqlIdx = json.indexOf("\"sql\"");
        if (sqlIdx < 0) {
            return json;
        }
        int colonIdx = json.indexOf(':', sqlIdx);
        int startQuote = json.indexOf('"', colonIdx + 1);
        int endQuote = json.indexOf('"', startQuote + 1);
        if (startQuote >= 0 && endQuote > startQuote) {
            return json.substring(startQuote + 1, endQuote);
        }
        return json;
    }

    private static void sendJsonResponse(HttpExchange exchange, int statusCode, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface DataPopulator {
        void populate(VectorSchemaRoot root);
    }

    /**
     * Minimal gRPC result producer that serves test data for any ticket request.
     */
    private final class TestResultProducer extends NoOpFlightProducer {

        private final Schema schema;
        private final DataPopulator populator;

        TestResultProducer(Schema schema, DataPopulator populator) {
            this.schema = schema;
            this.populator = populator;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                populator.populate(root);
                listener.putNext();
                listener.completed();
            }
        }
    }
}
