package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.rest.PangrovaRestClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link PaginatedResultSet} fetches pages lazily via the paginated result API.
 * Uses an embedded HTTP server as a test double (same pattern as {@link PangrovaStatementCancelTest}).
 */
@DisplayName("PaginatedResultSet")
class PaginatedResultSetTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("paginated fetch returns multiple pages of results")
    void paginatedFetchReturnsMultiplePages() throws Exception {
        String queryId = "q-paginated-001";
        AtomicInteger pageRequestCount = new AtomicInteger(0);

        // Page 1: rows (1, "alpha"), (2, "beta") with nextToken
        byte[] page1Data = buildArrowPage(new int[]{1, 2}, new String[]{"alpha", "beta"});
        // Page 2: rows (3, "gamma"), (4, "delta") without nextToken
        byte[] page2Data = buildArrowPage(new int[]{3, 4}, new String[]{"gamma", "delta"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            int page = pageRequestCount.incrementAndGet();
            String json;
            if (page == 1) {
                json = buildPageJson(page1Data, "token-page-2", 2);
            } else {
                json = buildPageJson(page2Data, null, 2);
            }
            sendJson(exchange, json);
        });

        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 2, null)) {
            // Row 1
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).isEqualTo("alpha");

            // Row 2
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(2);
            assertThat(rs.getString(2)).isEqualTo("beta");

            // Row 3 (triggers page 2 fetch)
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(3);
            assertThat(rs.getString(2)).isEqualTo("gamma");

            // Row 4
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(4);
            assertThat(rs.getString(2)).isEqualTo("delta");

            // No more rows
            assertThat(rs.next()).isFalse();
        }

        assertThat(pageRequestCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("setFetchSize stores value and getFetchSize returns it")
    void setFetchSizeStoresValue() throws Exception {
        String queryId = "q-fetchsize-001";
        byte[] pageData = buildArrowPage(new int[]{1}, new String[]{"x"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            sendJson(exchange, buildPageJson(pageData, null, 1));
        });
        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 50, null)) {
            rs.setFetchSize(100);
            assertThat(rs.getFetchSize()).isEqualTo(100);
        }
    }

    @Test
    @DisplayName("negative fetchSize is treated as zero")
    void negativeFetchSizeTreatedAsZero() throws Exception {
        String queryId = "q-negfetch-001";
        byte[] pageData = buildArrowPage(new int[]{1}, new String[]{"x"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            sendJson(exchange, buildPageJson(pageData, null, 1));
        });
        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 50, null)) {
            rs.setFetchSize(-5);
            assertThat(rs.getFetchSize()).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("single page exhausts correctly")
    void singlePageExhaustsCorrectly() throws Exception {
        String queryId = "q-single-001";
        byte[] pageData = buildArrowPage(new int[]{42}, new String[]{"only-row"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            sendJson(exchange, buildPageJson(pageData, null, 1));
        });
        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 10, null)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(42);
            assertThat(rs.getString(2)).isEqualTo("only-row");

            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @DisplayName("getRow() returns absolute 1-based row number across multiple pages")
    void getRowReturnsAbsoluteRowNumberAcrossPages() throws Exception {
        String queryId = "q-rowoffset-001";
        int rowsPerPage = 500;
        AtomicInteger pageRequestCount = new AtomicInteger(0);

        // Page 1: 500 rows (id 1..500)
        byte[] page1Data = buildSequentialArrowPage(1, rowsPerPage);
        // Page 2: 500 rows (id 501..1000)
        byte[] page2Data = buildSequentialArrowPage(rowsPerPage + 1, rowsPerPage);

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            int page = pageRequestCount.incrementAndGet();
            String json;
            if (page == 1) {
                json = buildPageJson(page1Data, "token-page-2", rowsPerPage);
            } else {
                json = buildPageJson(page2Data, null, rowsPerPage);
            }
            sendJson(exchange, json);
        });

        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, rowsPerPage, null)) {
            // Before first next(), getRow() must return 0
            assertThat(rs.getRow()).isEqualTo(0);
            assertThat(rs.isBeforeFirst()).isTrue();

            // Verify all 1000 rows return correct absolute row numbers
            for (int expected = 1; expected <= rowsPerPage * 2; expected++) {
                assertThat(rs.next())
                        .as("next() should return true for row %d", expected)
                        .isTrue();
                assertThat(rs.getRow())
                        .as("getRow() at absolute position %d", expected)
                        .isEqualTo(expected);
                assertThat(rs.getInt(1))
                        .as("id column value at row %d", expected)
                        .isEqualTo(expected);
            }

            // Verify position flags at the last row
            assertThat(rs.isLast()).isTrue();
            assertThat(rs.getRow()).isEqualTo(1000);

            // After exhaustion
            assertThat(rs.next()).isFalse();
            assertThat(rs.isAfterLast()).isTrue();
        }

        // Confirm exactly 2 page requests were made
        assertThat(pageRequestCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("getRow() handles variable-size pages correctly")
    void getRowHandlesVariableSizePages() throws Exception {
        String queryId = "q-varpage-001";
        AtomicInteger pageRequestCount = new AtomicInteger(0);

        // Page 1: 3 rows; Page 2: 5 rows; Page 3: 2 rows = 10 total
        byte[] page1Data = buildSequentialArrowPage(1, 3);
        byte[] page2Data = buildSequentialArrowPage(4, 5);
        byte[] page3Data = buildSequentialArrowPage(9, 2);

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            int page = pageRequestCount.incrementAndGet();
            String json = switch (page) {
                case 1 -> buildPageJson(page1Data, "tok-2", 3);
                case 2 -> buildPageJson(page2Data, "tok-3", 5);
                default -> buildPageJson(page3Data, null, 2);
            };
            sendJson(exchange, json);
        });

        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 10, null)) {
            for (int expected = 1; expected <= 10; expected++) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getRow())
                        .as("getRow() should be %d (absolute 1-based)", expected)
                        .isEqualTo(expected);
            }
            assertThat(rs.next()).isFalse();
        }

        assertThat(pageRequestCount.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("getMetaData() before next() fetches first page and resets cursor per JDBC spec")
    void getMetaDataBeforeNextCompliesWithJdbcSpec() throws Exception {
        String queryId = "q-metadata-001";
        AtomicInteger pageRequestCount = new AtomicInteger(0);

        byte[] page1Data = buildArrowPage(new int[]{10, 20}, new String[]{"first", "second"});
        byte[] page2Data = buildArrowPage(new int[]{30}, new String[]{"third"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            int page = pageRequestCount.incrementAndGet();
            String json;
            if (page == 1) {
                json = buildPageJson(page1Data, "tok-next", 2);
            } else {
                json = buildPageJson(page2Data, null, 1);
            }
            sendJson(exchange, json);
        });

        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 10, null)) {
            // Call getMetaData() BEFORE any next() — JDBC spec requires this to work
            java.sql.ResultSetMetaData meta = rs.getMetaData();

            // Metadata must be populated correctly
            assertThat(meta).isNotNull();
            assertThat(meta.getColumnCount()).isEqualTo(2);
            assertThat(meta.getColumnName(1)).isEqualTo("id");
            assertThat(meta.getColumnName(2)).isEqualTo("name");

            // After getMetaData(), cursor must still be at before-first position
            assertThat(rs.isBeforeFirst()).isTrue();
            assertThat(rs.getRow()).isEqualTo(0);

            // First next() must still return the first row (no data lost)
            assertThat(rs.next()).isTrue();
            assertThat(rs.getRow()).isEqualTo(1);
            assertThat(rs.getInt(1)).isEqualTo(10);
            assertThat(rs.getString(2)).isEqualTo("first");
            assertThat(rs.isFirst()).isTrue();

            // Continue iterating — all rows accessible
            assertThat(rs.next()).isTrue();
            assertThat(rs.getRow()).isEqualTo(2);
            assertThat(rs.getInt(1)).isEqualTo(20);

            // Third row on page 2
            assertThat(rs.next()).isTrue();
            assertThat(rs.getRow()).isEqualTo(3);
            assertThat(rs.getInt(1)).isEqualTo(30);

            assertThat(rs.next()).isFalse();
        }

        // getMetaData triggered 1 fetch, then next() for row 3 triggered another
        assertThat(pageRequestCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("isFirst() returns true only for the very first row of entire result set")
    void isFirstOnlyTrueForAbsoluteFirstRow() throws Exception {
        String queryId = "q-isfirst-001";
        AtomicInteger pageRequestCount = new AtomicInteger(0);

        byte[] page1Data = buildArrowPage(new int[]{1, 2}, new String[]{"a", "b"});
        byte[] page2Data = buildArrowPage(new int[]{3, 4}, new String[]{"c", "d"});

        server.createContext("/v1/query/" + queryId + "/result", exchange -> {
            int page = pageRequestCount.incrementAndGet();
            String json;
            if (page == 1) {
                json = buildPageJson(page1Data, "tok-2", 2);
            } else {
                json = buildPageJson(page2Data, null, 2);
            }
            sendJson(exchange, json);
        });

        server.start();

        PangrovaRestClient client = buildClient();
        try (ResultSet rs = new PaginatedResultSet(client, queryId, 2, null)) {
            // Row 1: isFirst() == true
            assertThat(rs.next()).isTrue();
            assertThat(rs.isFirst()).isTrue();

            // Row 2: isFirst() == false
            assertThat(rs.next()).isTrue();
            assertThat(rs.isFirst()).isFalse();

            // Row 3 (first row of page 2): isFirst() must still be false
            assertThat(rs.next()).isTrue();
            assertThat(rs.isFirst())
                    .as("isFirst() must be false for the first row of page 2")
                    .isFalse();

            // Row 4
            assertThat(rs.next()).isTrue();
            assertThat(rs.isFirst()).isFalse();
        }
    }

    // --- Helpers ---

    private PangrovaRestClient buildClient() {
        return PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(port)
                .connectTimeout(Duration.ofSeconds(5))
                .requestTimeout(Duration.ofSeconds(5))
                .build();
    }

    private byte[] buildArrowPage(int[] ids, String[] names) throws IOException {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));

        try (BufferAllocator alloc = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {

            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");

            idVec.allocateNew(ids.length);
            nameVec.allocateNew(ids.length);

            for (int i = 0; i < ids.length; i++) {
                idVec.set(i, ids[i]);
                nameVec.set(i, names[i].getBytes(StandardCharsets.UTF_8));
            }
            idVec.setValueCount(ids.length);
            nameVec.setValueCount(ids.length);
            root.setRowCount(ids.length);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    /**
     * Builds an Arrow IPC page with sequential integer ids starting at {@code startId}
     * and generated name strings. Used for large-page globalRowOffset tests.
     */
    private byte[] buildSequentialArrowPage(int startId, int count) throws IOException {
        int[] ids = new int[count];
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            ids[i] = startId + i;
            names[i] = "row-" + (startId + i);
        }
        return buildArrowPage(ids, names);
    }

    private String buildPageJson(byte[] arrowData, String nextToken, int rowCount) {
        String base64Data = Base64.getEncoder().encodeToString(arrowData);
        String tokenField = nextToken != null
                ? "\"next_token\":\"" + nextToken + "\""
                : "\"next_token\":null";
        return "{\"data\":\"" + base64Data + "\"," + tokenField + ",\"row_count\":" + rowCount + "}";
    }

    private static void sendJson(com.sun.net.httpserver.HttpExchange exchange, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
