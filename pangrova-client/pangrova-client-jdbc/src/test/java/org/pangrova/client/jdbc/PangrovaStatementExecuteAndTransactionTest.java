package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.transport.client.http.PangrovaRestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for F-COMMON-04 fix: Statement.execute()/executeUpdate() support
 * and Connection transaction method semantics.
 */
@DisplayName("F-COMMON-04: execute/executeUpdate + transaction semantics")
class PangrovaStatementExecuteAndTransactionTest {

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

    @Nested
    @DisplayName("Statement.execute()")
    class ExecuteTests {

        @Test
        @DisplayName("execute() rejects a completed SELECT without a Flight result endpoint")
        void executeSelectRequiresFlightEndpoint() throws Exception {
            AtomicReference<String> capturedSql = new AtomicReference<>();
            registerFinishedQueryEndpoint(capturedSql, "q-exec-select");
            registerStatusWithoutStats("q-exec-select");
            AtomicInteger cancellations = registerCancelEndpoint("q-exec-select", false);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                assertThatThrownBy(() -> stmt.execute("SELECT 1"))
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("no result endpoint available");
                assertThat(stmt.getResultSet()).isNull();
                assertThat(capturedSql.get()).contains("SELECT 1");
            }
            assertThat(cancellations.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("execute() returns false for INSERT and provides update count")
        void executeInsertReturnsFalse() throws Exception {
            AtomicReference<String> capturedSql = new AtomicReference<>();
            registerFinishedQueryEndpoint(capturedSql, "q-exec-insert");
            registerStatusWithStats("q-exec-insert", 5);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("INSERT INTO t VALUES (1, 'a')");
                assertThat(hasResultSet).isFalse();
                assertThat(stmt.getResultSet()).isNull();
                assertThat(stmt.getUpdateCount()).isEqualTo(5);
                assertThat(capturedSql.get()).contains("INSERT INTO t VALUES");
            }
        }

        @Test
        @DisplayName("execute() returns false for UPDATE")
        void executeUpdateReturnsFalse() throws Exception {
            AtomicReference<String> capturedSql = new AtomicReference<>();
            registerFinishedQueryEndpoint(capturedSql, "q-exec-update");
            registerStatusWithStats("q-exec-update", 3);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("UPDATE t SET x = 1 WHERE y = 2");
                assertThat(hasResultSet).isFalse();
                assertThat(stmt.getUpdateCount()).isEqualTo(3);
            }
        }

        @Test
        @DisplayName("execute() returns false for DELETE")
        void executeDeleteReturnsFalse() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-exec-delete");
            registerStatusWithStats("q-exec-delete", 10);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("DELETE FROM t WHERE id = 99");
                assertThat(hasResultSet).isFalse();
                assertThat(stmt.getUpdateCount()).isEqualTo(10);
            }
        }

        @Test
        @DisplayName("execute() returns false for DDL (CREATE TABLE)")
        void executeDdlReturnsFalse() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-exec-ddl");
            registerStatusWithStats("q-exec-ddl", 0);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("CREATE TABLE t (id INT)");
                assertThat(hasResultSet).isFalse();
                assertThat(stmt.getUpdateCount()).isEqualTo(0);
            }
        }

        @Test
        @DisplayName("execute() overloads delegate to execute(sql)")
        void executeOverloads() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-exec-ovl");
            registerStatusWithStats("q-exec-ovl", 2);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                assertThat(stmt.execute("INSERT INTO t VALUES (1)", Statement.NO_GENERATED_KEYS)).isFalse();
            }
        }

        @Test
        @DisplayName("execute() throws on closed statement")
        void executeOnClosedStatement() throws Exception {
            server.start();
            PangrovaConnection conn = createConnection();
            Statement stmt = conn.createStatement();
            stmt.close();
            assertThatThrownBy(() -> stmt.execute("SELECT 1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Nested
    @DisplayName("Statement.executeUpdate()")
    class ExecuteUpdateTests {

        @Test
        @DisplayName("executeUpdate() returns affected row count for INSERT")
        void executeUpdateInsert() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-upd-insert");
            registerStatusWithStats("q-upd-insert", 7);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                int count = stmt.executeUpdate("INSERT INTO t SELECT * FROM s");
                assertThat(count).isEqualTo(7);
                assertThat(stmt.getUpdateCount()).isEqualTo(7);
                assertThat(stmt.getResultSet()).isNull();
            }
        }

        @Test
        @DisplayName("executeUpdate() returns 0 when stats are absent")
        void executeUpdateNoStats() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-upd-nostats");
            registerStatusWithoutStats("q-upd-nostats");
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                int count = stmt.executeUpdate("DROP TABLE IF EXISTS t");
                assertThat(count).isEqualTo(0);
            }
        }

        @Test
        @DisplayName("executeUpdate() preserves the JDBC timeout exception")
        void executeUpdatePreservesTimeout() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-upd-timeout");
            AtomicInteger cancellations = registerCancelEndpoint("q-upd-timeout", true);
            server.createContext("/v1/query/q-upd-timeout/state", exchange ->
                    sendJson(exchange, 200,
                            "{\"query_id\":\"q-upd-timeout\",\"state\":\"RUNNING\"}"));
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);
                assertThatThrownBy(() -> stmt.executeUpdate("INSERT INTO t VALUES (1)"))
                        .isInstanceOf(SQLTimeoutException.class)
                        .hasMessageContaining("q-upd-timeout");
            }
            assertThat(cancellations.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("executeUpdate() overloads delegate correctly")
        void executeUpdateOverloads() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-upd-ovl");
            registerStatusWithStats("q-upd-ovl", 1);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                assertThat(stmt.executeUpdate("INSERT INTO t VALUES (1)", Statement.NO_GENERATED_KEYS))
                        .isEqualTo(1);
            }
        }

        @Test
        @DisplayName("executeUpdate() throws on closed statement")
        void executeUpdateOnClosedStatement() throws Exception {
            server.start();
            PangrovaConnection conn = createConnection();
            Statement stmt = conn.createStatement();
            stmt.close();
            assertThatThrownBy(() -> stmt.executeUpdate("INSERT INTO t VALUES (1)"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Nested
    @DisplayName("Statement.getMoreResults()")
    class GetMoreResultsTests {

        @Test
        @DisplayName("getMoreResults() returns false and clears state")
        void getMoreResultsClearsState() throws Exception {
            registerFinishedQueryEndpoint(new AtomicReference<>(), "q-more");
            registerStatusWithStats("q-more", 2);
            server.start();

            try (PangrovaConnection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO t VALUES (1)");
                assertThat(stmt.getUpdateCount()).isEqualTo(2);
                assertThat(stmt.getMoreResults()).isFalse();
                assertThat(stmt.getUpdateCount()).isEqualTo(-1);
                assertThat(stmt.getResultSet()).isNull();
            }
        }
    }

    @Nested
    @DisplayName("Connection transaction semantics")
    class TransactionTests {

        @Test
        @DisplayName("getAutoCommit() returns true by default")
        void autoCommitDefault() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                assertThat(conn.getAutoCommit()).isTrue();
            }
        }

        @Test
        @DisplayName("setAutoCommit(true) is a no-op")
        void setAutoCommitTrueIsNoop() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                conn.setAutoCommit(true); // should not throw
                assertThat(conn.getAutoCommit()).isTrue();
            }
        }

        @Test
        @DisplayName("setAutoCommit(false) throws SQLFeatureNotSupportedException")
        void setAutoCommitFalseThrows() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                assertThatThrownBy(() -> conn.setAutoCommit(false))
                        .isInstanceOf(SQLFeatureNotSupportedException.class)
                        .hasMessageContaining("does not support explicit transactions");
            }
        }

        @Test
        @DisplayName("commit() is a no-op in auto-commit mode")
        void commitIsNoop() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                conn.commit(); // should not throw
            }
        }

        @Test
        @DisplayName("rollback() throws SQLFeatureNotSupportedException")
        void rollbackThrows() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                assertThatThrownBy(conn::rollback)
                        .isInstanceOf(SQLFeatureNotSupportedException.class)
                        .hasMessageContaining("does not support rollback");
            }
        }

        @Test
        @DisplayName("rollback(Savepoint) throws SQLFeatureNotSupportedException")
        void rollbackWithSavepointThrows() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                assertThatThrownBy(() -> conn.rollback(null))
                        .isInstanceOf(SQLFeatureNotSupportedException.class);
            }
        }

        @Test
        @DisplayName("Connection.isReadOnly() defaults to false")
        void isReadOnlyDefaultFalse() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                assertThat(conn.isReadOnly()).isFalse();
            }
        }

        @Test
        @DisplayName("Connection.setReadOnly()/isReadOnly() roundtrips")
        void setReadOnlyRoundtrip() throws Exception {
            server.start();
            try (PangrovaConnection conn = createConnection()) {
                conn.setReadOnly(true);
                assertThat(conn.isReadOnly()).isTrue();
                conn.setReadOnly(false);
                assertThat(conn.isReadOnly()).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("isUpdateStatement() heuristic")
    class IsUpdateStatementTests {

        @Test
        @DisplayName("recognizes all DML keywords")
        void dmlKeywords() {
            assertThat(PangrovaStatement.isUpdateStatement("INSERT INTO t VALUES (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("UPDATE t SET x = 1")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("DELETE FROM t")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("MERGE INTO t USING s ON ...")).isTrue();
        }

        @Test
        @DisplayName("recognizes all DDL keywords")
        void ddlKeywords() {
            assertThat(PangrovaStatement.isUpdateStatement("CREATE TABLE t (id INT)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("DROP TABLE t")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("ALTER TABLE t ADD COLUMN x INT")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("TRUNCATE TABLE t")).isTrue();
        }

        @Test
        @DisplayName("recognizes Pangrova-specific update keywords")
        void pangrovaSpecificKeywords() {
            assertThat(PangrovaStatement.isUpdateStatement("COMPACT TABLE t")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("VACUUM TABLE t")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("GRANT SELECT ON t TO user1")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("REVOKE ALL ON t FROM user1")).isTrue();
        }

        @Test
        @DisplayName("classifies SELECT/SHOW/EXPLAIN as queries")
        void queryKeywords() {
            assertThat(PangrovaStatement.isUpdateStatement("SELECT * FROM t")).isFalse();
            assertThat(PangrovaStatement.isUpdateStatement("SHOW TABLES")).isFalse();
            assertThat(PangrovaStatement.isUpdateStatement("EXPLAIN SELECT 1")).isFalse();
            assertThat(PangrovaStatement.isUpdateStatement("DESCRIBE t")).isFalse();
            assertThat(PangrovaStatement.isUpdateStatement("WITH cte AS (SELECT 1) SELECT * FROM cte")).isFalse();
        }

        @Test
        @DisplayName("handles leading whitespace")
        void leadingWhitespace() {
            assertThat(PangrovaStatement.isUpdateStatement("  INSERT INTO t VALUES (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("\n\tSELECT 1")).isFalse();
        }

        @Test
        @DisplayName("handles leading line comments")
        void lineComments() {
            assertThat(PangrovaStatement.isUpdateStatement("-- comment\nINSERT INTO t VALUES (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("-- comment\nSELECT 1")).isFalse();
        }

        @Test
        @DisplayName("handles leading block comments")
        void blockComments() {
            assertThat(PangrovaStatement.isUpdateStatement("/* block */INSERT INTO t VALUES (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("/* comment */ SELECT 1")).isFalse();
        }

        @Test
        @DisplayName("is case-insensitive")
        void caseInsensitive() {
            assertThat(PangrovaStatement.isUpdateStatement("insert into t values (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("Insert Into t Values (1)")).isTrue();
            assertThat(PangrovaStatement.isUpdateStatement("select 1")).isFalse();
        }
    }

    // --- Test infrastructure ---

    private PangrovaConnection createConnection() {
        PangrovaRestClient restClient = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(port)
                .connectTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
        return new PangrovaConnection(restClient, null);
    }

    /** Complete protocol acknowledgement; it does not assert a remote resource-drain receipt. */
    private AtomicInteger registerCancelEndpoint(String queryId, boolean accepted) {
        AtomicInteger requests = new AtomicInteger();
        server.createContext("/v1/query/" + queryId + "/cancel", exchange -> {
            requests.incrementAndGet();
            String response = "{\"query_id\":\"%s\",\"cancelled\":%s,\"outcome\":\"%s\"}"
                    .formatted(queryId, accepted, accepted ? "CANCEL_ACCEPTED" : "ALREADY_TERMINAL");
            sendJson(exchange, accepted ? 202 : 200, response);
        });
        return requests;
    }

    /**
     * Registers a /v1/query/submit endpoint that returns FINISHED immediately.
     */
    private void registerFinishedQueryEndpoint(AtomicReference<String> capturedSql, String queryId) {
        server.createContext("/v1/query/submit", exchange -> {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            capturedSql.set(requestBody);
            String response = """
                    {
                      "query_id": "%s",
                      "state": "FINISHED",
                      "column_names": []
                    }
                    """.formatted(queryId);
            sendJson(exchange, 200, response);
        });
    }

    /**
     * Registers a status endpoint that returns FINISHED with stats showing the given row count.
     */
    private void registerStatusWithStats(String queryId, int rowCount) {
        server.createContext("/v1/query/" + queryId + "/state", exchange -> {
            String response = """
                    {
                      "query_id": "%s",
                      "state": "FINISHED",
                      "stats": {
                        "elapsed_time_ms": 100,
                        "queued_time_ms": 10,
                        "rows_returned": %d
                      }
                    }
                    """.formatted(queryId, rowCount);
            sendJson(exchange, 200, response);
        });
    }

    /**
     * Registers a status endpoint that returns FINISHED without stats.
     */
    private void registerStatusWithoutStats(String queryId) {
        server.createContext("/v1/query/" + queryId + "/state", exchange -> {
            String response = """
                    {
                      "query_id": "%s",
                      "state": "FINISHED"
                    }
                    """.formatted(queryId);
            sendJson(exchange, 200, response);
        });
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
