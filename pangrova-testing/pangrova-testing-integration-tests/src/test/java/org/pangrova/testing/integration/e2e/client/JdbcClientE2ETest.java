package org.pangrova.testing.integration.e2e.client;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for JDBC client full query lifecycle.
 *
 * <p>Validates the complete JDBC path through real infrastructure:
 * DriverManager → PangrovaDriver → PangrovaConnection → PangrovaStatement →
 * HTTP submit → Coordinator pipeline → Transport fetch →
 * PangrovaResultSet → JDBC ResultSet API.
 *
 * <p>Uses the real coordinator HTTP server started by SqlE2ETestBase.
 */
@DisplayName("JDBC Client E2E — Full Query Lifecycle")
class JdbcClientE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String testTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTestTable() {
        String suffix = Long.toHexString(System.nanoTime());
        testTable = "jdbc_test_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, testTable,
                "id BIGINT, name VARCHAR, amount BIGINT");
        insertTestData();
    }

    // -----------------------------------------------------------------------
    // JDBC Connection lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("JDBC Connection Lifecycle")
    class ConnectionLifecycle {

        @Test
        @DisplayName("DriverManager opens and closes connection without error")
        void connectionOpensAndCloses() throws SQLException {
            try (Connection conn = openJdbcConnection()) {
                assertThat(conn.isClosed()).isFalse();
            }
        }

        @Test
        @DisplayName("Closed connection rejects new statements")
        void closedConnectionRejectsStatements() throws SQLException {
            Connection conn = openJdbcConnection();
            conn.close();

            assertThatThrownBy(conn::createStatement)
                    .isInstanceOf(SQLException.class);
        }
    }

    // -----------------------------------------------------------------------
    // JDBC Statement execution
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("JDBC Statement Execution")
    class StatementExecution {

        @Test
        @DisplayName("executeQuery returns ResultSet with correct data")
        void executeQueryReturnsCorrectData() throws SQLException {
            try (Connection conn = openJdbcConnection();
                 Statement stmt = conn.createStatement()) {

                String sql = String.format(
                        "SELECT id, name, amount FROM %s.%s.%s ORDER BY id",
                        CATALOG, SCHEMA, testTable);
                ResultSet rs = stmt.executeQuery(sql);

                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
                assertThat(ids).hasSize(10);
                assertThat(ids).containsExactly(
                        1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
            }
        }

        @Test
        @DisplayName("Aggregation query via JDBC returns correct sum")
        void aggregationViaJdbcReturnsCorrectSum() throws SQLException {
            try (Connection conn = openJdbcConnection();
                 Statement stmt = conn.createStatement()) {

                String sql = String.format(
                        "SELECT SUM(amount) AS total FROM %s.%s.%s",
                        CATALOG, SCHEMA, testTable);
                ResultSet rs = stmt.executeQuery(sql);

                assertThat(rs.next()).isTrue();
                long total = rs.getLong("total");
                assertThat(total).isEqualTo(550L);
            }
        }
    }

    // -----------------------------------------------------------------------
    // ResultSet metadata
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("JDBC ResultSet Metadata")
    class ResultSetMetadataTests {

        @Test
        @DisplayName("ResultSetMetaData reports correct column count and names")
        void resultSetMetadataCorrect() throws SQLException {
            try (Connection conn = openJdbcConnection();
                 Statement stmt = conn.createStatement()) {

                String sql = String.format(
                        "SELECT id, name, amount FROM %s.%s.%s LIMIT 1",
                        CATALOG, SCHEMA, testTable);
                ResultSet rs = stmt.executeQuery(sql);
                ResultSetMetaData meta = rs.getMetaData();

                assertThat(meta.getColumnCount()).isEqualTo(3);
                assertThat(meta.getColumnName(1).toLowerCase()).isEqualTo("id");
                assertThat(meta.getColumnName(2).toLowerCase()).isEqualTo("name");
                assertThat(meta.getColumnName(3).toLowerCase()).isEqualTo("amount");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Cross-validation: JDBC vs direct pipeline
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("JDBC vs Direct Pipeline Equivalence")
    class JdbcDirectEquivalence {

        @Test
        @DisplayName("JDBC query result matches direct pipeline result")
        void jdbcMatchesDirectPipeline() throws SQLException {
            String sql = String.format(
                    "SELECT id, amount FROM %s.%s.%s ORDER BY id",
                    CATALOG, SCHEMA, testTable);

            List<TypedRow> directRows = sqlRunner.execute(sql);

            List<Long> jdbcAmounts = new ArrayList<>();
            try (Connection conn = openJdbcConnection();
                 Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    jdbcAmounts.add(rs.getLong("amount"));
                }
            }

            List<Long> directAmounts = directRows.stream()
                    .map(r -> r.getLong("amount"))
                    .toList();

            assertThat(jdbcAmounts).isEqualTo(directAmounts);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Connection openJdbcConnection() throws SQLException {
        String url = String.format(
                "jdbc:pangrova://localhost:%d/%s", coordinatorHttpPort(), CATALOG);
        Properties credentials = new Properties();
        credentials.setProperty("token", E2E_USER_BEARER_TOKEN);
        return DriverManager.getConnection(url, credentials);
    }

    private void insertTestData() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, name, amount) VALUES ", CATALOG, SCHEMA, testTable));
        for (int i = 1; i <= 10; i++) {
            if (i > 1) sb.append(", ");
            sb.append(String.format("(%d, 'item_%d', %d)", i, i, i * 10L));
        }
        sqlRunner.execute(sb.toString());
    }
}
