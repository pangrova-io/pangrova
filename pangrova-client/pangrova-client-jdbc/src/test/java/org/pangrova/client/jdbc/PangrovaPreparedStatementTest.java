package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.transport.client.http.PangrovaRestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PangrovaPreparedStatement} parameter substitution,
 * lifecycle, and BI tool compatibility.
 */
@DisplayName("PangrovaPreparedStatement")
class PangrovaPreparedStatementTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Nested
    @DisplayName("Parameter substitution")
    class ParameterSubstitution {

        @Test
        @DisplayName("substitutes string parameter with proper quoting")
        void stringParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE name = ?");
            ps.setString(1, "hello");
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE name = 'hello'");
        }

        @Test
        @DisplayName("escapes single quotes in string values")
        void stringWithSingleQuotes() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE name = ?");
            ps.setString(1, "it's a test");
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE name = 'it''s a test'");
        }

        @Test
        @DisplayName("substitutes integer parameter")
        void intParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE id = ?");
            ps.setInt(1, 42);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE id = 42");
        }

        @Test
        @DisplayName("substitutes long parameter")
        void longParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE id = ?");
            ps.setLong(1, 9999999999L);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE id = 9999999999");
        }

        @Test
        @DisplayName("substitutes double parameter")
        void doubleParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE price = ?");
            ps.setDouble(1, 3.14);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE price = 3.14");
        }

        @Test
        @DisplayName("substitutes float parameter as double")
        void floatParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE val = ?");
            ps.setFloat(1, 2.5f);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE val = 2.5");
        }

        @Test
        @DisplayName("substitutes BigDecimal parameter without scientific notation")
        void bigDecimalParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE amount = ?");
            ps.setBigDecimal(1, new BigDecimal("123456789.123456789"));
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE amount = 123456789.123456789");
        }

        @Test
        @DisplayName("substitutes boolean parameter")
        void booleanParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE active = ?");
            ps.setBoolean(1, true);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE active = TRUE");
        }

        @Test
        @DisplayName("substitutes NULL parameter")
        void nullParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE name = ?");
            ps.setNull(1, Types.VARCHAR);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE name = NULL");
        }

        @Test
        @DisplayName("substitutes Date parameter with DATE literal")
        void dateParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE d = ?");
            ps.setDate(1, Date.valueOf("2024-01-15"));
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE d = DATE '2024-01-15'");
        }

        @Test
        @DisplayName("substitutes Timestamp parameter with TIMESTAMP literal")
        void timestampParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE ts = ?");
            ps.setTimestamp(1, Timestamp.valueOf("2024-01-15 10:30:00"));
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE ts = TIMESTAMP '2024-01-15 10:30:00.0'");
        }

        @Test
        @DisplayName("substitutes setObject with null as NULL")
        void setObjectNull() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE x = ?");
            ps.setObject(1, null);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE x = NULL");
        }

        @Test
        @DisplayName("substitutes multiple parameters in order")
        void multipleParameters() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?");
            ps.setInt(1, 1);
            ps.setString(2, "two");
            ps.setDouble(3, 3.0);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE a = 1 AND b = 'two' AND c = 3.0");
        }

        @Test
        @DisplayName("does not substitute ? inside single-quoted strings")
        void questionMarkInString() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE name = '?' AND id = ?");
            ps.setInt(1, 5);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT * FROM t WHERE name = '?' AND id = 5");
        }

        @Test
        @DisplayName("does not substitute ? inside double-quoted identifiers")
        void questionMarkInIdentifier() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT \"col?\" FROM t WHERE id = ?");
            ps.setInt(1, 7);
            assertThat(ps.substituteParameters()).isEqualTo("SELECT \"col?\" FROM t WHERE id = 7");
        }

        @Test
        @DisplayName("throws when parameter not set")
        void missingParameter() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE a = ? AND b = ?");
            ps.setInt(1, 1);
            assertThatThrownBy(ps::substituteParameters)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Parameter 2 not set");
        }

        @Test
        @DisplayName("clearParameters removes all bound values")
        void clearParameters() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE id = ?");
            ps.setInt(1, 42);
            ps.clearParameters();
            assertThatThrownBy(ps::substituteParameters)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Parameter 1 not set");
        }
    }

    @Nested
    @DisplayName("ParameterMetaData")
    class ParameterMetaDataTests {

        @Test
        @DisplayName("reports correct parameter count")
        void parameterCount() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE a = ? AND b = ?");
            ParameterMetaData meta = ps.getParameterMetaData();
            assertThat(meta.getParameterCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("reports zero parameters for SQL without placeholders")
        void noParameters() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT 1");
            ParameterMetaData meta = ps.getParameterMetaData();
            assertThat(meta.getParameterCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("throws on out-of-range parameter index")
        void outOfRange() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT * FROM t WHERE id = ?");
            ParameterMetaData meta = ps.getParameterMetaData();
            assertThatThrownBy(() -> meta.getParameterType(2))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("out of range");
        }
    }

    @Nested
    @DisplayName("Lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("prepareStatement returns functional PreparedStatement")
        void connectionReturnsPreparedStatement() throws SQLException {
            PangrovaConnection conn = createConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT 1");
            assertThat(ps).isInstanceOf(PangrovaPreparedStatement.class);
            assertThat(ps.isClosed()).isFalse();
            ps.close();
            assertThat(ps.isClosed()).isTrue();
        }

        @Test
        @DisplayName("throws on operations after close")
        void throwsAfterClose() throws SQLException {
            PangrovaPreparedStatement ps = createPreparedStatement("SELECT ?");
            ps.close();
            assertThatThrownBy(() -> ps.setInt(1, 1))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("all prepareStatement overloads return functional instances")
        void allOverloads() throws SQLException {
            PangrovaConnection conn = createConnection();
            assertThat(conn.prepareStatement("SELECT 1", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY))
                    .isInstanceOf(PangrovaPreparedStatement.class);
            assertThat(conn.prepareStatement("SELECT 1", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY, java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT))
                    .isInstanceOf(PangrovaPreparedStatement.class);
            assertThat(conn.prepareStatement("SELECT 1", java.sql.Statement.RETURN_GENERATED_KEYS))
                    .isInstanceOf(PangrovaPreparedStatement.class);
        }
    }

    @Nested
    @DisplayName("Placeholder counting")
    class PlaceholderCounting {

        @Test
        @DisplayName("counts placeholders correctly ignoring quoted strings")
        void countPlaceholders() {
            assertThat(PangrovaPreparedStatement.countPlaceholders("SELECT ? FROM t WHERE a = ? AND b = '?'")).isEqualTo(2);
            assertThat(PangrovaPreparedStatement.countPlaceholders("SELECT 1")).isEqualTo(0);
            assertThat(PangrovaPreparedStatement.countPlaceholders("INSERT INTO t VALUES (?, ?, ?)")).isEqualTo(3);
        }
    }

    private PangrovaPreparedStatement createPreparedStatement(String sql) {
        PangrovaConnection conn = createConnection();
        return new PangrovaPreparedStatement(conn, sql);
    }

    private PangrovaConnection createConnection() {
        PangrovaRestClient restClient = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(port)
                .connectTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
        return new PangrovaConnection(restClient, null);
    }
}
