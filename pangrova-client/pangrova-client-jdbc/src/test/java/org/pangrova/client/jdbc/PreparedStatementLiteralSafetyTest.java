package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the production literal boundary through public setObject; does not execute SQL. */
class PreparedStatementLiteralSafetyTest {
    private static final String NUMERIC_PAYLOAD = "0 OR TRUE";
    private static final String TEMPORAL_PAYLOAD = "2024-01-15' OR TRUE --";

    private HttpServer server;
    private final AtomicInteger queryRequests = new AtomicInteger();

    @BeforeEach
    void openActualConnectionTarget() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> {
            queryRequests.incrementAndGet();
            try (exchange) {
                exchange.sendResponseHeaders(400, -1);
            }
        });
        server.start();
    }

    @AfterEach
    void closeActualConnectionTarget() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void arbitraryNumberCannotRenderSqlSyntaxThroughSetObject() {
        Number value = new Number() {
            @Override
            public int intValue() {
                return 0;
            }

            @Override
            public long longValue() {
                return 0;
            }

            @Override
            public float floatValue() {
                return 0;
            }

            @Override
            public double doubleValue() {
                return 0;
            }

            @Override
            public String toString() {
                return NUMERIC_PAYLOAD;
            }
        };

        assertUnsupportedNumericValue(value);
    }

    @Test
    void decimalSubclassCannotRenderSqlSyntaxThroughSetObject() {
        BigDecimal value = new BigDecimal("0") {
            @Override
            public String toPlainString() {
                return NUMERIC_PAYLOAD;
            }

            @Override
            public String toString() {
                return NUMERIC_PAYLOAD;
            }
        };

        assertUnsupportedNumericValue(value);
    }

    @Test
    void dateSubclassUsesItsActualDateInsteadOfOverriddenSqlText() throws SQLException {
        Date value = new Date(Date.valueOf("2024-01-15").getTime()) {
            @Override
            public String toString() {
                return TEMPORAL_PAYLOAD;
            }
        };

        assertThat(renderPublicBoundValue(value)).isEqualTo("SELECT DATE '2024-01-15'");
    }

    @Test
    void timeSubclassUsesItsActualTimeInsteadOfOverriddenSqlText() throws SQLException {
        Time value = new Time(Time.valueOf("10:30:45").getTime()) {
            @Override
            public String toString() {
                return TEMPORAL_PAYLOAD;
            }
        };

        assertThat(renderPublicBoundValue(value)).isEqualTo("SELECT TIME '10:30:45'");
    }

    @Test
    void timestampSubclassPreservesActualNanosWithoutOverriddenSqlText() throws SQLException {
        Timestamp original = Timestamp.valueOf("2024-01-15 10:30:45.123456789");
        Timestamp value = new Timestamp(original.getTime()) {
            @Override
            public String toString() {
                return TEMPORAL_PAYLOAD;
            }
        };
        value.setNanos(original.getNanos());

        assertThat(renderPublicBoundValue(value))
                .isEqualTo("SELECT TIMESTAMP '2024-01-15 10:30:45.123456789'");
    }

    @Test
    void actualBigIntegerRetainsItsFullDecimalPrecision() throws SQLException {
        assertThat(renderPublicBoundValue(new BigInteger("123456789012345678901234567890")))
                .isEqualTo("SELECT 123456789012345678901234567890");
    }

    @Test
    void actualFiniteFloatRetainsItsNumericValue() throws SQLException {
        assertThat(renderPublicBoundValue(2.5f)).isEqualTo("SELECT 2.5");
        assertThat(renderPublicBoundValue(-2.5f)).isEqualTo("SELECT -2.5");
    }

    @Test
    void actualFiniteDoubleRetainsItsNumericValue() throws SQLException {
        assertThat(renderPublicBoundValue(3.14d)).isEqualTo("SELECT 3.14");
        assertThat(renderPublicBoundValue(-3.14d)).isEqualTo("SELECT -3.14");
    }

    @Test
    void nonFiniteNumbersFailWithNumericRangeState() {
        for (Number value : new Number[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> renderPublicBoundValue(value))
                    .isInstanceOfSatisfying(SQLException.class,
                            failure -> assertThat(failure.getSQLState()).isEqualTo("22003"));
        }
    }

    @Test
    void maliciousTextRemainsOneQuotedValue() throws SQLException {
        assertThat(renderPublicBoundValue("x' OR TRUE --"))
                .isEqualTo("SELECT 'x'' OR TRUE --'");
    }

    private void assertUnsupportedNumericValue(Object value) {
        assertThatThrownBy(() -> renderPublicBoundValue(value))
                .isInstanceOfSatisfying(SQLException.class,
                        failure -> assertThat(failure.getSQLState()).isEqualTo("07006"));
        var client = PangrovaRestClient.builder()
                .coordinatorHost(server.getAddress().getHostString())
                .coordinatorPort(server.getAddress().getPort()).build();
        assertThatThrownBy(() -> {
            try (var connection = new PangrovaConnection(client, null);
                 var statement = connection.prepareStatement("SELECT ?")) {
                statement.setObject(1, value);
                statement.executeQuery();
            }
        }).isInstanceOfSatisfying(SQLException.class,
                failure -> assertThat(failure.getSQLState()).isEqualTo("07006"));
        assertThat(queryRequests.get()).isZero();
    }

    private String renderPublicBoundValue(Object value) throws SQLException {
        var client = PangrovaRestClient.builder()
                .coordinatorHost(server.getAddress().getHostString())
                .coordinatorPort(server.getAddress().getPort())
                .build();
        try (var connection = new PangrovaConnection(client, null);
             var statement = (PangrovaPreparedStatement) connection.prepareStatement("SELECT ?")) {
            statement.setObject(1, value);
            return statement.substituteParameters();
        }
    }
}
