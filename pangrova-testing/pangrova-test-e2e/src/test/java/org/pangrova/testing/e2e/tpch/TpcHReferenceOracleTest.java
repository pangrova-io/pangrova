package org.pangrova.testing.e2e.tpch;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.e2e.SharedTestContainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the independent reference fixture; not a Pangrova SQL E2E claim. */
class TpcHReferenceOracleTest extends ContainerTestBase {
    @Test
    void inputRoundTripsDatesNullsAndNumericAggregates() throws Exception {
        try (var oracle = new TpcHReferenceOracle(connection())) {
            oracle.executeInput("CREATE TABLE sample (id BIGINT, price DOUBLE PRECISION, day DATE, note VARCHAR)");
            oracle.executeInput("INSERT INTO sample VALUES (1, 2.5, DATE '1995-01-01', NULL), "
                    + "(2, 3.5, DATE '1995-01-02', 'sample')");
            var rows = oracle.query("SELECT SUM(price) AS total, COUNT(*) AS num FROM sample");
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("total")).isEqualTo(6.0);
            assertThat(rows.get(0).getRaw("num")).isEqualTo(2L);
            var row = oracle.query("SELECT day, note FROM sample WHERE id = 1").get(0);
            assertThat(row.getRaw("day")).isEqualTo(LocalDate.of(1995, 1, 1));
            assertThat(row.getRaw("note")).isNull();
        }
    }

    @Test
    void invalidQueryIsNotConvertedToEmptyResults() throws Exception {
        try (var oracle = new TpcHReferenceOracle(connection())) {
            assertThatThrownBy(() -> oracle.query("SELECT * FROM absent_table"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reference query failed")
                    .hasCauseInstanceOf(java.sql.SQLException.class);
        }
    }

    @Test
    void invalidInputIsNotAccepted() throws Exception {
        try (var oracle = new TpcHReferenceOracle(connection())) {
            assertThatThrownBy(() -> oracle.executeInput("invalid input"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reference input failed");
        }
    }

    @Test
    void schemasAreIsolatedAndConnectionsClose() throws Exception {
        Connection first = connection();
        Connection second = connection();
        try (var left = new TpcHReferenceOracle(first);
             var right = new TpcHReferenceOracle(second)) {
            left.executeInput("CREATE TABLE sample (id BIGINT)");
            right.executeInput("CREATE TABLE sample (id BIGINT)");
            left.executeInput("INSERT INTO sample VALUES (7)");
            assertThat(left.query("SELECT id FROM sample")).hasSize(1);
            assertThat(right.query("SELECT id FROM sample")).isEmpty();
        }
        assertThat(first.isClosed()).isTrue();
        assertThat(second.isClosed()).isTrue();
    }

    private static Connection connection() throws Exception {
        var pg = SharedTestContainers.POSTGRES;
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }
}
