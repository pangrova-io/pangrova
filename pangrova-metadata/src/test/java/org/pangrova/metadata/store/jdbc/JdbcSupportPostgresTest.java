package org.pangrova.metadata.store.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Round-trips metadata-owned {@link JdbcSupport} instants on self-created scratch tables. */
class JdbcSupportPostgresTest {

    @Test
    void preservesOneInstantAcrossConnectionsWithDifferentTimeZones() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("utc_instant");
        Instant source = Instant.parse("2026-09-19T04:05:06.123456789Z");
        Instant expected = Instant.parse("2026-09-19T04:05:06.123456Z");

        try (Connection utc = dataSource.getConnection()) {
            setTimeZone(utc, "UTC");
            try (var statement = utc.createStatement()) {
                statement.execute("CREATE TABLE absolute_time_test ("
                        + "id INT PRIMARY KEY, recorded_at TIMESTAMPTZ(6))");
            }
            try (var statement = utc.prepareStatement(
                    "INSERT INTO absolute_time_test (id,recorded_at) VALUES (?,?)")) {
                statement.setInt(1, 1);
                JdbcSupport.setInstant(statement, 2, source);
                statement.executeUpdate();
                statement.setInt(1, 2);
                JdbcSupport.setNullableInstant(statement, 2, null);
                statement.executeUpdate();
            }
        }

        try (Connection losAngeles = dataSource.getConnection()) {
            setTimeZone(losAngeles, "America/Los_Angeles");
            try (var statement = losAngeles.prepareStatement(
                    "SELECT recorded_at FROM absolute_time_test WHERE id=1");
                    var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(JdbcSupport.getInstant(result, "recorded_at")).isEqualTo(expected);
                assertThat(JdbcSupport.getInstant(result, 1)).isEqualTo(expected);
            }
            try (var statement = losAngeles.prepareStatement(
                    "SELECT recorded_at FROM absolute_time_test WHERE id=2");
                    var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(JdbcSupport.getInstant(result, "recorded_at")).isNull();
            }
        }
    }

    @Test
    void instantsSurviveCrossConnectionTimeZones() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("zone_scratch");
        Instant acquiredAt = Instant.parse("2026-09-19T04:05:06.123456Z");
        Instant expiresAt = Instant.parse("2026-09-19T04:06:06.654321Z");

        try (Connection utc = dataSource.getConnection()) {
            setTimeZone(utc, "UTC");
            try (var statement = utc.createStatement()) {
                statement.execute("CREATE TABLE jdbc_support_zone_probe ("
                        + "id TEXT PRIMARY KEY, acquired_at TIMESTAMPTZ(6), "
                        + "expires_at TIMESTAMPTZ(6))");
            }
            try (var statement = utc.prepareStatement(
                    "INSERT INTO jdbc_support_zone_probe (id,acquired_at,expires_at) "
                            + "VALUES (?,?,?)")) {
                statement.setString(1, "zone-1");
                JdbcSupport.setInstant(statement, 2, acquiredAt);
                JdbcSupport.setInstant(statement, 3, expiresAt);
                statement.executeUpdate();
                statement.setString(1, "zone-2");
                JdbcSupport.setNullableInstant(statement, 2, null);
                JdbcSupport.setInstant(statement, 3, expiresAt);
                statement.executeUpdate();
            }
        }

        try (Connection losAngeles = dataSource.getConnection()) {
            setTimeZone(losAngeles, "America/Los_Angeles");
            assertStoredInstants(losAngeles, "zone-1", acquiredAt, expiresAt);
            assertNullAcquiredAt(losAngeles, "zone-2", expiresAt);
        }
    }

    private static void assertStoredInstants(
            Connection connection, String id, Instant acquiredAt, Instant expiresAt)
            throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT acquired_at,expires_at FROM jdbc_support_zone_probe WHERE id=?");
                var result = execute(statement, id)) {
            assertThat(result.next()).isTrue();
            assertThat(JdbcSupport.getInstant(result, "acquired_at")).isEqualTo(acquiredAt);
            assertThat(JdbcSupport.getInstant(result, "expires_at")).isEqualTo(expiresAt);
        }
    }

    private static void assertNullAcquiredAt(
            Connection connection, String id, Instant expiresAt) throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT acquired_at,expires_at FROM jdbc_support_zone_probe WHERE id=?");
                var result = execute(statement, id)) {
            assertThat(result.next()).isTrue();
            assertThat(JdbcSupport.getInstant(result, "acquired_at")).isNull();
            assertThat(JdbcSupport.getInstant(result, "expires_at")).isEqualTo(expiresAt);
        }
    }

    private static java.sql.ResultSet execute(java.sql.PreparedStatement statement, String id)
            throws Exception {
        statement.setString(1, id);
        return statement.executeQuery();
    }

    private static void setTimeZone(Connection connection, String timeZone) throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT set_config('TimeZone', ?, false)")) {
            statement.setString(1, timeZone);
            statement.executeQuery();
        }
    }
}
