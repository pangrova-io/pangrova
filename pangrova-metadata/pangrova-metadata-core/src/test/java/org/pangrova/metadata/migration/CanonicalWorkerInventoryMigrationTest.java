package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class CanonicalWorkerInventoryMigrationTest {

    @Test
    void removesOldRegistrationsBeforeEnforcingCanonicalInventory() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("worker_v30_upgrade");
        migrateTo(dataSource, "29");
        insertWorker(dataSource, "legacy-protocol", 1, "legacy", null);
        insertWorker(dataSource, "missing-inventory", 2, "VECTOR", null);
        insertWorker(dataSource, "canonical", 2, "VECTOR", "canonical-proto");

        migrateTo(dataSource, "30");

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery(
                    "SELECT worker_id,protocol_version,capability_inventory_proto "
                            + "FROM worker_registry ORDER BY worker_id")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("worker_id")).isEqualTo("canonical");
                assertThat(rows.getInt("protocol_version")).isEqualTo(2);
                assertThat(rows.getString("capability_inventory_proto"))
                        .isEqualTo("canonical-proto");
                assertThat(rows.next()).isFalse();
            }
            try (var columns = statement.executeQuery(
                    "SELECT column_name,is_nullable,column_default FROM information_schema.columns "
                            + "WHERE table_schema=current_schema() AND table_name='worker_registry' "
                            + "AND column_name IN ('capabilities','capability_inventory_proto') "
                            + "ORDER BY column_name")) {
                assertThat(columns.next()).isTrue();
                assertThat(columns.getString("column_name"))
                        .isEqualTo("capability_inventory_proto");
                assertThat(columns.getString("is_nullable")).isEqualTo("NO");
                assertThat(columns.next()).isFalse();
            }
        }
    }

    private static void migrateTo(DataSource dataSource, String version) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version))
                .load()
                .migrate();
    }

    private static void insertWorker(
            DataSource dataSource,
            String workerId,
            int protocolVersion,
            String capabilities,
            String inventory) throws Exception {
        String sql = "INSERT INTO worker_registry "
                + "(worker_id,host,http_port,transport_port,protocol_version,capabilities,"
                + "capability_inventory_proto,last_heartbeat_ms,registered_at_ms) "
                + "VALUES (?,?,?,?,?,?,?,?,?)";
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            statement.setString(1, workerId);
            statement.setString(2, "127.0.0.1");
            statement.setInt(3, 8081);
            statement.setInt(4, 8082);
            statement.setInt(5, protocolVersion);
            statement.setString(6, capabilities);
            statement.setString(7, inventory);
            statement.setLong(8, 1L);
            statement.setLong(9, 1L);
            statement.executeUpdate();
        }
    }
}
