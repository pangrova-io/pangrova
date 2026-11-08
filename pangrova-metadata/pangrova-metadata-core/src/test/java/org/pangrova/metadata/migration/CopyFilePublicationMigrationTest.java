package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the one-time fingerprint upgrade against actual PostgreSQL migrations. */
class CopyFilePublicationMigrationTest {

    @Test
    void emptyJournalMigratesAndInstallsPublicationAuthority() throws Exception {
        DataSource source = SharedPostgresContainer.isolatedDataSource("copy_publication_fresh");
        migrateTo(source, "57");

        migrateTo(source, "58");

        assertThat(count(source, "SELECT count(*) FROM information_schema.tables "
                + "WHERE table_schema=current_schema() AND table_name='copy_file_publications'"))
                .isEqualTo(1);
        assertThat(count(source, "SELECT count(*) FROM information_schema.columns "
                + "WHERE table_schema=current_schema() AND table_name='write_commit_journal' "
                + "AND column_name IN ('copy_claim_key','copy_load_generation',"
                + "'copy_claim_digest','copy_table_uuid')")).isEqualTo(4);
    }

    @Test
    void everyOldJournalStateBlocksUpgradeAtomically() throws Exception {
        for (String state : new String[]{"PENDING", "COMMITTED", "ABORTED"}) {
            DataSource source = SharedPostgresContainer.isolatedDataSource("copy_upgrade_" + state);
            migrateTo(source, "57");
            try (var connection = source.getConnection();
                    var statement = connection.prepareStatement(
                            "INSERT INTO write_commit_journal "
                            + "(query_id,catalog_name,table_name,state,timestamp_ms,"
                            + "idempotency_key,execution_fencing_token,tenant_id,base_snapshot_id,"
                            + "write_output_binding,commit_fingerprint) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setString(1, "old-" + state);
                statement.setString(2, "iceberg");
                statement.setString(3, "sales.orders");
                statement.setString(4, state);
                statement.setLong(5, 1L);
                statement.setString(6, "a".repeat(64));
                statement.setLong(7, 0L);
                statement.setString(8, "tenant-a");
                statement.setLong(9, -1L);
                statement.setString(10, "old-binding");
                statement.setString(11, "a".repeat(64));
                statement.executeUpdate();
            }

            assertThatThrownBy(() -> migrateTo(source, "58"))
                    .hasStackTraceContaining("Drain every write_commit_journal row");
            assertThat(count(source, "SELECT count(*) FROM write_commit_journal")).isEqualTo(1);
            assertThat(count(source, "SELECT count(*) FROM information_schema.tables "
                    + "WHERE table_schema=current_schema() AND table_name='copy_file_publications'"))
                    .isZero();
            assertThat(count(source, "SELECT count(*) FROM flyway_schema_history "
                    + "WHERE version='58' AND success")).isZero();
        }
    }

    private static void migrateTo(DataSource source, String version) {
        Flyway.configure().dataSource(source).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version)).load().migrate();
    }

    private static long count(DataSource source, String sql) throws Exception {
        try (var connection = source.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }
}
