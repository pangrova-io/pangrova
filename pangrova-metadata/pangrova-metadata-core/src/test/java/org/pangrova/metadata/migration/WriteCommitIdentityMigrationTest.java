package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteCommitIdentityMigrationTest {

    @Test
    void purgesTerminalLegacyCommitOnNonemptyV24Upgrade() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("write_commit_v33_terminal");
        migrateTo(dataSource, "24");
        insertLegacyIntent(dataSource, "committed-before-purge", "COMMITTED");

        migrateTo(dataSource, "33");

        assertThat(count(dataSource, "SELECT count(*) FROM write_commit_journal")).isZero();
        assertThat(count(dataSource, "SELECT count(*) FROM information_schema.columns "
                + "WHERE table_schema=current_schema() AND table_name='write_commit_journal' "
                + "AND column_name IN ('tenant_id','base_snapshot_id',"
                + "'write_output_binding','commit_fingerprint') "
                + "AND is_nullable='NO'")).isEqualTo(4);
        assertThat(count(dataSource, "SELECT count(*) FROM flyway_schema_history "
                + "WHERE version='33' AND success")).isEqualTo(1);
    }

    @Test
    void unresolvedLegacyIntentBlocksUpgradeWithoutPurgingTerminalRows() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("write_commit_v33_pending");
        migrateTo(dataSource, "24");
        insertLegacyIntent(dataSource, "committed-before-purge", "COMMITTED");
        insertLegacyIntent(dataSource, "pending-reconciliation", "PENDING");

        assertThatThrownBy(() -> migrateTo(dataSource, "33"))
                .hasStackTraceContaining("Unresolved legacy write commit intents");

        assertThat(count(dataSource, "SELECT count(*) FROM write_commit_journal")).isEqualTo(2);
        assertThat(count(dataSource, "SELECT count(*) FROM information_schema.columns "
                + "WHERE table_schema=current_schema() AND table_name='write_commit_journal' "
                + "AND column_name='commit_fingerprint'")).isZero();
        assertThat(count(dataSource, "SELECT count(*) FROM flyway_schema_history "
                + "WHERE version='33' AND success")).isZero();
    }

    private static void migrateTo(DataSource dataSource, String version) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version))
                .load()
                .migrate();
    }

    private static void insertLegacyIntent(
            DataSource dataSource, String queryId, String state) throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "INSERT INTO write_commit_journal "
                        + "(query_id,catalog_name,table_name,state,timestamp_ms,"
                        + "idempotency_key,execution_fencing_token) VALUES (?,?,?,?,?,?,?)")) {
            statement.setString(1, queryId);
            statement.setString(2, "catalog");
            statement.setString(3, "schema.table");
            statement.setString(4, state);
            statement.setLong(5, 1L);
            statement.setString(6, queryId);
            statement.setLong(7, 0L);
            statement.executeUpdate();
        }
    }

    private static long count(DataSource dataSource, String sql) throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }
}
