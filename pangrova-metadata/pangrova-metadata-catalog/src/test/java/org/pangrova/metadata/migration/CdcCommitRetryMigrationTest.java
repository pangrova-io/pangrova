package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcCommitRetryMigrationTest {

    @Test
    void rolledBackBatchCanRetryButTwoActiveOwnersCannotShareAnIdentity() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("cdc_retry_v39");
        migrateTo(dataSource, "38");
        insert(dataSource, "first", "ROLLED_BACK");

        migrateTo(dataSource, "39");
        insert(dataSource, "retry", "PREPARED");

        assertThatThrownBy(() -> insert(dataSource, "second-active", "PREPARED"))
                .isInstanceOf(SQLException.class)
                .satisfies(failure -> assertThat(((SQLException) failure).getSQLState())
                        .isEqualTo("23505"));
        assertThat(count(dataSource)).isEqualTo(2);
    }

    private static void migrateTo(DataSource dataSource, String version) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version))
                .load()
                .migrate();
    }

    private static void insert(DataSource dataSource, String id, String state) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "INSERT INTO cdc_commit_state "
                        + "(id,source_id,partition_num,start_offset,end_offset,state,"
                        + "deadline,created_at,batch_key) "
                        + "VALUES (?, 'source', 0, 0, 9, ?, now(), now(), 'batch')")) {
            statement.setString(1, id);
            statement.setString(2, state);
            statement.executeUpdate();
        }
    }

    private static long count(DataSource dataSource) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT count(*) FROM cdc_commit_state")) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }
}
