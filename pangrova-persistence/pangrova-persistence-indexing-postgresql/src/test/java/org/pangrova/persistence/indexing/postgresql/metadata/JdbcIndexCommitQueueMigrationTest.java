package org.pangrova.persistence.indexing.postgresql.metadata;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcIndexCommitQueueMigrationTest {

    @Test
    void existingPendingEventsReceiveDurablePerTableOrder() throws SQLException {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("commit_migration");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("37").load().migrate();
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO index_commit_log "
                    + "(tenant_id,catalog_name,schema_name,table_name,snapshot_id,"
                    + "added_files,removed_files,event_type,processed,created_at) VALUES "
                    + "('tenant','catalog','schema','hot','late','[]','[]','APPEND',FALSE,"
                    + "'2026-09-05 01:02:05+00'),"
                    + "('tenant','catalog','schema','hot','early','[]','[]','APPEND',FALSE,"
                    + "'2026-09-05 01:02:03+00'),"
                    + "('tenant','catalog','schema','other','other','[]','[]','APPEND',FALSE,"
                    + "'2026-09-05 01:02:04+00')");
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .load().migrate();
        JdbcIndexMetadataStore store = new JdbcIndexMetadataStore(dataSource, org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent);

        assertThat(store.pollUnprocessedCommitEvents(3, 1L))
                .extracting(CommitLogEntry::snapshotId)
                .containsExactly("early", "other", "late");
    }
}
