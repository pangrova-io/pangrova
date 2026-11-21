package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.indexing.api.metadata.CommitEventRequest;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcIndexCommitQueueScaleTest {

    @Test
    void millionEventHotTableDoesNotRequireBacklogScanOrHideAnotherHead()
            throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("commit_scale");
        var store = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        store.recordCommitEvent(new CommitEventRequest(
                "tenant", "catalog", "schema", "hot", "hot-head",
                "[]", "[]", "APPEND", now));
        store.recordCommitEvent(new CommitEventRequest(
                "tenant", "catalog", "schema", "other", "other-head",
                "[]", "[]", "APPEND", now));
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO index_commit_log "
                    + "(tenant_id,catalog_name,schema_name,table_name,snapshot_id,"
                    + "added_files,removed_files,event_type,processed,created_at,table_sequence) "
                    + "SELECT 'tenant','catalog','schema','hot','backlog',"
                    + "'[]'::jsonb,'[]'::jsonb,'APPEND',FALSE,"
                    + "'2026-09-05 01:02:03+00'::timestamptz,n "
                    + "FROM generate_series(2,1000000) AS n");
            statement.executeUpdate("UPDATE index_commit_table_heads "
                    + "SET next_sequence=1000001 WHERE table_name='hot'");
            statement.execute("ANALYZE index_commit_log");
        }

        StringBuilder plan = new StringBuilder();
        try (var connection = dataSource.getConnection();
                var explain = connection.prepareStatement(
                        "EXPLAIN (ANALYZE, BUFFERS) " + JdbcIndexCommitQueue.POLL_SQL)) {
            explain.setLong(1, 9L);
            explain.setInt(2, 2);
            explain.setLong(3, 9L);
            explain.setInt(4, 2);
            explain.setInt(5, 2);
            try (var rows = explain.executeQuery()) {
                while (rows.next()) {
                    plan.append(rows.getString(1)).append('\n');
                }
            }
        }
        System.out.println("index_commit_poll_1m_explain:\n" + plan);
        assertThat(plan.toString())
                .containsAnyOf("idx_icl_pending_table_sequence", "idx_icl_table_sequence")
                .doesNotContain("Seq Scan on index_commit_log", "WindowAgg");
        assertThat(store.pollUnprocessedCommitEvents(2, 9L))
                .extracting(CommitLogEntry::snapshotId)
                .containsExactlyInAnyOrder("hot-head", "other-head");
    }
}
