package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.streaming.JdbcStreamStateStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;

class StreamCheckpointIdentityMigrationTest extends ContainerTestBase {
    @Test
    void upgradePreservesExistingCheckpointAssociationAndDeletionCascadesCachedOffsets() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("stream_identity_upgrade");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("73").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO ingestion_stream_state
                        (stream_name,tenant_id,lifecycle_state,target_table,source_properties,source_format,
                         created_at,updated_at)
                    VALUES ('legacy','tenant','RUNNING','catalog.db.target','topic=orders','debezium-json',
                            CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """);
            statement.executeUpdate("""
                    INSERT INTO ingestion_stream_offsets VALUES ('legacy','orders',0,11,CURRENT_TIMESTAMP)
                    """);
            statement.executeUpdate("""
                    INSERT INTO cdc_commit_state (id,source_id,state,deadline,created_at,batch_key,offset_ranges)
                    VALUES ('old','legacy','COMMITTED',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'old',
                            '[{"topic":"orders","partition":0,"startOffset":0,"endOffset":10}]'::jsonb)
                    """);
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        var store = new JdbcStreamStateStore(dataSource);
        var state = store.loadState("legacy").orElseThrow();
        assertThat(state.checkpointId()).isEqualTo("legacy");
        assertThat(store.delete(state)).isTrue();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT count(*) FROM ingestion_stream_offsets")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isZero();
            }
            // Durable publication evidence survives deletion of the stream itself.
            try (var rows = statement.executeQuery("SELECT source_id FROM cdc_commit_state WHERE id='old'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo(state.checkpointId());
            }
        }
    }
}
