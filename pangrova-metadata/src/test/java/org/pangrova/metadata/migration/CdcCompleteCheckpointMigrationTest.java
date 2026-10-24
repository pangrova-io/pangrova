package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcCompleteCheckpointMigrationTest extends ContainerTestBase {
    @Test
    void preservesLegacyEvidenceAndRequiresCompleteCheckpointsForNewIntents() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("cdc_checkpoint_upgrade");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("72").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO cdc_commit_state (id,source_id,partition_num,start_offset,end_offset,
                        state,deadline,created_at,batch_key)
                    VALUES ('old','stream',2,10,19,'COMMITTED',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'old')
                    """);
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT partition_num,start_offset,end_offset,offset_ranges "
                    + "FROM cdc_commit_state WHERE id='old'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt("partition_num")).isEqualTo(2);
                assertThat(rows.getLong("start_offset")).isEqualTo(10);
                assertThat(rows.getLong("end_offset")).isEqualTo(19);
                assertThat(rows.getString("offset_ranges")).isNull();
            }
            String insert = "INSERT INTO cdc_commit_state (id,source_id,state,deadline,created_at,"
                    + "batch_key,offset_ranges) VALUES ('new','stream','PREPARED',CURRENT_TIMESTAMP,"
                    + "CURRENT_TIMESTAMP,'new',%s)";
            assertThatThrownBy(() -> statement.executeUpdate(insert.formatted("NULL")))
                    .hasMessageContaining("cdc_complete_checkpoint");
            assertThatThrownBy(() -> statement.executeUpdate(insert.formatted("'[]'::jsonb")))
                    .hasMessageContaining("cdc_complete_checkpoint");
            assertThat(statement.executeUpdate(insert.formatted("""
                    '[{"topic":"orders","partition":0,"startOffset":20,"endOffset":29},
                      {"topic":"payments","partition":1,"startOffset":40,"endOffset":49}]'::jsonb
                    """))).isEqualTo(1);
        }
    }
}
