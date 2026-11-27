package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamingBootstrapDdlIntegrationTest extends ContainerTestBase {
    private static final String TENANT = "tenant";
    private static final String STREAM = "orders";

    @Test
    void createWritesOneCompleteRunningDefinition() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stream_create_definition");
        try (var connection = source.getConnection(); var sql = connection.createStatement()) {
            sql.execute("CREATE TABLE stream_ddl_writes (operation text, state text)");
            sql.execute("""
                    CREATE FUNCTION audit_stream_ddl_write() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        INSERT INTO stream_ddl_writes VALUES (TG_OP, NEW.lifecycle_state);
                        RETURN NEW;
                    END;
                    $$
                    """);
            sql.execute("""
                    CREATE TRIGGER audit_stream_ddl_write
                    AFTER INSERT OR UPDATE ON ingestion_stream_state
                    FOR EACH ROW EXECUTE FUNCTION audit_stream_ddl_write()
                    """);
        }
        var streaming = new StreamingBootstrap(source);

        streaming.getDdlExecutor().create(createStatement(), TENANT, "creator");

        var durable = streaming.stateStore().loadState(STREAM).orElseThrow();
        assertThat(durable.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
        assertThat(durable.sourceProvider()).isEqualTo("kafka");
        assertThat(durable.sourceFormat()).isEqualTo("debezium-json");
        assertThat(durable.targetTable()).isEqualTo("catalog.db.orders");
        assertThat(durable.creatorIdentity()).isEqualTo("creator");
        try (var connection = source.getConnection(); var sql = connection.createStatement();
             var writes = sql.executeQuery("SELECT operation, state FROM stream_ddl_writes")) {
            assertThat(writes.next()).isTrue();
            assertThat(writes.getString("operation")).isEqualTo("INSERT");
            assertThat(writes.getString("state")).isEqualTo("RUNNING");
            assertThat(writes.next()).isFalse();
        }
    }

    @Test
    void anotherCoordinatorCanFenceAndResumeTheSameCheckpoint() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stream_ddl_authority");
        var owner = new StreamingBootstrap(source);
        var ddlCoordinator = new StreamingBootstrap(source);
        var ddl = ddlCoordinator.getDdlExecutor();
        ddl.create(createStatement(), TENANT, "creator");
        var original = owner.stateStore().loadState(STREAM).orElseThrow();
        var partition = new TopicPartitionOffset(STREAM, 0);
        owner.offsetStore().acknowledge(original, Map.of(partition, 2L));

        ddl.alter(new AlterStreamStatement(Identifier.of(STREAM), new AlterStreamAction.Suspend()), TENANT);
        assertThat(owner.stateStore().listByState(StreamLifecycleState.RUNNING)).isEmpty();
        assertThatThrownBy(() -> owner.offsetStore().acknowledge(original, Map.of(partition, 3L)))
                .isInstanceOf(StaleEpochException.class);

        ddl.alter(new AlterStreamStatement(Identifier.of(STREAM), new AlterStreamAction.Resume()), TENANT);
        var resumed = owner.stateStore().listByState(StreamLifecycleState.RUNNING).get(0);
        assertThat(resumed.checkpointId()).isEqualTo(original.checkpointId());
        assertThat(resumed.epoch()).isEqualTo(original.epoch() + 2);
        assertThat(owner.offsetStore().loadOffsets(STREAM)).containsExactlyEntriesOf(Map.of(partition, 2L));
        owner.offsetStore().acknowledge(resumed, Map.of(partition, 3L));
        assertThat(owner.offsetStore().loadOffsets(STREAM)).containsExactlyEntriesOf(Map.of(partition, 3L));
    }

    private static CreateStreamStatement createStatement() {
        return new CreateStreamStatement(Identifier.of(STREAM), false, "kafka",
                Map.of("bootstrap.servers", "unused:9092", "topic", STREAM), "debezium-json",
                Identifier.of("catalog", "db", STREAM), Map.of());
    }
}
