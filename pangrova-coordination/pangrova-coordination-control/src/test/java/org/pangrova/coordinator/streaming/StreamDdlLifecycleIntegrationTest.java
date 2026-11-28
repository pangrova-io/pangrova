package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.DropStreamStatement;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamOffsetStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamDdlLifecycleIntegrationTest extends ContainerTestBase {
    private DataSource dataSource;
    private JdbcStreamStateStore states;
    private StreamState state;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("stream_ddl_lifecycle");
        states = new JdbcStreamStateStore(dataSource);
        state = StreamState.builder().sourceProvider("kafka").streamName("orders-stream").tenantId("tenant")
                .lifecycleState(StreamLifecycleState.RUNNING).targetTable("catalog.db.orders")
                .sourceProperties(Map.of()).sourceFormat("debezium-json")
                .createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build();
        assertThat(states.createState(state)).isTrue();
    }

    @Test
    void invalidSourcesAndFormatsAreRejectedBeforePersistence() {
        var ddl = ddl();
        var invalidFormat = new CreateStreamStatement(Identifier.of("unsupported"), false, "kafka",
                Map.of("bootstrap.servers", "unused:9092", "topic", "orders"), "debezium-avro",
                Identifier.of("orders"), Map.of());
        assertThatThrownBy(() -> ddl.create(invalidFormat, "tenant", "user"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("debezium-avro");
        var unknownSource = new CreateStreamStatement(Identifier.of("unsupported"), false, "audit_feed",
                Map.of("channel", "orders"), "debezium-json", Identifier.of("orders"), Map.of());
        assertThatThrownBy(() -> ddl.create(unknownSource, "tenant", "user"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("audit_feed");
        var invalidKafka = new CreateStreamStatement(Identifier.of("unsupported"), false, "kafka",
                Map.of("topic", "orders"), "debezium-json", Identifier.of("orders"), Map.of());
        assertThatThrownBy(() -> ddl.create(invalidKafka, "tenant", "user"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bootstrap.servers");
        assertThat(states.loadState("unsupported")).isEmpty();
    }

    @Test
    void suspendRejectsOffsetAcknowledgementFromTheSupersededRevision() {
        var offsets = new JdbcStreamOffsetStore(dataSource);
        ddl().alter(new AlterStreamStatement(Identifier.of(state.streamName()),
                new AlterStreamAction.Suspend()), state.tenantId());
        var suspended = states.loadState(state.streamName()).orElseThrow();
        assertThat(suspended.lifecycleState()).isEqualTo(StreamLifecycleState.SUSPENDED);
        assertThat(suspended.epoch()).isEqualTo(state.epoch() + 1);
        assertThatThrownBy(() -> offsets.acknowledge(state, Map.of(new TopicPartitionOffset("orders", 0), 1L)))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void dropCascadesOffsetsAndRejectsAnAcknowledgementAfterRecreation() {
        var offsets = new JdbcStreamOffsetStore(dataSource);
        var partition = new TopicPartitionOffset("orders", 0);
        offsets.acknowledge(state, Map.of(partition, 42L));

        ddl().drop(new DropStreamStatement(Identifier.of(state.streamName()), false), state.tenantId());
        assertThat(states.loadState(state.streamName())).isEmpty();
        assertThat(offsets.loadOffsets(state.streamName())).isEmpty();

        var replacement = state.toBuilder().checkpointId("stream:replacement").build();
        assertThat(states.createState(replacement)).isTrue();
        assertThatThrownBy(() -> offsets.acknowledge(state, Map.of(partition, 44L)))
                .isInstanceOf(StaleEpochException.class);
        offsets.acknowledge(replacement, Map.of(partition, 1L));
        assertThat(offsets.loadOffsets(state.streamName())).containsExactlyEntriesOf(Map.of(partition, 1L));
    }

    @Test
    void configurationChangeFencesOldCallbacksAndPreservesCheckpointIdentity() {
        ddl().alter(new AlterStreamStatement(Identifier.of(state.streamName()),
                new AlterStreamAction.SetProperties(Map.of("batch_size", "5"))), state.tenantId());
        var updated = states.loadState(state.streamName()).orElseThrow();
        assertThat(updated.streamProperties()).containsEntry("batch_size", "5");
        assertThat(updated.epoch()).isEqualTo(state.epoch() + 1);
        assertThat(updated.checkpointId()).isEqualTo(state.checkpointId());
        assertThatThrownBy(() -> states.saveState(state.withError("old failure")))
                .isInstanceOf(StaleEpochException.class);
    }

    private StreamDdlExecutor ddl() {
        var loader = getClass().getClassLoader();
        return new StreamDdlExecutor(new StreamDdlContext(states, CdcRecordConverterRegistry.discover(loader),
                10, CdcSourceRegistry.discover(loader)));
    }
}
