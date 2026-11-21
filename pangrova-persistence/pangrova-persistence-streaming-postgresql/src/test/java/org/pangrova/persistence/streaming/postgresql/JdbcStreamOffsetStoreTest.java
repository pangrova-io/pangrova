package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import org.pangrova.streaming.api.exception.StreamingStoreException;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.stream.Stream;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Tag("integration")
class JdbcStreamOffsetStoreTest extends ContainerTestBase {

    private DataSource dataSource;
    private JdbcStreamOffsetStore offsetStore;
    private JdbcStreamStateStore stateStore;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("stream_offset");
        stateStore = new JdbcStreamStateStore(dataSource);
        offsetStore = new JdbcStreamOffsetStore(dataSource);
    }

    @Test
    void saveAndLoadOffsetsRoundTrip() {
        insertParentStream("stream-1");

        TopicPartitionOffset tp0 = new TopicPartitionOffset("orders", 0);
        TopicPartitionOffset tp1 = new TopicPartitionOffset("orders", 1);
        Map<TopicPartitionOffset, Long> offsets = Map.of(tp0, 100L, tp1, 200L);

        offsetStore.saveOffsets("stream-1", offsets);

        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("stream-1");
        assertThat(loaded).hasSize(2);
        assertThat(loaded.get(tp0)).isEqualTo(100L);
        assertThat(loaded.get(tp1)).isEqualTo(200L);
    }

    @Test
    void loadOffsetsReturnsEmptyForMissing() {
        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("nonexistent");
        assertThat(loaded).isEmpty();
    }

    @Test
    void saveOffsetsUpsertOverwrites() {
        insertParentStream("stream-u");

        TopicPartitionOffset tp = new TopicPartitionOffset("orders", 0);
        offsetStore.saveOffsets("stream-u", Map.of(tp, 50L));
        offsetStore.saveOffsets("stream-u", Map.of(tp, 150L));

        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("stream-u");
        assertThat(loaded.get(tp)).isEqualTo(150L);
    }

    @Test
    void journalAcknowledgementCannotRewindAnOffsetOrReachANewerRevision() {
        StreamState before = insertParentStream("stream-fenced");
        var partition = new TopicPartitionOffset("orders", 0);
        offsetStore.acknowledge(before, Map.of(partition, 150L));
        offsetStore.acknowledge(before, Map.of(partition, 50L));
        assertThat(offsetStore.loadOffsets(before.streamName())).containsEntry(partition, 150L);

        StreamState replacement = before.withReacquisition("new-owner");
        stateStore.saveStateIfEpoch(replacement, before.epoch());
        assertThatThrownBy(() -> offsetStore.acknowledge(before, Map.of(partition, 200L)))
                .isInstanceOf(StaleEpochException.class);
        assertThat(offsetStore.loadOffsets(before.streamName())).containsEntry(partition, 150L);

        offsetStore.acknowledge(replacement, Map.of(partition, 200L));
        assertThat(offsetStore.loadOffsets(before.streamName())).containsEntry(partition, 200L);
    }

    @Test
    void saveOffsetsIsAtomicBatch() {
        insertParentStream("stream-batch");

        TopicPartitionOffset tp0 = new TopicPartitionOffset("topic-a", 0);
        TopicPartitionOffset tp1 = new TopicPartitionOffset("topic-a", 1);
        TopicPartitionOffset tp2 = new TopicPartitionOffset("topic-a", 2);

        offsetStore.saveOffsets("stream-batch",
                Map.of(tp0, 10L, tp1, 20L, tp2, 30L));

        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("stream-batch");
        assertThat(loaded).hasSize(3);
        assertThat(loaded.get(tp0)).isEqualTo(10L);
        assertThat(loaded.get(tp1)).isEqualTo(20L);
        assertThat(loaded.get(tp2)).isEqualTo(30L);
    }

    @Test
    void deleteOffsetsRemovesAll() {
        insertParentStream("stream-del");

        TopicPartitionOffset tp0 = new TopicPartitionOffset("orders", 0);
        TopicPartitionOffset tp1 = new TopicPartitionOffset("orders", 1);
        offsetStore.saveOffsets("stream-del", Map.of(tp0, 100L, tp1, 200L));

        offsetStore.deleteOffsets("stream-del");

        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("stream-del");
        assertThat(loaded).isEmpty();
    }

    @Test
    void saveEmptyOffsetsIsNoOp() {
        insertParentStream("stream-empty");
        offsetStore.saveOffsets("stream-empty", Map.of());

        Map<TopicPartitionOffset, Long> loaded = offsetStore.loadOffsets("stream-empty");
        assertThat(loaded).isEmpty();
    }

    @Test
    void multipleStreamsIsolated() {
        insertParentStream("stream-x");
        insertParentStream("stream-y");

        TopicPartitionOffset tp = new TopicPartitionOffset("shared-topic", 0);
        offsetStore.saveOffsets("stream-x", Map.of(tp, 100L));
        offsetStore.saveOffsets("stream-y", Map.of(tp, 999L));

        assertThat(offsetStore.loadOffsets("stream-x").get(tp)).isEqualTo(100L);
        assertThat(offsetStore.loadOffsets("stream-y").get(tp)).isEqualTo(999L);
    }

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedBatchDoesNotCommitOffsets(Throwable failure, SQLException rollbackFailure) {
        insertParentStream("stream-failure");
        var source = JdbcMutationFault.afterMutation(dataSource,
                "INSERT INTO ingestion_stream_offsets ", failure, rollbackFailure);
        var failingStore = new JdbcStreamOffsetStore(source);
        Throwable caught = catchThrowable(() -> failingStore.saveOffsets("stream-failure",
                Map.of(new TopicPartitionOffset("orders", 0), 10L,
                        new TopicPartitionOffset("orders", 1), 20L)));

        assertThat(offsetStore.loadOffsets("stream-failure")).isEmpty();
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(StreamingStoreException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after batch"), new IllegalStateException("after batch"),
                new AssertionError("after batch")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private StreamState insertParentStream(String streamName) {
        Instant now = Instant.now();
        StreamState state = new StreamState(
                streamName, "tenant-test", "checkpoint-" + streamName,
                StreamLifecycleState.RUNNING,
                "catalog.schema.t", "kafka",
                Map.of("bootstrap.servers", "b:9092", "topic", "t"),
                "debezium-json",
                Map.of(), "user", "coord",
                0, 0, null,
                0L, 0L, 0L, null,
                now, now);
        stateStore.createState(state);
        return state;
    }
}
