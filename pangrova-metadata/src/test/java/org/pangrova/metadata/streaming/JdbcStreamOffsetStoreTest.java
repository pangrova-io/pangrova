package org.pangrova.metadata.streaming;

import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcStreamOffsetStoreTest extends ContainerTestBase {

    private DataSource dataSource;
    private JdbcStreamOffsetStore offsetStore;
    private JdbcStreamStateStore stateStore;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.dataSource();
        initSchema(dataSource);
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

    // ── Helpers ──────────────────────────────────────────────────────

    private void insertParentStream(String streamName) {
        Instant now = Instant.now();
        StreamState state = new StreamState(
                streamName, "tenant-test", "checkpoint-" + streamName,
                StreamLifecycleState.RUNNING,
                "catalog.schema.t",
                Map.of("bootstrap.servers", "b:9092", "topic", "t"),
                "debezium-json",
                Map.of(), "user", "coord",
                0, 0, null,
                0L, 0L, 0L, null,
                now, now);
        stateStore.createState(state);
    }

    private static void initSchema(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ingestion_stream_offsets");
            stmt.execute("DROP TABLE IF EXISTS ingestion_stream_state");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_stream_state (
                        stream_name       VARCHAR(255) NOT NULL PRIMARY KEY,
                        tenant_id         VARCHAR(255) NOT NULL,
                        checkpoint_id     VARCHAR(255) NOT NULL UNIQUE,
                        lifecycle_state   VARCHAR(32)  NOT NULL,
                        target_table      VARCHAR(512) NOT NULL,
                        source_properties TEXT         NOT NULL,
                        source_format     VARCHAR(64)  NOT NULL,
                        stream_properties TEXT,
                        creator_identity  VARCHAR(255),
                        coordinator_id    VARCHAR(255),
                        epoch             BIGINT       NOT NULL DEFAULT 0,
                        schema_version    INT          NOT NULL DEFAULT 0,
                        error_message     TEXT,
                        records_consumed  BIGINT       NOT NULL DEFAULT 0,
                        records_written   BIGINT       NOT NULL DEFAULT 0,
                        records_dead_lettered BIGINT   NOT NULL DEFAULT 0,
                        last_offset_commit_time TIMESTAMP,
                        created_at        TIMESTAMP    NOT NULL,
                        updated_at        TIMESTAMP    NOT NULL
                    )
                    """);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_stream_offsets (
                        stream_name      VARCHAR(255) NOT NULL,
                        topic            VARCHAR(512) NOT NULL,
                        partition_id     INT          NOT NULL,
                        committed_offset BIGINT       NOT NULL,
                        updated_at       TIMESTAMP    NOT NULL,
                        PRIMARY KEY (stream_name, topic, partition_id),
                        FOREIGN KEY (stream_name) REFERENCES ingestion_stream_state(stream_name)
                            ON DELETE CASCADE
                    )
                    """);
        }
    }
}
