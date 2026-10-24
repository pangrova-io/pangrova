package org.pangrova.metadata.streaming;

import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
class JdbcStreamStateStoreTest extends ContainerTestBase {

    private DataSource dataSource;
    private JdbcStreamStateStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.dataSource();
        initSchema(dataSource);
        store = new JdbcStreamStateStore(dataSource);
    }

    @Test
    void saveAndLoadRoundTrip() {
        StreamState state = newState("stream-1", "tenant-a");
        store.createState(state);

        Optional<StreamState> loaded = store.loadState("stream-1");
        assertThat(loaded).isPresent();
        StreamState got = loaded.get();
        assertThat(got.streamName()).isEqualTo("stream-1");
        assertThat(got.tenantId()).isEqualTo("tenant-a");
        assertThat(got.lifecycleState()).isEqualTo(StreamLifecycleState.CREATING);
        assertThat(got.targetTable()).isEqualTo("catalog.schema.orders");
        assertThat(got.sourceProperties()).containsEntry("bootstrap.servers", "broker:9092");
        assertThat(got.sourceProperties()).containsEntry("topic", "orders-cdc");
        assertThat(got.sourceFormat()).isEqualTo("debezium-json");
        assertThat(got.streamProperties()).containsEntry("batch_size", "1000");
        assertThat(got.creatorIdentity()).isEqualTo("user-1");
        assertThat(got.coordinatorId()).isEqualTo("coord-1");
        assertThat(got.epoch()).isEqualTo(0L);
        assertThat(got.schemaVersion()).isEqualTo(1);
        assertThat(got.recordsConsumed()).isEqualTo(0L);
        assertThat(got.recordsWritten()).isEqualTo(0L);
        assertThat(got.recordsDeadLettered()).isEqualTo(0L);
    }

    @Test
    void loadStateReturnsEmptyForMissing() {
        assertThat(store.loadState("nonexistent")).isEmpty();
    }

    @Test
    void saveStateUpdatesTheSameIncarnation() {
        StreamState original = newState("stream-u", "tenant-a");
        store.createState(original);

        StreamState updated = original.withLifecycleState(StreamLifecycleState.RUNNING);
        store.saveState(updated);

        StreamState got = store.loadState("stream-u").orElseThrow();
        assertThat(got.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
    }

    @Test
    void listByTenantFiltersCorrectly() {
        store.createState(newState("s-a1", "tenant-a"));
        store.createState(newState("s-a2", "tenant-a"));
        store.createState(newState("s-b1", "tenant-b"));

        List<StreamState> tenantA = store.listByTenant("tenant-a");
        assertThat(tenantA).hasSize(2);
        assertThat(tenantA).allMatch(s -> s.tenantId().equals("tenant-a"));

        List<StreamState> tenantB = store.listByTenant("tenant-b");
        assertThat(tenantB).hasSize(1);
        assertThat(tenantB.get(0).streamName()).isEqualTo("s-b1");
    }

    @Test
    void listByStateFiltersCorrectly() {
        store.createState(newState("s-run", "t").withLifecycleState(StreamLifecycleState.RUNNING));
        store.createState(newState("s-sus", "t").withLifecycleState(StreamLifecycleState.SUSPENDED));
        store.createState(newState("s-run2", "t").withLifecycleState(StreamLifecycleState.RUNNING));

        List<StreamState> running = store.listByState(StreamLifecycleState.RUNNING);
        assertThat(running).hasSize(2);
        assertThat(running).allMatch(s -> s.lifecycleState() == StreamLifecycleState.RUNNING);
    }

    @Test
    void listAllReturnsEverything() {
        store.createState(newState("s1", "t1"));
        store.createState(newState("s2", "t2"));
        store.createState(newState("s3", "t1"));

        List<StreamState> all = store.listAll();
        assertThat(all).hasSize(3);
    }

    @Test
    void deleteRemovesEntry() {
        store.createState(newState("s-del", "t"));
        assertThat(store.loadState("s-del")).isPresent();

        boolean deleted = store.delete(store.loadState("s-del").orElseThrow());
        assertThat(deleted).isTrue();
        assertThat(store.loadState("s-del")).isEmpty();
    }

    @Test
    void deleteReturnsFalseForMissing() {
        assertThat(store.delete(newState("nonexistent", "t"))).isFalse();
    }

    @Test
    void saveStateIfEpochSucceedsWithMatchingEpoch() {
        StreamState original = newState("s-cas", "t");
        store.createState(original);

        StreamState updated = original.toBuilder()
                .lifecycleState(StreamLifecycleState.RUNNING)
                .epoch(1)
                .updatedAt(Instant.now())
                .build();
        store.saveStateIfEpoch(updated, 0L);

        StreamState got = store.loadState("s-cas").orElseThrow();
        assertThat(got.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
        assertThat(got.epoch()).isEqualTo(1L);
    }

    @Test
    void saveStateIfEpochThrowsOnStaleEpoch() {
        StreamState original = newState("s-stale", "t");
        store.createState(original);

        StreamState updated = original.toBuilder()
                .lifecycleState(StreamLifecycleState.RUNNING)
                .epoch(1)
                .updatedAt(Instant.now())
                .build();

        assertThatThrownBy(() -> store.saveStateIfEpoch(updated, 999L))
                .isInstanceOf(StaleEpochException.class);

        StreamState got = store.loadState("s-stale").orElseThrow();
        assertThat(got.epoch()).isEqualTo(0L);
        assertThat(got.lifecycleState()).isEqualTo(StreamLifecycleState.CREATING);
    }

    @Test
    void sourcePropertiesWithSpecialCharactersRoundTrip() {
        Map<String, String> props = Map.of(
                "bootstrap.servers", "broker1:9092,broker2:9092",
                "key.with=equals", "value=with=equals",
                "key.with\nnewline", "value\nwith\nnewlines");
        StreamState state = newState("s-special", "t").toBuilder()
                .sourceProperties(props)
                .build();
        store.createState(state);

        StreamState got = store.loadState("s-special").orElseThrow();
        assertThat(got.sourceProperties()).isEqualTo(props);
    }

    @Test
    void emptyStreamPropertiesRoundTrip() {
        StreamState state = newState("s-empty-props", "t").toBuilder()
                .streamProperties(Map.of())
                .build();
        store.createState(state);

        StreamState got = store.loadState("s-empty-props").orElseThrow();
        assertThat(got.streamProperties()).isEmpty();
    }

    @Test
    void nullableFieldsRoundTrip() {
        StreamState state = StreamState.builder()
                .streamName("s-nulls")
                .tenantId("t")
                .lifecycleState(StreamLifecycleState.CREATING)
                .targetTable("catalog.schema.t")
                .sourceProperties(Map.of("bootstrap.servers", "b:9092", "topic", "t"))
                .sourceFormat("debezium-json")
                .streamProperties(Map.of())
                .creatorIdentity(null)
                .coordinatorId(null)
                .epoch(0)
                .schemaVersion(0)
                .errorMessage(null)
                .lastOffsetCommitTime(null)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        store.createState(state);

        StreamState got = store.loadState("s-nulls").orElseThrow();
        assertThat(got.creatorIdentity()).isNull();
        assertThat(got.coordinatorId()).isNull();
        assertThat(got.errorMessage()).isNull();
        assertThat(got.lastOffsetCommitTime()).isNull();
    }

    // ── Helpers ──────────────────────────────────────────────────────

    @Test
    void deletedIncarnationCannotBeResurrectedOrOverwriteItsReplacement() {
        var original = newState("recreated", "tenant-a");
        assertThat(store.createState(original)).isTrue();
        assertThat(store.delete(original)).isTrue();
        assertThatThrownBy(() -> store.saveState(original.withError("late failure")))
                .isInstanceOf(StaleEpochException.class);
        assertThat(store.loadState(original.streamName())).isEmpty();

        var replacement = newState("recreated", "tenant-b");
        assertThat(replacement.checkpointId()).isNotEqualTo(original.checkpointId());
        assertThat(store.createState(replacement)).isTrue();
        assertThat(store.createState(original)).isFalse();
        assertThatThrownBy(() -> store.saveState(original.withError("late failure")))
                .isInstanceOf(StaleEpochException.class);
        assertThatThrownBy(() -> store.saveStateIfEpoch(original, replacement.epoch()))
                .isInstanceOf(StaleEpochException.class);
        assertThat(store.delete(original)).isFalse();
        var loaded = store.loadState(original.streamName()).orElseThrow();
        assertThat(loaded.checkpointId()).isEqualTo(replacement.checkpointId());
        assertThat(loaded.tenantId()).isEqualTo("tenant-b");
        assertThat(loaded.errorMessage()).isNull();
    }

    @Test
    void ordinaryUpdateAndDeleteCannotBypassTenantOrEpochFences() {
        var original = newState("fenced", "tenant-a");
        assertThat(store.createState(original)).isTrue();
        var forged = original.toBuilder().tenantId("tenant-b").build();
        assertThatThrownBy(() -> store.saveStateIfEpoch(forged, original.epoch()))
                .isInstanceOf(StaleEpochException.class);
        assertThat(store.delete(forged)).isFalse();
        var reacquired = original.withReacquisition("another-coordinator");
        store.saveStateIfEpoch(reacquired, original.epoch());
        assertThat(reacquired.checkpointId()).isEqualTo(original.checkpointId());
        assertThatThrownBy(() -> store.saveState(original.withError("old owner")))
                .isInstanceOf(StaleEpochException.class);
        assertThat(store.delete(original)).isFalse();
        assertThat(store.loadState(original.streamName()).orElseThrow().epoch()).isEqualTo(reacquired.epoch());
    }

    private static StreamState newState(String name, String tenantId) {
        Instant now = Instant.now();
        return new StreamState(
                name, tenantId, "stream:" + java.util.UUID.randomUUID(),
                StreamLifecycleState.CREATING,
                "catalog.schema.orders",
                Map.of("bootstrap.servers", "broker:9092", "topic", "orders-cdc"),
                "debezium-json",
                Map.of("batch_size", "1000"),
                "user-1", "coord-1",
                0, 1, null,
                0L, 0L, 0L, null,
                now, now);
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
        }
    }
}
