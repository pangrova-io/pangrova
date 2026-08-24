package org.pangrova.metadata.property.streaming;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.streaming.JdbcStreamStateStore;
import org.pangrova.metadata.streaming.StreamLifecycleState;
import org.pangrova.metadata.streaming.StreamState;
import org.junit.jupiter.api.Tag;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("integration")
class StreamStateStoreRoundTripPropertyTest {

    private JdbcStreamStateStore store;

    @BeforeProperty
    void setUp() throws Exception {
        assumeTrue(SharedPostgresContainer.isAvailable());
        DataSource dataSource = SharedPostgresContainer.dataSource();
        initSchema(dataSource);
        store = new JdbcStreamStateStore(dataSource);
    }

    @Property(tries = 10)
    void saveAndLoadPreservesAllFields(@ForAll("streamStates") StreamState original) {
                store.delete(original.streamName());
        store.saveState(original);

        Optional<StreamState> loaded = store.loadState(original.streamName());
        assertThat(loaded).isPresent();

        StreamState got = loaded.get();
        assertThat(got.streamName()).isEqualTo(original.streamName());
        assertThat(got.tenantId()).isEqualTo(original.tenantId());
        assertThat(got.lifecycleState()).isEqualTo(original.lifecycleState());
        assertThat(got.targetTable()).isEqualTo(original.targetTable());
        assertThat(got.sourceProperties()).isEqualTo(original.sourceProperties());
        assertThat(got.sourceFormat()).isEqualTo(original.sourceFormat());
        assertThat(got.streamProperties()).isEqualTo(original.streamProperties());
        assertThat(got.creatorIdentity()).isEqualTo(original.creatorIdentity());
        assertThat(got.coordinatorId()).isEqualTo(original.coordinatorId());
        assertThat(got.epoch()).isEqualTo(original.epoch());
        assertThat(got.schemaVersion()).isEqualTo(original.schemaVersion());
        assertThat(got.errorMessage()).isEqualTo(original.errorMessage());
        assertThat(got.recordsConsumed()).isEqualTo(original.recordsConsumed());
        assertThat(got.recordsWritten()).isEqualTo(original.recordsWritten());
        assertThat(got.recordsDeadLettered()).isEqualTo(original.recordsDeadLettered());

        assertThat(got.createdAt().truncatedTo(ChronoUnit.MILLIS))
                .isEqualTo(original.createdAt().truncatedTo(ChronoUnit.MILLIS));
        assertThat(got.updatedAt().truncatedTo(ChronoUnit.MILLIS))
                .isEqualTo(original.updatedAt().truncatedTo(ChronoUnit.MILLIS));

        if (original.lastOffsetCommitTime() != null) {
            assertThat(got.lastOffsetCommitTime()).isNotNull();
            assertThat(got.lastOffsetCommitTime().truncatedTo(ChronoUnit.MILLIS))
                    .isEqualTo(original.lastOffsetCommitTime().truncatedTo(ChronoUnit.MILLIS));
        } else {
            assertThat(got.lastOffsetCommitTime()).isNull();
        }
    }

    @Property(tries = 10)
    void upsertOverwritePreservesLatestState(
            @ForAll("streamStates") StreamState first,
            @ForAll("streamStates") StreamState second) {
        StreamState s1 = first;
        StreamState s2 = second.toBuilder().streamName(first.streamName()).build();

        store.saveState(s1);
        store.saveState(s2);

        Optional<StreamState> loaded = store.loadState(s1.streamName());
        assertThat(loaded).isPresent();

        StreamState got = loaded.get();
        assertThat(got.tenantId()).isEqualTo(s2.tenantId());
        assertThat(got.lifecycleState()).isEqualTo(s2.lifecycleState());
        assertThat(got.targetTable()).isEqualTo(s2.targetTable());
        assertThat(got.sourceProperties()).isEqualTo(s2.sourceProperties());
        assertThat(got.sourceFormat()).isEqualTo(s2.sourceFormat());
        assertThat(got.epoch()).isEqualTo(s2.epoch());
    }

    // ── Generators ──────────────────────────────────────────────────

    @Provide
    Arbitrary<StreamState> streamStates() {
        Arbitrary<String> streamNames = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "stream-" + s);
        Arbitrary<String> tenantIds = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(10)
                .map(s -> "tenant-" + s);
        Arbitrary<StreamLifecycleState> lifecycleStates =
                Arbitraries.of(StreamLifecycleState.values());
        Arbitrary<String> targetTables = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(15)
                .map(s -> "catalog.schema." + s);
        Arbitrary<Map<String, String>> sourceProps = safeMapArbitrary();
        Arbitrary<String> sourceFormats = Arbitraries.of(
                "debezium-json", "canal-json", "maxwell-json");
        Arbitrary<Map<String, String>> streamProps = safeMapArbitrary();
        Arbitrary<String> nullableStrings = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(20)
                .injectNull(0.3);
        Arbitrary<Long> epochs = Arbitraries.longs().between(0, 10_000);
        Arbitrary<Integer> schemaVersions = Arbitraries.integers().between(0, 100);
        Arbitrary<Long> counters = Arbitraries.longs().between(0, 1_000_000);
        Arbitrary<Instant> timestamps = Arbitraries.longs()
                .between(1_000_000_000L, 2_000_000_000L)
                .map(Instant::ofEpochSecond);
        Arbitrary<Instant> nullableTimestamps = timestamps.injectNull(0.3);

        Arbitrary<IdentityGroup> identityGroup = Combinators.combine(
                streamNames, tenantIds, lifecycleStates, targetTables,
                sourceProps, sourceFormats, streamProps
        ).as(IdentityGroup::new);

        Arbitrary<MetadataGroup> metadataGroup = Combinators.combine(
                nullableStrings, nullableStrings, epochs,
                schemaVersions, nullableStrings, counters
        ).as(MetadataGroup::new);

        Arbitrary<CounterGroup> counterGroup = Combinators.combine(
                counters, counters, nullableTimestamps,
                timestamps, timestamps
        ).as(CounterGroup::new);

        return Combinators.combine(identityGroup, metadataGroup, counterGroup)
                .as((id, meta, ctr) -> new StreamState(
                        id.streamName, id.tenantId, id.lifecycleState, id.targetTable,
                        id.sourceProperties, id.sourceFormat, id.streamProperties,
                        meta.creatorIdentity, meta.coordinatorId,
                        meta.epoch, meta.schemaVersion, meta.errorMessage,
                        meta.recordsConsumed, ctr.recordsWritten, ctr.recordsDeadLettered,
                        ctr.lastOffsetCommitTime, ctr.createdAt, ctr.updatedAt));
    }

    private record IdentityGroup(
            String streamName, String tenantId, StreamLifecycleState lifecycleState,
            String targetTable, Map<String, String> sourceProperties,
            String sourceFormat, Map<String, String> streamProperties) {}

    private record MetadataGroup(
            String creatorIdentity, String coordinatorId, long epoch,
            int schemaVersion, String errorMessage, long recordsConsumed) {}

    private record CounterGroup(
            long recordsWritten, long recordsDeadLettered,
            Instant lastOffsetCommitTime, Instant createdAt, Instant updatedAt) {}

    private static Arbitrary<Map<String, String>> safeMapArbitrary() {
        Arbitrary<String> keys = Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('0', '9')
                .withChars('.', '-')
                .ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> values = Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .withChars('.', '-', ':', ',')
                .ofMinLength(1).ofMaxLength(30);
        return Arbitraries.maps(keys, values)
                .ofMinSize(1).ofMaxSize(5);
    }

    // ── Schema ──────────────────────────────────────────────────────

    private static void initSchema(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ingestion_stream_state");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_stream_state (
                        stream_name       VARCHAR(255) NOT NULL PRIMARY KEY,
                        tenant_id         VARCHAR(255) NOT NULL,
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
