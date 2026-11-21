package org.pangrova.persistence.streaming.postgresql.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.junit.jupiter.api.Tag;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
class StreamEpochFencingPropertyTest extends ContainerTestBase {

    private JdbcStreamStateStore store;

    @BeforeProperty
    void setUp() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("stream_epoch");
        store = new JdbcStreamStateStore(dataSource);
    }

    @Property(tries = 10)
    void saveStateIfEpochSucceedsWhenEpochMatches(
            @ForAll("streamStates") StreamState original,
            @ForAll("lifecycleStates") StreamLifecycleState newLifecycle) {

        store.loadState(original.streamName()).ifPresent(store::delete);
        assertThat(store.createState(original)).isTrue();

        StreamState updated = original.toBuilder()
                .lifecycleState(newLifecycle)
                .epoch(original.epoch() + 1)
                .updatedAt(Instant.now())
                .build();

        store.saveStateIfEpoch(updated, original.epoch());

        Optional<StreamState> loaded = store.loadState(original.streamName());
        assertThat(loaded).isPresent();
        assertThat(loaded.get().lifecycleState()).isEqualTo(newLifecycle);
        assertThat(loaded.get().epoch()).isEqualTo(original.epoch() + 1);
    }

    @Property(tries = 10)
    void saveStateIfEpochThrowsOnStalEpoch(
            @ForAll("streamStates") StreamState original,
            @ForAll("staleEpochOffsets") long staleOffset) {

        store.loadState(original.streamName()).ifPresent(store::delete);
        assertThat(store.createState(original)).isTrue();

        long staleEpoch = original.epoch() + staleOffset;

        StreamState updated = original.toBuilder()
                .lifecycleState(StreamLifecycleState.SUSPENDED)
                .epoch(staleEpoch)
                .updatedAt(Instant.now())
                .build();

        assertThatThrownBy(() -> store.saveStateIfEpoch(updated, staleEpoch))
                .isInstanceOf(StaleEpochException.class);
    }

    @Property(tries = 10)
    void staleEpochRejectionPreservesOriginalState(
            @ForAll("streamStates") StreamState original,
            @ForAll("staleEpochOffsets") long staleOffset) {

        store.loadState(original.streamName()).ifPresent(store::delete);
        assertThat(store.createState(original)).isTrue();

        long staleEpoch = original.epoch() + staleOffset;

        StreamState updated = original.toBuilder()
                .lifecycleState(StreamLifecycleState.DROPPING)
                .errorMessage("should-not-persist")
                .epoch(999_999L)
                .updatedAt(Instant.now())
                .build();

        try {
            store.saveStateIfEpoch(updated, staleEpoch);
        } catch (StaleEpochException ignored) {
            // expected
        }

        Optional<StreamState> loaded = store.loadState(original.streamName());
        assertThat(loaded).isPresent();

        StreamState got = loaded.get();
        assertThat(got.epoch()).isEqualTo(original.epoch());
        assertThat(got.lifecycleState()).isEqualTo(original.lifecycleState());
        assertThat(got.errorMessage()).isEqualTo(original.errorMessage());
        assertThat(got.tenantId()).isEqualTo(original.tenantId());
        assertThat(got.targetTable()).isEqualTo(original.targetTable());
        assertThat(got.sourceProperties()).isEqualTo(original.sourceProperties());
        assertThat(got.recordsConsumed()).isEqualTo(original.recordsConsumed());
        assertThat(got.recordsWritten()).isEqualTo(original.recordsWritten());
    }

    // ── Generators ──────────────────────────────────────────────────

    @Provide
    Arbitrary<StreamLifecycleState> lifecycleStates() {
        return Arbitraries.of(StreamLifecycleState.values());
    }

    @Provide
    Arbitrary<Long> staleEpochOffsets() {
        return Arbitraries.longs().between(-1000, 1000)
                .filter(offset -> offset != 0);
    }

    @Provide
    Arbitrary<StreamState> streamStates() {
        Arbitrary<String> streamNames = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "stream-" + s);
        Arbitrary<String> tenantIds = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(10)
                .map(s -> "tenant-" + s);
        Arbitrary<StreamLifecycleState> states =
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
                streamNames, tenantIds, states, targetTables,
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
                        id.streamName, id.tenantId, "checkpoint-" + id.streamName, id.lifecycleState, id.targetTable,
                        "kafka", id.sourceProperties, id.sourceFormat, id.streamProperties,
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

}
