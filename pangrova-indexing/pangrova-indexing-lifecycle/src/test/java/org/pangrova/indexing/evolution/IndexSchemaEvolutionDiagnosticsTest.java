package org.pangrova.indexing.evolution;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSchemaEvolutionDiagnosticsTest {
    private static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");

    @Test
    void reportsUnknownImmediatelyAndOnlyOlderIntendedWithoutMutatingState() {
        var old = record(IndexSchemaEvolutionState.INTENDED, NOW.minusSeconds(301));
        var young = record(IndexSchemaEvolutionState.INTENDED, NOW.minusSeconds(1));
        var unknown = record(IndexSchemaEvolutionState.UNKNOWN, NOW);
        var diagnostics = new IndexSchemaEvolutionDiagnostics(store(List.of(old, young, unknown), () -> {}));

        var report = diagnostics.poll(() -> true, NOW).orElseThrow();
        assertThat(report.sample()).extracting(IndexSchemaEvolutionDiagnostics.Operation::operationId)
                .containsExactly(old.operationId(), unknown.operationId());
        assertThat(report.truncated()).isFalse();
    }

    @Test
    void boundsOldestSampleAndRepeatsAtMostEveryFifteenMinutes() {
        var calls = new AtomicInteger();
        var records = java.util.stream.IntStream.rangeClosed(0, IndexSchemaEvolutionDiagnostics.SAMPLE_LIMIT)
                .mapToObj(i -> record(IndexSchemaEvolutionState.UNKNOWN, NOW.minusSeconds(100 - i))).toList();
        var diagnostics = new IndexSchemaEvolutionDiagnostics(store(records, calls::incrementAndGet));
        var report = diagnostics.poll(() -> true, NOW).orElseThrow();
        assertThat(report.sample()).hasSize(IndexSchemaEvolutionDiagnostics.SAMPLE_LIMIT);
        assertThat(report.truncated()).isTrue();
        assertThat(diagnostics.poll(() -> true, NOW.plusSeconds(60))).isEmpty();
        assertThat(calls).hasValue(1);
        assertThat(diagnostics.poll(() -> true, NOW.plus(IndexSchemaEvolutionDiagnostics.REPORT_INTERVAL))).isPresent();
        assertThat(calls).hasValue(2);
    }

    @Test
    void youngIntendedDoesNotThrottleLaterActionableObservation() {
        var intent = record(IndexSchemaEvolutionState.INTENDED, NOW.minusSeconds(299));
        var diagnostics = new IndexSchemaEvolutionDiagnostics(store(List.of(intent), () -> {}));
        assertThat(diagnostics.poll(() -> true, NOW)).isEmpty();
        assertThat(diagnostics.poll(() -> true, NOW.plusSeconds(1))).isPresent();
    }

    @Test
    void demotionBeforeOrDuringQueryDoesNotReportOrThrottleNextLeader() {
        var leader = new AtomicBoolean(false);
        var calls = new AtomicInteger();
        var record = record(IndexSchemaEvolutionState.UNKNOWN, NOW);
        var diagnostics = new IndexSchemaEvolutionDiagnostics(store(List.of(record), () -> {
            if (calls.incrementAndGet() == 1) {
                leader.set(false);
            }
        }));
        assertThat(diagnostics.poll(leader::get, NOW)).isEmpty();
        assertThat(calls).hasValue(0);
        leader.set(true);
        assertThat(diagnostics.poll(leader::get, NOW)).isEmpty();
        assertThat(calls).hasValue(1);
        leader.set(true);
        assertThat(diagnostics.poll(leader::get, NOW)).isPresent();
        assertThat(calls).hasValue(2);
    }

    private static IndexSchemaEvolutionStore store(List<IndexSchemaEvolutionRecord> records, Runnable read) {
        return new IndexSchemaEvolutionStore() {
            @Override
            public List<IndexSchemaEvolutionRecord> listUnresolvedSchemaEvolution(int limit) {
                assertThat(limit).isEqualTo(IndexSchemaEvolutionDiagnostics.SAMPLE_LIMIT + 1);
                read.run();
                return records;
            }
        };
    }

    private static IndexSchemaEvolutionRecord record(IndexSchemaEvolutionState state, Instant created) {
        return new IndexSchemaEvolutionRecord(UUID.randomUUID(), null,
                SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items"), state, created, created);
    }
}
