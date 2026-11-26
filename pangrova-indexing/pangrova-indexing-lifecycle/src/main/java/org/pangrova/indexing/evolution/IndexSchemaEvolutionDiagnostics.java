package org.pangrova.indexing.evolution;

import org.pangrova.indexing.api.enums.SchemaChangeType;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Bounded read-only maintenance diagnostics; never an operation outcome authority. */
final class IndexSchemaEvolutionDiagnostics {
    static final int SAMPLE_LIMIT = 25;
    static final Duration INTENDED_REPORT_AGE = Duration.ofMinutes(5);
    static final Duration REPORT_INTERVAL = Duration.ofMinutes(15);

    private final IndexSchemaEvolutionStore store;
    private final AtomicReference<Instant> nextReportAt = new AtomicReference<>(Instant.MIN);

    IndexSchemaEvolutionDiagnostics(IndexSchemaEvolutionStore store) {
        this.store = java.util.Objects.requireNonNull(store, "store");
    }

    /**
     * Polls unresolved state using a bounded oldest-first sample. Age is solely
     * an alert threshold for INTENDED records; it never establishes an outcome.
     */
    Optional<Report> poll(BooleanSupplier stillLeader, Instant now) {
        Instant next = nextReportAt.get();
        if (!stillLeader.getAsBoolean() || now.isBefore(next)) {
            return Optional.empty();
        }
        List<IndexSchemaEvolutionRecord> unresolved = store.listUnresolvedSchemaEvolution(SAMPLE_LIMIT + 1);
        if (!stillLeader.getAsBoolean()) {
            return Optional.empty();
        }
        Instant intendedCutoff = now.minus(INTENDED_REPORT_AGE);
        boolean truncated = unresolved.size() > SAMPLE_LIMIT;
        List<Operation> sample = unresolved.stream().limit(SAMPLE_LIMIT)
                .filter(record -> reportable(record, intendedCutoff))
                .map(record -> new Operation(record.operationId(), record.event().type(), record.state(),
                        Math.max(0, Duration.between(record.createdAt(), now).toMinutes())))
                .toList();
        if (sample.isEmpty() && !truncated) {
            return Optional.empty();
        }
        if (!stillLeader.getAsBoolean() || !nextReportAt.compareAndSet(next, now.plus(REPORT_INTERVAL))) {
            return Optional.empty();
        }
        return Optional.of(new Report(sample, truncated));
    }

    private static boolean reportable(IndexSchemaEvolutionRecord record, Instant intendedCutoff) {
        return record.state() == IndexSchemaEvolutionState.UNKNOWN
                || (record.state() == IndexSchemaEvolutionState.INTENDED
                    && !record.createdAt().isAfter(intendedCutoff));
    }

    record Operation(UUID operationId, SchemaChangeType type, IndexSchemaEvolutionState state, long ageMinutes) {}

    record Report(List<Operation> sample, boolean truncated) {
        Report {
            sample = List.copyOf(sample);
        }
    }
}
