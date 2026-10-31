package org.pangrova.materialization.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.InMemoryMaterializationRepository;
import org.pangrova.materialization.MaterializationTestFixtures;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PhysicalGenerationStore;

class GenerationGarbageCollectorTest {

    private static final Instant NOW = MaterializationTestFixtures.NOW;
    private static final Duration RETENTION = Duration.ofHours(1);
    private static final SourceVector VECTOR = MaterializationTestFixtures.vector(
            MaterializationTestFixtures.source("orders"));

    private static final GenerationId HEAD_ID = generationId("d1");
    private static final GenerationId RETIRED_ID = generationId("d2");
    private static final GenerationId CANDIDATE_ID = generationId("d3");
    private static final GenerationId QUARANTINED_ID = generationId("d4");
    private static final GenerationId BASE_ID = generationId("d5");
    private static final GenerationId DEPENDENT_ID = generationId("d6");

    private static final AttemptId HEAD_ATTEMPT = attemptId("e1");
    private static final AttemptId RETIRED_ATTEMPT = attemptId("e2");
    private static final AttemptId ABANDONED_ATTEMPT = attemptId("e3");
    private static final AttemptId QUARANTINED_ATTEMPT = attemptId("e4");
    private static final AttemptId BUILDING_ATTEMPT = attemptId("e5");

    private InMemoryMaterializationRepository repository;
    private RecordingStore store;
    private GenerationGarbageCollector collector;
    private MaterializationObject object;

    @BeforeEach
    void setUp() {
        repository = new InMemoryMaterializationRepository(new FixedClock(NOW));
        store = new RecordingStore();
        collector = new GenerationGarbageCollector(repository, store, new FixedClock(NOW));
        object = MaterializationTestFixtures.creating(
                MaterializationKind.DYNAMIC_TABLE, RefreshMode.FULL);
        repository.putObject(object);
    }

    @Test
    void collectsRetiredGenerationAfterRetention() {
        Generation head = published(HEAD_ID, HEAD_ATTEMPT, candidateTable(1), NOW);
        Generation retired = published(RETIRED_ID, RETIRED_ATTEMPT, candidateTable(0),
                NOW.minus(Duration.ofHours(2))).retire();
        repository.putObject(object.publish(HEAD_ID, 0, NOW));
        repository.putGeneration(head);
        repository.putGeneration(retired);

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.examined()).isEqualTo(1);
        assertThat(report.collected()).isEqualTo(1);
        assertThat(report.failed()).isZero();
        assertThat(store.dropped).containsExactly(candidateTable(0));
        assertThat(repository.findGeneration(RETIRED_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Collected.INSTANCE);
        assertThat(repository.findGeneration(HEAD_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Published.INSTANCE);
        assertThat(repository.gcLedger()).hasSize(1);
        assertThat(repository.gcLedger().get(0).state())
                .isEqualTo(GcLedgerEntry.GcState.APPLIED);
        assertThat(repository.events())
                .extracting(event -> event.eventType())
                .contains(MaterializationEventType.GC_COLLECTED);
    }

    @Test
    void skipsGenerationHeldByALiveReaderPin() {
        Generation head = published(HEAD_ID, HEAD_ATTEMPT, candidateTable(1), NOW);
        Generation retired = published(RETIRED_ID, RETIRED_ATTEMPT, candidateTable(0),
                NOW.minus(Duration.ofHours(2))).retire();
        repository.putObject(object.publish(HEAD_ID, 0, NOW));
        repository.putGeneration(head);
        repository.putGeneration(retired);
        repository.pinReader(new ReaderPin(RETIRED_ID, QueryId.random(), "query-owner",
                NOW.plus(Duration.ofMinutes(10)), NOW));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.examined()).isZero();
        assertThat(store.dropped).isEmpty();
        assertThat(repository.gcLedger()).isEmpty();
    }

    @Test
    void collectionRechecksReaderPinsInsertedAfterSelection() {
        Generation head = published(HEAD_ID, HEAD_ATTEMPT, candidateTable(1), NOW);
        Generation retired = published(RETIRED_ID, RETIRED_ATTEMPT, candidateTable(0),
                NOW.minus(Duration.ofHours(2))).retire();
        repository.putObject(object.publish(HEAD_ID, 0, NOW));
        repository.putGeneration(head);
        repository.putGeneration(retired);
        GcLedgerEntry entry = GcLedgerEntry.pending(RETIRED_ID, "state=RETIRED", NOW);
        repository.recordGcDecision(entry);
        // A reader pins the generation after the collector selected it and
        // before the row lock is taken.
        repository.pinReader(new ReaderPin(RETIRED_ID, QueryId.random(), "query-owner",
                NOW.plus(Duration.ofMinutes(10)), NOW));

        boolean collected = repository.collectGeneration(retired, entry,
                new MaterializationEvent("gc-race", MaterializationEventType.GC_COLLECTED,
                        object.id(), Optional.empty(), Optional.of(RETIRED_ATTEMPT),
                        Optional.of(RETIRED_ID), Optional.empty(),
                        MaterializationTestFixtures.REQUESTER,
                        MaterializationTestFixtures.REQUESTER, "collected", NOW),
                () -> store.drop(object.id(), candidateTable(0)));

        assertThat(collected).isFalse();
        assertThat(store.dropped).isEmpty();
        assertThat(repository.findGeneration(RETIRED_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Retired.INSTANCE);
        assertThat(repository.gcLedger()).singleElement().satisfies(ledger ->
                assertThat(ledger.state()).isEqualTo(GcLedgerEntry.GcState.PENDING));
    }

    @Test
    void collectsGenerationWhoseReaderPinExpired() {
        Generation head = published(HEAD_ID, HEAD_ATTEMPT, candidateTable(1), NOW);
        Generation retired = published(RETIRED_ID, RETIRED_ATTEMPT, candidateTable(0),
                NOW.minus(Duration.ofHours(2))).retire();
        repository.putObject(object.publish(HEAD_ID, 0, NOW));
        repository.putGeneration(head);
        repository.putGeneration(retired);
        repository.pinReader(new ReaderPin(RETIRED_ID, QueryId.random(), "query-owner",
                NOW.minus(Duration.ofMinutes(1)), NOW.minus(Duration.ofMinutes(2))));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.collected()).isEqualTo(1);
        assertThat(store.dropped).containsExactly(candidateTable(0));
        assertThat(repository.findReaderPins(RETIRED_ID)).isEmpty();
    }

    @Test
    void collectsCandidateWhoseAttemptIsTerminal() {
        PhysicalTableRef table = candidateTable(2);
        repository.putGeneration(Generation.candidate(CANDIDATE_ID, object.id(),
                MaterializationTestFixtures.INCARNATION, 1, ABANDONED_ATTEMPT, Optional.empty(),
                table, RefreshKind.Full.INSTANCE, VECTOR, NOW.minus(Duration.ofHours(2))));
        repository.putAttempt(attempt(ABANDONED_ATTEMPT, table)
                .markBuilding(NOW.minus(Duration.ofHours(2)))
                .abandon("owner lost", NOW.minus(Duration.ofHours(2))));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.collected()).isEqualTo(1);
        assertThat(store.dropped).containsExactly(table);
        assertThat(repository.findGeneration(CANDIDATE_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Collected.INSTANCE);
    }

    @Test
    void neverCollectsQuarantinedGeneration() {
        PhysicalTableRef table = candidateTable(3);
        repository.putGeneration(Generation.candidate(QUARANTINED_ID, object.id(),
                        MaterializationTestFixtures.INCARNATION, 1, QUARANTINED_ATTEMPT,
                        Optional.empty(), table, RefreshKind.Full.INSTANCE, VECTOR,
                        NOW.minus(Duration.ofHours(3)))
                .quarantine());
        repository.putAttempt(attempt(QUARANTINED_ATTEMPT, table)
                .markBuilding(NOW.minus(Duration.ofHours(3)))
                .quarantine("ambiguous commit", NOW.minus(Duration.ofHours(3))));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.examined()).isZero();
        assertThat(store.dropped).isEmpty();
        assertThat(repository.findGeneration(QUARANTINED_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Quarantined.INSTANCE);
    }

    @Test
    void retainsGenerationAndMarksLedgerFailedWhenDropFails() {
        Generation head = published(HEAD_ID, HEAD_ATTEMPT, candidateTable(1), NOW);
        Generation retired = published(RETIRED_ID, RETIRED_ATTEMPT, candidateTable(0),
                NOW.minus(Duration.ofHours(2))).retire();
        repository.putObject(object.publish(HEAD_ID, 0, NOW));
        repository.putGeneration(head);
        repository.putGeneration(retired);
        store.failures.add(candidateTable(0));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.examined()).isEqualTo(1);
        assertThat(report.collected()).isZero();
        assertThat(report.failed()).isEqualTo(1);
        assertThat(repository.findGeneration(RETIRED_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Retired.INSTANCE);
        assertThat(repository.gcLedger()).hasSize(1);
        assertThat(repository.gcLedger().get(0).state())
                .isEqualTo(GcLedgerEntry.GcState.FAILED);
    }

    @Test
    void retainsBaseGenerationReferencedByALiveGeneration() {
        PhysicalTableRef baseTable = candidateTable(4);
        PhysicalTableRef dependentTable = candidateTable(5);
        repository.putGeneration(Generation.candidate(BASE_ID, object.id(),
                MaterializationTestFixtures.INCARNATION, 1, ABANDONED_ATTEMPT, Optional.empty(),
                baseTable, RefreshKind.Full.INSTANCE, VECTOR, NOW.minus(Duration.ofHours(3))));
        repository.putAttempt(attempt(ABANDONED_ATTEMPT, baseTable)
                .markBuilding(NOW.minus(Duration.ofHours(3)))
                .abandon("owner lost", NOW.minus(Duration.ofHours(3))));
        repository.putGeneration(Generation.candidate(DEPENDENT_ID, object.id(),
                MaterializationTestFixtures.INCARNATION, 2, BUILDING_ATTEMPT,
                Optional.of(BASE_ID), dependentTable, RefreshKind.IncrementalAppendOnly.INSTANCE,
                VECTOR, NOW.minus(Duration.ofHours(2))));
        repository.putAttempt(attempt(BUILDING_ATTEMPT, dependentTable)
                .markBuilding(NOW.minus(Duration.ofHours(2))));

        GcReport report = collector.runOnce(new GcPolicy(RETENTION, 10));

        assertThat(report.examined()).isZero();
        assertThat(store.dropped).isEmpty();
        assertThat(repository.findGeneration(BASE_ID).orElseThrow().state())
                .isEqualTo(GenerationState.Candidate.INSTANCE);
    }

    private Generation published(GenerationId id, AttemptId attemptId, PhysicalTableRef table,
            Instant at) {
        return Generation.candidate(id, object.id(), MaterializationTestFixtures.INCARNATION, 1,
                        attemptId, Optional.empty(), table, RefreshKind.Full.INSTANCE, VECTOR, at)
                .commit(MaterializationTestFixtures.evidence("commit-" + id.value()), at)
                .publish(at);
    }

    private RefreshAttempt attempt(AttemptId attemptId, PhysicalTableRef table) {
        return RefreshAttempt.reserve(attemptId, object,
                MaterializationTestFixtures.definition(object.id(), VECTOR),
                MaterializationTestFixtures.REQUESTER,
                new RefreshAttempt.AttemptScope(Optional.empty(), RefreshKind.Full.INSTANCE, VECTOR,
                        TriggerKind.MANUAL, MaterializationTestFixtures.fence(), table),
                NOW.minus(Duration.ofHours(3)));
    }

    private static PhysicalTableRef candidateTable(int discriminator) {
        return MaterializationTestFixtures.candidate(discriminator);
    }

    private static GenerationId generationId(String suffix) {
        return new GenerationId(UUID.fromString(
                "00000000-0000-0000-0000-0000000000" + suffix));
    }

    private static AttemptId attemptId(String suffix) {
        return new AttemptId(UUID.fromString(
                "00000000-0000-0000-0000-0000000000" + suffix));
    }

    private static final class RecordingStore implements PhysicalGenerationStore {

        private final List<PhysicalTableRef> dropped = new ArrayList<>();
        private final Set<PhysicalTableRef> failures = new HashSet<>();

        @Override
        public boolean exists(org.pangrova.materialization.domain.MaterializationId object,
                PhysicalTableRef table) {
            return !dropped.contains(table);
        }

        @Override
        public void drop(org.pangrova.materialization.domain.MaterializationId object,
                PhysicalTableRef table) {
            if (failures.contains(table)) {
                throw new IllegalStateException("drop failed for " + table.qualifiedName());
            }
            dropped.add(table);
        }
    }

    private record FixedClock(Instant now) implements Clock {
    }
}
