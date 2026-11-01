package org.pangrova.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.spi.connector.materialization.CommitEvidence;

/** Durable read-model projections and upstream-driven schedule advancement. */
class MaterializationRepositoryProjectionTest {

    private static final Instant NOW = MaterializationTestFixtures.NOW;
    private static final TargetLag LAG = new TargetLag(Duration.ofMinutes(1));

    private final InMemoryMaterializationRepository repository =
            new InMemoryMaterializationRepository(() -> NOW);

    @Test
    void publicationBringsActiveDependentScheduleDue() {
        MaterializationId upstream = MaterializationTestFixtures.objectId("upstream_dt");
        MaterializationId dependent = MaterializationTestFixtures.objectId("dependent_mv");
        seedObject(upstream, MaterializationTestFixtures.definition(upstream, 1, "SELECT 1"),
                false);
        seedObject(dependent, dependentDefinition(upstream, dependent), true);
        Instant future = NOW.plus(Duration.ofHours(1));
        repository.putSchedule(Schedule.scheduled(dependent, LAG, future));
        long versionBefore = repository.findSchedule(dependent).orElseThrow().version();

        Publication first = publish(upstream);

        Schedule advanced = repository.findSchedule(dependent).orElseThrow();
        assertThat(advanced.nextDueAt()).contains(NOW);
        assertThat(advanced.version()).isEqualTo(versionBefore + 1);

        Publication second = publish(upstream, first);
        assertThat(second.header().revision()).isEqualTo(2);
        assertThat(repository.findSchedule(dependent).orElseThrow().nextDueAt()).contains(NOW);
        assertThat(repository.findSchedule(dependent).orElseThrow().version())
                .isEqualTo(versionBefore + 1);
    }

    @Test
    void projectsObjectsAttemptsDependenciesAndGcHistoryByTenant() {
        MaterializationId upstream = MaterializationTestFixtures.objectId("upstream_dt");
        MaterializationId dependent = MaterializationTestFixtures.objectId("dependent_mv");
        seedObject(upstream, MaterializationTestFixtures.definition(upstream, 1, "SELECT 1"),
                false);
        seedObject(dependent, dependentDefinition(upstream, dependent), true);
        publish(upstream);

        assertThat(repository.listObjects(MaterializationTestFixtures.TENANT))
                .extracting(object -> object.id().name().toString())
                .containsExactly("db.dependent_mv", "db.upstream_dt");

        assertThat(repository.findAttempts(MaterializationTestFixtures.TENANT,
                Optional.of(upstream), 100))
                .extracting(RefreshAttempt::object)
                .containsOnly(upstream);
        assertThat(repository.findAttempts(MaterializationTestFixtures.TENANT,
                Optional.empty(), 100)).hasSize(1);

        assertThat(repository.findDependencies(MaterializationTestFixtures.TENANT,
                Optional.of(dependent)))
                .singleElement()
                .satisfies(edge -> {
                    assertThat(edge.upstream()).isEqualTo(upstream);
                    assertThat(edge.downstream()).isEqualTo(dependent);
                    assertThat(edge.downstreamDefinitionRevision()).isEqualTo(1);
                });

        Generation retained = commitGeneration(dependent, 3);
        repository.putGeneration(retained);
        repository.recordGcDecision(GcLedgerEntry.pending(
                retained.id(), "retired generation", NOW));
        assertThat(repository.findGcHistory(MaterializationTestFixtures.TENANT,
                Optional.of(dependent), 100))
                .singleElement()
                .satisfies(entry -> {
                    assertThat(entry.object()).isEqualTo(dependent);
                    assertThat(entry.generation()).isEqualTo(retained.id());
                    assertThat(entry.decision())
                            .isEqualTo(GcLedgerEntry.GcDecision.COLLECT);
                });
    }

    private DefinitionRevision dependentDefinition(MaterializationId upstream,
            MaterializationId dependent) {
        return MaterializationTestFixtures.definition(dependent, 1,
                "SELECT * FROM upstream_dt",
                Set.of(MaterializationTestFixtures.source("upstream_dt")),
                Set.of(MaterializationTestFixtures.dependency(upstream, dependent, 1)));
    }

    private void seedObject(MaterializationId object, DefinitionRevision definition,
            boolean active) {
        MaterializationObject creating = MaterializationObject.creating(
                object, MaterializationKind.DYNAMIC_TABLE, MaterializationTestFixtures.INCARNATION,
                MaterializationTestFixtures.DEFINER, SecurityMode.DEFINER,
                RefreshMode.FULL, Optional.empty(), NOW);
        repository.putObject(active
                ? creating.publish(GenerationId.random(), 0, NOW) : creating);
        repository.putDefinition(definition);
    }

    private Publication publish(MaterializationId object) {
        MaterializationObject header = repository.findObject(object).orElseThrow();
        return publish(object, header.revision(), new CommandId("publish-" + UUID.randomUUID()));
    }

    private Publication publish(MaterializationId object, Publication previous) {
        return publish(object, previous.header().revision(), new CommandId("publish-"
                + UUID.randomUUID()));
    }

    private Publication publish(MaterializationId object, long expectedRevision,
            CommandId commandId) {
        AttemptId attemptId = AttemptId.random();
        PhysicalTableRef table = MaterializationTestFixtures.candidate(
                object.name().tableName().hashCode() & 0x7fff);
        SourceVector vector = MaterializationTestFixtures.vector(
                MaterializationTestFixtures.source(object.name().tableName()));
        CommitEvidence evidence = new CommitEvidence(
                "iceberg", "commit-" + attemptId.value(), "manifest", "v2", NOW);
        RefreshAttempt attempt = MaterializationTestFixtures.attempt(
                        object, attemptId, table, RefreshKind.Full.INSTANCE,
                        vector, Optional.empty())
                .markBuilding(NOW)
                .markCommitted(evidence, NOW);
        Generation candidate = MaterializationTestFixtures.candidateGeneration(
                        object, GenerationId.random(), attemptId, table,
                        RefreshKind.Full.INSTANCE, vector, Optional.empty())
                .commit(evidence, NOW);
        repository.putAttempt(attempt);
        repository.putGeneration(candidate);
        var result = repository.publish(new PublishCommand(object, expectedRevision, candidate,
                        attempt, evidence, MaterializationTestFixtures.fence(), commandId),
                event(object, "published"));
        return new Publication(result.object(), result.generation(), attempt, evidence);
    }

    private Generation commitGeneration(MaterializationId object, int discriminator) {
        AttemptId attemptId = AttemptId.random();
        PhysicalTableRef table = MaterializationTestFixtures.candidate(discriminator);
        SourceVector vector = MaterializationTestFixtures.vector(
                MaterializationTestFixtures.source(object.name().tableName()));
        CommitEvidence evidence = new CommitEvidence(
                "iceberg", "commit-" + attemptId.value(), "manifest", "v2", NOW);
        RefreshAttempt attempt = MaterializationTestFixtures.attempt(
                        object, attemptId, table, RefreshKind.Full.INSTANCE,
                        vector, Optional.empty())
                .markBuilding(NOW)
                .markCommitted(evidence, NOW);
        Generation candidate = MaterializationTestFixtures.candidateGeneration(
                        object, GenerationId.random(), attemptId, table,
                        RefreshKind.Full.INSTANCE, vector, Optional.empty())
                .commit(evidence, NOW);
        repository.putAttempt(attempt);
        return candidate;
    }

    private MaterializationEvent event(MaterializationId object, String detail) {
        return new MaterializationEvent(UUID.randomUUID().toString(),
                MaterializationEventType.GENERATION_PUBLISHED, object, Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                MaterializationTestFixtures.REQUESTER, MaterializationTestFixtures.DEFINER,
                detail, NOW);
    }

    private record Publication(MaterializationObject header, Generation generation,
            RefreshAttempt attempt, CommitEvidence evidence) {
    }
}
