package org.pangrova.materialization.kernel.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.kernel.InMemoryMaterializationRepository;
import org.pangrova.materialization.kernel.MaterializationTestFixtures;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.DependencyKind;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationAuthorizer;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.usecase.DropMaterialization;
import org.pangrova.materialization.usecase.KernelPorts;
import org.pangrova.materialization.usecase.MaterializationService;

class MaterializationServiceDropTest {

    private final InMemoryMaterializationRepository repository =
            new InMemoryMaterializationRepository(() -> MaterializationTestFixtures.NOW);

    @Test
    void dropSuspendsActiveSchedule() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        repository.putObject(active(object, MaterializationKind.DYNAMIC_TABLE));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW));

        service().drop(new DropMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-1")));

        assertThat(repository.storedObject(object).orElseThrow().state())
                .isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(repository.findSchedule(object).orElseThrow().state())
                .isEqualTo(ScheduleState.SUSPENDED);
    }

    @Test
    void dropWithoutScheduleStillDrops() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_mv");
        repository.putObject(active(object, MaterializationKind.MATERIALIZED_VIEW));

        service().drop(new DropMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-2")));

        assertThat(repository.storedObject(object).orElseThrow().state())
                .isEqualTo(ObjectState.Dropped.INSTANCE);
    }

    @Test
    void dropRetiresThePublishedGenerationAndSettlesInFlightAttempts() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        SourceVector vector = MaterializationTestFixtures.vector(
                MaterializationTestFixtures.source("orders"));
        PhysicalTableRef headTable = MaterializationTestFixtures.candidate(0);
        Generation published = Generation.candidate(MaterializationTestFixtures.GENERATION_ID,
                        object, MaterializationTestFixtures.INCARNATION, 1,
                        MaterializationTestFixtures.ATTEMPT_ID, Optional.empty(), headTable,
                        RefreshKind.Full.INSTANCE, vector, MaterializationTestFixtures.NOW)
                .commit(MaterializationTestFixtures.evidence("commit-head"),
                        MaterializationTestFixtures.NOW)
                .publish(MaterializationTestFixtures.NOW);
        repository.putObject(active(object, MaterializationKind.DYNAMIC_TABLE));
        repository.putGeneration(published);

        AttemptId inFlightId = new AttemptId(
                java.util.UUID.fromString("00000000-0000-0000-0000-0000000000cc"));
        PhysicalTableRef inFlightTable = MaterializationTestFixtures.candidate(1);
        Generation inFlight = Generation.candidate(
                        new GenerationId(java.util.UUID.fromString(
                                "00000000-0000-0000-0000-0000000000dd")),
                        object, MaterializationTestFixtures.INCARNATION, 1, inFlightId,
                        Optional.empty(), inFlightTable,
                        RefreshKind.Full.INSTANCE, vector, MaterializationTestFixtures.NOW)
                .quarantine();
        repository.putGeneration(inFlight);
        repository.putAttempt(MaterializationTestFixtures.attempt(object, inFlightId,
                        inFlightTable, RefreshKind.Full.INSTANCE, vector,
                        Optional.empty())
                .markBuilding(MaterializationTestFixtures.NOW)
                .quarantine("ambiguous commit", MaterializationTestFixtures.NOW));

        service().drop(new DropMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-retire")));

        MaterializationObject stored = repository.storedObject(object).orElseThrow();
        assertThat(stored.state()).isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(stored.publishedGeneration()).isEmpty();
        assertThat(repository.findGeneration(MaterializationTestFixtures.GENERATION_ID)
                .orElseThrow().state()).isEqualTo(GenerationState.Retired.INSTANCE);
        assertThat(repository.findAttempt(inFlightId).orElseThrow().state())
                .isEqualTo(AttemptState.Abandoned.INSTANCE);
        assertThat(repository.findActiveAttempt(object)).isEmpty();
    }

    @Test
    void dropRejectsLiveDependentsUnlessCascadeIsRequested() {
        MaterializationId upstream = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationId downstream = MaterializationTestFixtures.objectId("orders_mv");
        repository.putObject(active(upstream, MaterializationKind.DYNAMIC_TABLE));
        repository.putObject(active(downstream, MaterializationKind.MATERIALIZED_VIEW));
        repository.putDefinition(MaterializationTestFixtures.definition(downstream, 1,
                "SELECT * FROM orders_dt",
                Set.of(MaterializationTestFixtures.source("orders")),
                Set.of(new Dependency(upstream, downstream, 1, DependencyKind.DIRECT))));

        assertThatThrownBy(() -> service().drop(new DropMaterialization(upstream,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-3"))))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("db.orders_mv")
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_DEPENDENCY_CONFLICT);
        assertThat(repository.storedObject(upstream).orElseThrow().state())
                .isEqualTo(ObjectState.Active.INSTANCE);
        assertThat(repository.storedObject(downstream).orElseThrow().state())
                .isEqualTo(ObjectState.Active.INSTANCE);
    }

    @Test
    void cascadeDropsDependentsBeforeTheirUpstream() {
        MaterializationId upstream = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationId downstream = MaterializationTestFixtures.objectId("orders_mv");
        MaterializationId leaf = MaterializationTestFixtures.objectId("orders_leaf");
        repository.putObject(active(upstream, MaterializationKind.DYNAMIC_TABLE));
        repository.putObject(active(downstream, MaterializationKind.MATERIALIZED_VIEW));
        repository.putObject(active(leaf, MaterializationKind.MATERIALIZED_VIEW));
        repository.putDefinition(MaterializationTestFixtures.definition(downstream, 1,
                "SELECT * FROM orders_dt",
                Set.of(MaterializationTestFixtures.source("orders")),
                Set.of(new Dependency(upstream, downstream, 1, DependencyKind.DIRECT))));
        repository.putDefinition(MaterializationTestFixtures.definition(leaf, 1,
                "SELECT * FROM orders_mv",
                Set.of(MaterializationTestFixtures.source("orders")),
                Set.of(new Dependency(downstream, leaf, 1, DependencyKind.DIRECT))));

        service().drop(new DropMaterialization(upstream,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-4"), true));

        assertThat(repository.storedObject(upstream).orElseThrow().state())
                .isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(repository.storedObject(downstream).orElseThrow().state())
                .isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(repository.storedObject(leaf).orElseThrow().state())
                .isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(repository.events())
                .extracting(event -> event.object().name() + ":" + event.eventType())
                .containsSubsequence(
                        "db.orders_leaf:OBJECT_DROPPED",
                        "db.orders_mv:OBJECT_DROPPED",
                        "db.orders_dt:OBJECT_DROPPED");
    }

    private static MaterializationObject active(MaterializationId object,
            MaterializationKind kind) {
        return MaterializationObject.creating(object, kind,
                        MaterializationTestFixtures.INCARNATION,
                        MaterializationTestFixtures.DEFINER,
                        org.pangrova.materialization.domain.SecurityMode.DEFINER,
                        RefreshMode.FULL, Optional.empty(), MaterializationTestFixtures.NOW)
                .publish(MaterializationTestFixtures.GENERATION_ID, 0,
                        MaterializationTestFixtures.NOW);
    }

    private MaterializationService service() {
        Clock clock = () -> MaterializationTestFixtures.NOW;
        return new MaterializationService(new KernelPorts(
                repository, new EmptyCatalog(), new UnusedPlanner(), new UnusedExecution(),
                new MaterializationTestFixtures.RecordingPins(),
                new AllowAllAuthorizer(), (object, duration) -> {
                    throw new UnsupportedOperationException();
                }, (fence, now) -> {
                    throw new UnsupportedOperationException();
                }, verification -> {
                    throw new UnsupportedOperationException();
                }, MaterializationTestFixtures.extensions(), UUID::randomUUID, clock));
    }

    private static final class EmptyCatalog implements MaterializationCatalog {
        @Override
        public Optional<PublishedMaterialization> findPublished(MaterializationId object) {
            return Optional.empty();
        }

        @Override
        public List<PublishedMaterialization> listPublished(TenantId tenant) {
            return List.of();
        }

        @Override
        public long registryEpoch(TenantId tenant) {
            return 0;
        }
    }

    private static final class UnusedPlanner implements DefinitionPlanner {
        @Override
        public PlannedDefinition plan(PlanningRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class UnusedExecution implements ExecutionEngine {
        @Override
        public ExecutionResult execute(ExecutionRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class AllowAllAuthorizer implements MaterializationAuthorizer {
        @Override
        public void requireCreate(MaterializationId object, MaterializationKind kind,
                Principal requester, Set<SourceRef> sources) {
        }

        @Override
        public void requireManage(MaterializationId object, Principal requester,
                org.pangrova.materialization.port.MaterializationAction action) {
        }

        @Override
        public void requireRead(MaterializationId object, Principal requester) {
        }

        @Override
        public MaterializationWriteCapability issueWriteCapability(RefreshAttempt attempt,
                GenerationId generation) {
            throw new UnsupportedOperationException();
        }
    }
}
