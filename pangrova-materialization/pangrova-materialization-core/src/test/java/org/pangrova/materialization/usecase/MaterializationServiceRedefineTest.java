package org.pangrova.materialization.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.InMemoryMaterializationRepository;
import org.pangrova.materialization.MaterializationTestFixtures;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.connector.spi.materialization.SourceRef;

class MaterializationServiceRedefineTest {

    private final InMemoryMaterializationRepository repository =
            new InMemoryMaterializationRepository(() -> MaterializationTestFixtures.NOW);
    private final List<PlanningRequest> planningRequests = new ArrayList<>();

    @Test
    void redefineRecordsNewRevisionAndKeepsPublishedGeneration() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationObject original = installActive(object);
        long commandRevision = original.revision();
        GenerationId head = original.publishedGeneration().orElseThrow();

        service().redefine(new RedefineMaterialization(object,
                MaterializationTestFixtures.REQUESTER, "SELECT id FROM orders WHERE id > 10",
                java.util.Map.of("catalog", "lake"), new CommandId("redefine-1")));

        MaterializationObject redefined = repository.storedObject(object).orElseThrow();
        assertThat(redefined.definitionRevision()).isEqualTo(2);
        assertThat(redefined.revision()).isEqualTo(commandRevision + 1);
        assertThat(redefined.publishedGeneration()).contains(head);
        assertThat(repository.findDefinition(object, 2).orElseThrow().sql())
                .isEqualTo("SELECT id FROM orders WHERE id > 10");
        assertThat(repository.findDefinition(object, 2).orElseThrow().sessionProperties())
                .containsEntry("catalog", "lake");
        assertThat(planningRequests).singleElement().satisfies(request -> {
            assertThat(request.definitionRevision()).isEqualTo(2);
            assertThat(request.definer()).isEqualTo(MaterializationTestFixtures.DEFINER);
        });
        assertThat(repository.events())
                .extracting(event -> event.eventType())
                .contains(MaterializationEventType.OBJECT_ALTERED);
    }

    @Test
    void sameCommandIdReplaysWithoutAnotherRevision() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        installActive(object);
        MaterializationService service = service();
        RedefineMaterialization command = new RedefineMaterialization(object,
                MaterializationTestFixtures.REQUESTER, "SELECT id FROM orders",
                java.util.Map.of(), new CommandId("redefine-2"));

        service.redefine(command);
        MaterializationObject first = repository.storedObject(object).orElseThrow();
        service.redefine(command);

        assertThat(repository.storedObject(object).orElseThrow().revision())
                .isEqualTo(first.revision());
        assertThat(repository.findDefinition(object, 3)).isEmpty();
    }

    @Test
    void redefineRejectsDroppedObject() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationObject active = active(object);
        MaterializationObject dropping = active.transitionTo(
                ObjectState.Dropping.INSTANCE, active.revision(), MaterializationTestFixtures.NOW);
        repository.putObject(dropping.transitionTo(
                ObjectState.Dropped.INSTANCE, dropping.revision(), MaterializationTestFixtures.NOW));
        repository.putSchedule(Schedule.manual(object));

        assertThatThrownBy(() -> service().redefine(new RedefineMaterialization(object,
                MaterializationTestFixtures.REQUESTER, "SELECT id FROM orders",
                java.util.Map.of(), new CommandId("redefine-dropped"))))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("redefine requires an active or suspended materialization");
    }

    @Test
    void plannerFailureLeavesHeaderUnchanged() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationObject original = installActive(object);
        MaterializationService service = new MaterializationService(new KernelPorts(
                repository, new EmptyCatalog(), request -> {
                    throw MaterializationException.invalidDefinition("cannot bind new definition");
                }, new UnusedExecution(),
                new MaterializationTestFixtures.RecordingPins(),
                new AllowAllAuthorizer(), (objectId, duration) -> {
                    throw new UnsupportedOperationException();
                }, (fence, now) -> {
                    throw new UnsupportedOperationException();
                }, verification -> {
                    throw new UnsupportedOperationException();
                }, MaterializationTestFixtures.extensions(), UUID::randomUUID,
                (Clock) () -> MaterializationTestFixtures.NOW));

        assertThatThrownBy(() -> service.redefine(new RedefineMaterialization(object,
                MaterializationTestFixtures.REQUESTER, "SELECT missing FROM orders",
                java.util.Map.of(), new CommandId("redefine-failed"))))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("cannot bind new definition");

        MaterializationObject unchanged = repository.storedObject(object).orElseThrow();
        assertThat(unchanged.revision()).isEqualTo(original.revision());
        assertThat(unchanged.definitionRevision()).isEqualTo(1);
        assertThat(repository.findDefinition(object, 2)).isEmpty();
    }

    private MaterializationObject installActive(MaterializationId object) {
        MaterializationObject active = active(object);
        repository.putObject(active);
        repository.putDefinition(MaterializationTestFixtures.definition(
                object, 1, "SELECT id FROM orders"));
        repository.putSchedule(Schedule.manual(object));
        return active;
    }

    private static MaterializationObject active(MaterializationId object) {
        return MaterializationObject.creating(object, MaterializationKind.DYNAMIC_TABLE,
                        MaterializationTestFixtures.INCARNATION,
                        MaterializationTestFixtures.DEFINER,
                        org.pangrova.materialization.domain.SecurityMode.DEFINER,
                        RefreshMode.FULL, Optional.empty(), MaterializationTestFixtures.NOW)
                .publish(MaterializationTestFixtures.GENERATION_ID, 0,
                        MaterializationTestFixtures.NOW);
    }

    private MaterializationService service() {
        DefinitionPlanner planner = request -> {
            planningRequests.add(request);
            Set<SourceRef> sources = Set.of(MaterializationTestFixtures.source("orders"));
            return new PlannedDefinition(
                    MaterializationTestFixtures.binding(request.definitionSql()),
                    MaterializationTestFixtures.outputSchema(),
                    MaterializationTestFixtures.vector(sources.toArray(SourceRef[]::new)),
                    Set.of(),
                    MaterializationTestFixtures.policies(sources),
                    Set.of(RefreshKind.Full.INSTANCE),
                    org.pangrova.materialization.domain.PartitionAlignment.NONE);
        };
        Clock clock = () -> MaterializationTestFixtures.NOW;
        return new MaterializationService(new KernelPorts(
                repository, new EmptyCatalog(), planner, new UnusedExecution(),
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

    private static final class UnusedExecution implements ExecutionEngine {
        @Override
        public ExecutionResult execute(ExecutionRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class AllowAllAuthorizer
            implements org.pangrova.materialization.port.MaterializationAuthorizer {
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
