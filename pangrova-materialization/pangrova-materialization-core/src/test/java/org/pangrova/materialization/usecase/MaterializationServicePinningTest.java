package org.pangrova.materialization.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.InMemoryMaterializationRepository;
import org.pangrova.materialization.MaterializationTestFixtures;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.PinState;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.FenceValidator;
import org.pangrova.materialization.port.LeaseService;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationAuthorizer;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.connector.api.materialization.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceRef;

class MaterializationServicePinningTest {

    private static final MaterializationId OBJECT = new MaterializationId(
            MaterializationTestFixtures.TENANT, QualifiedName.parse("lake.db.orders_dt"));

    private InMemoryMaterializationRepository repository;
    private MaterializationTestFixtures.RecordingPins pins;
    private RecordingAuthorizer authorizer;
    private RecordingExecution execution;
    private TestRuntime runtime;

    @BeforeEach
    void setUp() {
        runtime = new TestRuntime();
        repository = new InMemoryMaterializationRepository(runtime);
        pins = new MaterializationTestFixtures.RecordingPins();
        authorizer = new RecordingAuthorizer();
        execution = new RecordingExecution();
    }

    @Test
    void pinsEveryPlannedSourceExactlyAndReleasesAfterPublication() {
        SourceRef source = MaterializationTestFixtures.source("orders");
        SourceVector vector = MaterializationTestFixtures.vector(source);
        MaterializationOutcome outcome = service(vector).create(command());

        assertThat(outcome.newlyPublished()).isTrue();
        MaterializationWriteCapability capability = authorizer.singleCapability();
        assertThat(pins.pinned()).containsExactly(new PinnedSource(
                MaterializationTestFixtures.TENANT.value(), source,
                vector.snapshots().get(source), capability.attempt().value().toString()));
        assertThat(pins.released()).isEqualTo(pins.pinned());
        assertThat(execution.requests).singleElement().satisfies(request ->
                assertThat(request.attempt().sourceVector()).isEqualTo(vector));
        assertThat(repository.sourcePins(capability.attempt()))
                .isNotEmpty()
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void rejectsAProviderThatDoesNotPinTheExactSnapshot() {
        SourceRef source = MaterializationTestFixtures.source("orders");
        SourceVector vector = MaterializationTestFixtures.vector(source);
        pins.respondWith(pinned -> new PinnedSource(pinned.tenant(), pinned.source(),
                MaterializationTestFixtures.snapshot(pinned.source(), "other-snapshot"),
                pinned.pinOwner()));

        assertThatThrownBy(() -> service(vector).create(command()))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
                    assertThat(error.getMessage()).contains("exact planned snapshot");
                });

        assertThat(execution.requests).isEmpty();
        MaterializationWriteCapability capability = authorizer.singleCapability();
        assertThat(repository.findAttempt(capability.attempt()).orElseThrow().state())
                .isEqualTo(org.pangrova.materialization.domain.AttemptState.Failed.INSTANCE);
        Generation generation = repository.findGeneration(capability.generation()).orElseThrow();
        assertThat(generation.state()).isEqualTo(GenerationState.Collected.INSTANCE);
        assertThat(pins.released()).isEqualTo(pins.pinned());
        assertThat(repository.sourcePins(capability.attempt()))
                .isNotEmpty()
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void firstPinFailureFailsTheAttemptWithoutExecuting() {
        SourceRef source = MaterializationTestFixtures.source("orders");
        pins.failNextPin(new MaterializationSourceException(
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE, "expired tag"));

        assertThatThrownBy(() -> service(MaterializationTestFixtures.vector(source)).create(command()))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE);
                    assertThat(error.getMessage())
                            .contains("SNAPSHOT_UNAVAILABLE")
                            .contains(source.canonicalName());
                });

        assertThat(execution.requests).isEmpty();
        MaterializationWriteCapability capability = authorizer.singleCapability();
        assertThat(repository.findAttempt(capability.attempt()).orElseThrow().state())
                .isEqualTo(org.pangrova.materialization.domain.AttemptState.Failed.INSTANCE);
        assertThat(repository.findGeneration(capability.generation()).orElseThrow().state())
                .isEqualTo(GenerationState.Collected.INSTANCE);
        assertThat(pins.pinned()).isEmpty();
        assertThat(repository.sourcePins(capability.attempt()))
                .isNotEmpty()
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void laterPinFailureReleasesTheHoldsAlreadyAcquired() {
        SourceRef orders = MaterializationTestFixtures.source("orders");
        SourceRef customers = MaterializationTestFixtures.source("customers");
        SourceVector vector = MaterializationTestFixtures.vector(orders, customers);
        pins.failPinOnCall(2, new MaterializationSourceException(
                MaterializationSourceException.Reason.PIN_UNSUPPORTED, "no retention"));

        assertThatThrownBy(() -> service(vector).create(command()))
                .isInstanceOfSatisfying(MaterializationException.class, error ->
                        assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE));

        assertThat(execution.requests).isEmpty();
        MaterializationWriteCapability capability = authorizer.singleCapability();
        assertThat(pins.pinned()).hasSize(1);
        assertThat(pins.released()).isEqualTo(pins.pinned());
        assertThat(repository.sourcePins(capability.attempt()))
                .hasSize(2)
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void releaseFailureAfterPublicationIsReportedAndKeepsDurablePinsForRecovery() {
        SourceRef source = MaterializationTestFixtures.source("orders");
        pins.failUnpin(new IllegalStateException("tag release failed"));

        assertThatThrownBy(() -> service(MaterializationTestFixtures.vector(source)).create(command()))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REPOSITORY_FAILURE);
                    assertThat(error.getMessage()).contains("pin release failed");
                });

        MaterializationWriteCapability capability = authorizer.singleCapability();
        assertThat(repository.findObject(OBJECT).orElseThrow().publishedGeneration())
                .contains(capability.generation());
        assertThat(repository.sourcePins(capability.attempt()))
                .isNotEmpty()
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.ACQUIRED));
    }

    private CreateMaterialization command() {
        return new CreateMaterialization(
                OBJECT,
                MaterializationKind.DYNAMIC_TABLE,
                "SELECT id, name FROM orders",
                MaterializationTestFixtures.REQUESTER,
                MaterializationTestFixtures.DEFINER,
                SecurityMode.DEFINER,
                RefreshMode.FULL,
                Optional.empty(),
                Optional.<TargetLag>empty(),
                Map.of(),
                new CommandId("create-pin-1"),
                false);
    }

    private MaterializationService service(SourceVector vector) {
        DefinitionPlanner planner = request -> new PlannedDefinition(
                MaterializationTestFixtures.binding(request.definitionSql()),
                MaterializationTestFixtures.outputSchema(),
                vector,
                Set.of(),
                MaterializationTestFixtures.policies(vector.snapshots().keySet()),
                Set.of(RefreshKind.Full.INSTANCE),
                org.pangrova.materialization.domain.PartitionAlignment.NONE);
        return new MaterializationService(new KernelPorts(
                repository, new EmptyCatalog(), planner, execution, pins, authorizer,
                runtime, runtime, verification -> {
                }, MaterializationTestFixtures.extensions(), UUID::randomUUID, runtime));
    }

    private static final class RecordingExecution implements ExecutionEngine {

        private final List<ExecutionRequest> requests = new ArrayList<>();

        @Override
        public ExecutionResult execute(ExecutionRequest request) {
            requests.add(request);
            return new ExecutionResult(
                    MaterializationTestFixtures.evidence("pin-commit"), 2, "execution-1");
        }
    }

    private static final class RecordingAuthorizer implements MaterializationAuthorizer {

        private final List<MaterializationWriteCapability> capabilities = new ArrayList<>();

        private MaterializationWriteCapability singleCapability() {
            assertThat(capabilities).hasSize(1);
            return capabilities.get(0);
        }

        @Override
        public void requireCreate(MaterializationId object, MaterializationKind kind,
                Principal requester, Set<SourceRef> sources) {
        }

        @Override
        public void requireManage(MaterializationId object, Principal requester,
                MaterializationAction action) {
        }

        @Override
        public void requireRead(MaterializationId object, Principal requester) {
        }

        @Override
        public MaterializationWriteCapability issueWriteCapability(RefreshAttempt attempt,
                GenerationId generation) {
            MaterializationWriteCapability capability = new MaterializationWriteCapability(
                    attempt.object(), attempt.incarnation(), attempt.definitionRevision(),
                    attempt.id(), attempt.candidateTable(), generation,
                    attempt.sourceVector().canonicalHash(), attempt.definer(), 0,
                    MaterializationTestFixtures.NOW,
                    MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(30)));
            capabilities.add(capability);
            return capability;
        }
    }

    private static final class TestRuntime implements LeaseService, FenceValidator, Clock {

        @Override
        public ExecutionFence issueExecutionFence(MaterializationId object,
                Duration leaseDuration) {
            return new ExecutionFence("pin-anchor", 1,
                    MaterializationTestFixtures.NOW.plus(leaseDuration));
        }

        @Override
        public void requireValid(ExecutionFence fence, Instant now) {
            fence.requireValidAt(now);
        }

        @Override
        public Instant now() {
            return MaterializationTestFixtures.NOW;
        }
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
}
