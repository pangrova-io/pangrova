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
import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TriggerKind;
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
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.materialization.port.PublicationVerification;
import org.pangrova.connector.spi.materialization.ChangeSetMode;
import org.pangrova.connector.spi.materialization.MaterializationSourceException;
import org.pangrova.connector.spi.materialization.SourceRef;

class MaterializationServiceIncrementalTest {

    private static final MaterializationId OBJECT = new MaterializationId(
            MaterializationTestFixtures.TENANT, QualifiedName.parse("lake.db.orders_dt"));
    private static final SourceRef ORDERS = MaterializationTestFixtures.source("orders");

    private InMemoryMaterializationRepository repository;
    private MaterializationTestFixtures.RecordingPins pins;
    private RecordingExecution execution;
    private RecordingAuthorizer authorizer;
    private MutablePlanner planner;
    private TestRuntime runtime;

    @BeforeEach
    void setUp() {
        runtime = new TestRuntime();
        repository = new InMemoryMaterializationRepository(runtime);
        pins = new MaterializationTestFixtures.RecordingPins();
        execution = new RecordingExecution();
        authorizer = new RecordingAuthorizer();
        planner = new MutablePlanner(MaterializationTestFixtures.vector(ORDERS),
                Set.of(RefreshKind.Full.INSTANCE));
    }

    @Test
    void creationBootstrapsFullThenRefreshAppendsAProvenDelta() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        MaterializationOutcome created = service().create(command(RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), "create-1"));
        Generation base = created.generation();

        assertThat(created.newlyPublished()).isTrue();
        assertThat(execution.requests).singleElement().satisfies(request -> {
            assertThat(request.attempt().refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
            assertThat(request.attempt().baseGeneration()).isEmpty();
            assertThat(request.plan()).get().satisfies(plan -> {
                assertThat(plan.kind()).isEqualTo(RefreshKind.Full.INSTANCE);
                assertThat(plan.changeSets()).isEmpty();
            });
        });

        planner.vector = MaterializationTestFixtures.vector(Map.of(ORDERS, MaterializationTestFixtures.snapshot(ORDERS, "snap-2")), 2);
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);

        MaterializationOutcome refreshed = service().refresh(new RefreshMaterialization(
                OBJECT, MaterializationTestFixtures.REQUESTER, TriggerKind.MANUAL,
                new CommandId("refresh-1")));

        assertThat(refreshed.newlyPublished()).isTrue();
        assertThat(refreshed.generation().refreshKind())
                .isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
        assertThat(refreshed.generation().baseGeneration()).contains(base.id());
        assertThat(execution.requests).hasSize(2);
        assertThat(execution.requests.get(1).plan()).get().satisfies(plan -> {
            assertThat(plan.kind()).isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
            assertThat(plan.canonicalSql())
                    .contains("SELECT * FROM \"lake\".")
                    .contains("UNION ALL SELECT id, name FROM orders");
            assertThat(plan.changeSets()).containsOnlyKeys(ORDERS);
        });
        assertThat(pins.changeSets()).singleElement().satisfies(plan -> {
            assertThat(plan.mode()).isEqualTo(ChangeSetMode.APPEND_ONLY);
            assertThat(plan.fromSnapshot().snapshotToken()).isEqualTo("snap-0");
            assertThat(plan.toSnapshot().snapshotToken()).isEqualTo("snap-2");
        });
        assertThat(pins.pinned()).hasSize(3);
        assertThat(pins.released()).containsExactlyInAnyOrderElementsOf(pins.pinned());
        assertThat(repository.findReaderPins(base.id())).isEmpty();
        assertThat(repository.findAttempt(capability(1).attempt()).orElseThrow().state())
                .isEqualTo(AttemptState.Published.INSTANCE);
    }

    @Test
    void appendOnlyFailureNeverFallsBackToAFullRefresh() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        service().create(command(RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), "create-1"));
        Generation base = repository.findPublishedGeneration(OBJECT).orElseThrow();
        planner.vector = MaterializationTestFixtures.vector(Map.of(ORDERS, MaterializationTestFixtures.snapshot(ORDERS, "snap-2")), 2);
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        pins.failChangeSet(new MaterializationSourceException(
                MaterializationSourceException.Reason.PIN_UNSUPPORTED, "no change log"));

        assertThatThrownBy(() -> service().refresh(new RefreshMaterialization(
                OBJECT, MaterializationTestFixtures.REQUESTER, TriggerKind.MANUAL,
                new CommandId("refresh-1"))))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE);
                    assertThat(error.getMessage()).contains("PIN_UNSUPPORTED");
                });

        assertThat(execution.requests).hasSize(1);
        MaterializationWriteCapability failed = capability(1);
        assertThat(repository.findAttempt(failed.attempt()).orElseThrow().state())
                .isEqualTo(AttemptState.Failed.INSTANCE);
        assertThat(repository.findGeneration(failed.generation()).orElseThrow().state())
                .isEqualTo(GenerationState.Collected.INSTANCE);
        assertThat(repository.findPublishedGeneration(OBJECT).orElseThrow().id()).isEqualTo(base.id());
        assertThat(repository.findReaderPins(base.id())).isEmpty();
        assertThat(pins.released()).hasSize(3).containsExactlyInAnyOrderElementsOf(pins.pinned());
    }

    @Test
    void incrementalModeRequiresACompatibleRegisteredStrategyAtCreation() {
        assertThatThrownBy(() -> service().create(command(RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), "create-1")))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
                    assertThat(error.getMessage()).contains("APPEND_ONLY");
                });
        assertThat(execution.requests).isEmpty();
        assertThat(repository.findObject(OBJECT)).isEmpty();

        assertThatThrownBy(() -> service().create(command(RefreshMode.INCREMENTAL,
                Optional.empty(), "create-2")))
                .isInstanceOfSatisfying(MaterializationException.class, error ->
                        assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED));
        assertThat(execution.requests).isEmpty();
    }

    @Test
    void fullModeRejectsAnIncrementalStrategy() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE);

        assertThatThrownBy(() -> service().create(command(RefreshMode.FULL,
                Optional.of(StrategyId.APPEND_ONLY), "create-1")))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
                    assertThat(error.getMessage()).contains("FULL refresh mode");
                });
        assertThat(execution.requests).isEmpty();
    }

    @Test
    void incrementalModeWithoutConfiguredStrategySelectsAppendOnly() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);

        service().create(command(RefreshMode.INCREMENTAL, Optional.empty(), "create-1"));
        assertThat(execution.requests.get(0).attempt().refreshKind())
                .isEqualTo(RefreshKind.Full.INSTANCE);

        planner.vector = MaterializationTestFixtures.vector(Map.of(ORDERS, MaterializationTestFixtures.snapshot(ORDERS, "snap-2")), 2);
        service().refresh(new RefreshMaterialization(OBJECT, MaterializationTestFixtures.REQUESTER,
                TriggerKind.MANUAL, new CommandId("refresh-1")));

        assertThat(execution.requests.get(1).attempt().refreshKind())
                .isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
    }

    @Test
    void redefinitionBootstrapsAFullGenerationBeforeIncrementalResumes() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        service().create(command(RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), "create-1"));
        Generation base = repository.findPublishedGeneration(OBJECT).orElseThrow();

        service().redefine(new RedefineMaterialization(OBJECT,
                MaterializationTestFixtures.REQUESTER,
                "SELECT id, name FROM orders WHERE id > 10", Map.of(),
                new CommandId("redefine-1")));

        planner.vector = MaterializationTestFixtures.vector(Map.of(ORDERS,
                MaterializationTestFixtures.snapshot(ORDERS, "snap-3")), 3);
        MaterializationOutcome refreshed = service().refresh(new RefreshMaterialization(
                OBJECT, MaterializationTestFixtures.REQUESTER, TriggerKind.MANUAL,
                new CommandId("refresh-1")));

        assertThat(refreshed.generation().refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(refreshed.generation().definitionRevision()).isEqualTo(2);
        assertThat(refreshed.generation().baseGeneration()).isEmpty();
        assertThat(refreshed.generation().id()).isNotEqualTo(base.id());
        assertThat(execution.requests.get(1).attempt().baseGeneration()).isEmpty();
        assertThat(execution.requests.get(1).plan()).get().satisfies(plan -> {
            assertThat(plan.kind()).isEqualTo(RefreshKind.Full.INSTANCE);
            assertThat(plan.changeSets()).isEmpty();
        });
        assertThat(pins.changeSets()).isEmpty();
    }

    @Test
    void partitionReplaceRefreshPlansOnlyTheChangedPartitions() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE,
                RefreshKind.IncrementalPartitionReplace.INSTANCE);
        planner.alignment = new org.pangrova.materialization.domain.PartitionAlignment(
                org.pangrova.materialization.domain.PartitionAlignment.Shape.SCAN,
                List.of(new org.pangrova.materialization.domain.PartitionAlignment.Column(
                        "region", "name", false)));
        MaterializationOutcome created = service().create(command(RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.PARTITION_REPLACE), "create-1"));
        Generation base = created.generation();

        planner.vector = MaterializationTestFixtures.vector(
                Map.of(ORDERS, MaterializationTestFixtures.snapshot(ORDERS, "snap-2")), 2);
        pins.respondWithPartitions(List.of(
                new org.pangrova.connector.spi.materialization.PartitionChange(List.of(
                        new org.pangrova.connector.spi.materialization.PartitionValue("region", "'eu'")))));

        MaterializationOutcome refreshed = service().refresh(new RefreshMaterialization(
                OBJECT, MaterializationTestFixtures.REQUESTER, TriggerKind.MANUAL,
                new CommandId("refresh-1")));

        assertThat(refreshed.generation().refreshKind())
                .isEqualTo(RefreshKind.IncrementalPartitionReplace.INSTANCE);
        assertThat(refreshed.generation().baseGeneration()).contains(base.id());
        assertThat(execution.requests.get(1).plan()).get().satisfies(plan -> {
            assertThat(plan.kind()).isEqualTo(RefreshKind.IncrementalPartitionReplace.INSTANCE);
            assertThat(plan.canonicalSql())
                    .contains("WHERE ((\"name\" IS NULL OR \"name\" <> 'eu'))")
                    .contains("WHERE (\"name\" = 'eu')");
            assertThat(plan.changeSets()).containsOnlyKeys(ORDERS);
        });
        assertThat(pins.changeSets()).singleElement().satisfies(plan -> {
            assertThat(plan.mode()).isEqualTo(ChangeSetMode.PARTITION_REPLACE);
            assertThat(plan.changedPartitions()).hasSize(1);
        });
    }

    private MaterializationWriteCapability capability(int index) {
        return authorizer.capabilities.get(index);
    }

    private CreateMaterialization command(
            RefreshMode mode, Optional<StrategyId> strategy, String commandId) {
        return new CreateMaterialization(
                OBJECT,
                MaterializationKind.DYNAMIC_TABLE,
                "SELECT id, name FROM orders",
                MaterializationTestFixtures.REQUESTER,
                MaterializationTestFixtures.DEFINER,
                SecurityMode.DEFINER,
                mode,
                strategy,
                Optional.<TargetLag>empty(),
                Map.of(),
                new CommandId(commandId),
                false);
    }

    private MaterializationService service() {
        return new MaterializationService(new KernelPorts(
                repository, new EmptyCatalog(), planner, execution, pins, authorizer,
                runtime, runtime, verification -> {
                }, MaterializationTestFixtures.extensions(), UUID::randomUUID, runtime));
    }

    private static final class MutablePlanner implements DefinitionPlanner {

        private SourceVector vector;
        private Set<RefreshKind> kinds;
        private org.pangrova.materialization.domain.PartitionAlignment alignment =
                org.pangrova.materialization.domain.PartitionAlignment.NONE;

        private MutablePlanner(SourceVector vector, Set<RefreshKind> kinds) {
            this.vector = vector;
            this.kinds = kinds;
        }

        @Override
        public PlannedDefinition plan(PlanningRequest request) {
            return new PlannedDefinition(
                    MaterializationTestFixtures.binding(request.definitionSql()),
                    MaterializationTestFixtures.outputSchema(),
                    vector,
                    Set.of(),
                    MaterializationTestFixtures.policies(vector.snapshots().keySet()),
                    kinds,
                    alignment);
        }
    }

    private static final class RecordingExecution implements ExecutionEngine {

        private final List<ExecutionRequest> requests = new ArrayList<>();

        @Override
        public ExecutionResult execute(ExecutionRequest request) {
            requests.add(request);
            return new ExecutionResult(
                    MaterializationTestFixtures.evidence("commit-" + requests.size()),
                    requests.size(), "execution-" + requests.size());
        }
    }

    private static final class RecordingAuthorizer implements MaterializationAuthorizer {

        private final List<MaterializationWriteCapability> capabilities = new ArrayList<>();

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
            return new ExecutionFence("incremental-anchor", 1,
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
        public List<PublishedMaterialization> listPublished(
                org.pangrova.materialization.domain.TenantId tenant) {
            return List.of();
        }

        @Override
        public long registryEpoch(org.pangrova.materialization.domain.TenantId tenant) {
            return 0;
        }
    }
}
