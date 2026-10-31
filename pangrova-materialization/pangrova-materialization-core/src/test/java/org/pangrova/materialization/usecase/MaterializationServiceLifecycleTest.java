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
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.DependencyKind;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.FenceValidator;
import org.pangrova.materialization.port.LeaseService;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationAuthorizer;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.connector.spi.materialization.MaterializationSourceException;
import org.pangrova.connector.spi.materialization.SourceRef;

class MaterializationServiceLifecycleTest {

    private static final MaterializationId OBJECT = new MaterializationId(
            MaterializationTestFixtures.TENANT, QualifiedName.parse("lake.db.orders_dt"));
    private static final MaterializationId RENAMED = new MaterializationId(
            MaterializationTestFixtures.TENANT, QualifiedName.parse("lake.db.orders_dt_v2"));
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
    void createIfNotExistsReturnsTheExistingGenerationWithoutExecutingAgain() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));

        MaterializationOutcome repeated = service().create(create(
                "SELECT id FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-2", true));

        assertThat(repeated.newlyPublished()).isFalse();
        assertThat(repeated.generation().id()).isEqualTo(created.generation().id());
        assertThat(repeated.generation().definitionRevision()).isEqualTo(1);
        assertThat(execution.requests).hasSize(1);
        assertThat(repository.findDefinition(OBJECT, 2)).isEmpty();
    }

    @Test
    void duplicateCreateWithoutIfNotExistsIsRejected() {
        service().create(create("SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));

        assertThatThrownBy(() -> service().create(create("SELECT id, name FROM orders",
                RefreshMode.FULL, Optional.empty(), Optional.empty(), "create-2", false)))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
                    assertThat(error.getMessage()).contains("already exists");
                });
        assertThat(execution.requests).hasSize(1);
    }

    @Test
    void createAfterDropStartsANewIncarnation() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        service().drop(new DropMaterialization(OBJECT, MaterializationTestFixtures.REQUESTER,
                new CommandId("drop-1")));

        MaterializationOutcome recreated = service().create(create(
                "SELECT id FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-2", false));

        assertThat(recreated.newlyPublished()).isTrue();
        assertThat(recreated.object().incarnation())
                .isNotEqualTo(created.object().incarnation());
        assertThat(recreated.object().definitionRevision()).isEqualTo(1);
        assertThat(recreated.generation().definitionRevision()).isEqualTo(1);
        assertThat(repository.findObject(OBJECT).orElseThrow().state())
                .isEqualTo(org.pangrova.materialization.domain.ObjectState.Active.INSTANCE);
        assertThat(repository.findDefinition(OBJECT, 1)).isPresent();
        assertThat(execution.requests).hasSize(2);
    }

    @Test
    void createOrReplaceCreatesTheObjectWhenItIsAbsent() {
        MaterializationOutcome outcome = service().createOrReplace(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "replace-new", false));

        assertThat(outcome.newlyPublished()).isTrue();
        assertThat(outcome.object().definitionRevision()).isEqualTo(1);
        assertThat(repository.findPublishedGeneration(OBJECT)).contains(outcome.generation());
    }

    @Test
    void createOrReplaceKeepsTheOldHeadUntilTheReplacementPublishes() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        GenerationId base = created.generation().id();
        pins.failNextPin(new MaterializationSourceException(
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE, "snapshot expired"));

        CreateMaterialization replacement = create(
                "SELECT id, name FROM orders WHERE id > 10", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "replace-1", false);
        assertThatThrownBy(() -> service().createOrReplace(replacement))
                .isInstanceOfSatisfying(MaterializationException.class, error ->
                        assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE));

        MaterializationObject replaced = repository.storedObject(OBJECT).orElseThrow();
        assertThat(replaced.definitionRevision()).isEqualTo(2);
        assertThat(replaced.publishedGeneration()).contains(base);
        assertThat(repository.findPublishedGeneration(OBJECT).orElseThrow().definitionRevision())
                .isEqualTo(1);
        assertThat(repository.findDefinition(OBJECT, 2).orElseThrow().sql())
                .isEqualTo("SELECT id, name FROM orders WHERE id > 10");

        MaterializationOutcome published = service().createOrReplace(replacement);

        assertThat(published.newlyPublished()).isTrue();
        assertThat(published.object().definitionRevision()).isEqualTo(2);
        assertThat(published.generation().definitionRevision()).isEqualTo(2);
        assertThat(published.generation().id()).isNotEqualTo(base);
        assertThat(repository.findPublishedGeneration(OBJECT)).contains(published.generation());
        assertThat(execution.requests).hasSize(2);
    }

    @Test
    void setRefreshModeRebuildsTheHeadBeforeApplyingTheNewConfiguration() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);

        MaterializationOutcome outcome = service().setRefreshMode(new SetRefreshMode(OBJECT,
                MaterializationTestFixtures.REQUESTER, RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), new CommandId("mode-1")));

        assertThat(outcome.newlyPublished()).isTrue();
        assertThat(outcome.generation().refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(outcome.object().refreshMode()).isEqualTo(RefreshMode.INCREMENTAL);
        assertThat(outcome.object().incrementalStrategy()).contains(StrategyId.APPEND_ONLY);
        assertThat(created.object().refreshMode()).isEqualTo(RefreshMode.FULL);
        assertThat(execution.requests).hasSize(2);
        assertThat(execution.requests.get(1).attempt().trigger()).isEqualTo(TriggerKind.REPAIR);
        assertThat(execution.requests.get(1).attempt().refreshKind())
                .isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(repository.storedObject(OBJECT).orElseThrow().refreshMode())
                .isEqualTo(RefreshMode.INCREMENTAL);
        assertThat(repository.events())
                .extracting(event -> event.eventType())
                .containsSubsequence(MaterializationEventType.GENERATION_PUBLISHED,
                        MaterializationEventType.OBJECT_ALTERED);
    }

    @Test
    void setRefreshModeFullClearsTheIncrementalStrategyAfterARebuild() {
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        service().create(create("SELECT id, name FROM orders", RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), Optional.empty(), "create-1", false));

        MaterializationOutcome outcome = service().setRefreshMode(new SetRefreshMode(OBJECT,
                MaterializationTestFixtures.REQUESTER, RefreshMode.FULL, Optional.empty(),
                new CommandId("mode-full")));

        assertThat(outcome.generation().refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(outcome.object().refreshMode()).isEqualTo(RefreshMode.FULL);
        assertThat(outcome.object().incrementalStrategy()).isEmpty();
        assertThat(execution.requests).hasSize(2);
    }

    @Test
    void failedRebuildLeavesTheRefreshModeAndHeadUnchanged() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        planner.kinds = Set.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE);
        pins.failNextPin(new IllegalStateException("pin refused"));

        assertThatThrownBy(() -> service().setRefreshMode(new SetRefreshMode(OBJECT,
                MaterializationTestFixtures.REQUESTER, RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), new CommandId("mode-fail"))))
                .isInstanceOf(IllegalStateException.class);

        MaterializationObject stored = repository.storedObject(OBJECT).orElseThrow();
        assertThat(stored.refreshMode()).isEqualTo(RefreshMode.FULL);
        assertThat(stored.incrementalStrategy()).isEmpty();
        assertThat(stored.publishedGeneration()).contains(created.generation().id());
        assertThat(stored.revision()).isEqualTo(created.object().revision());
    }

    @Test
    void retryingARecordedRefreshModeChangeAppliesThePendingConfiguration() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        CommandId commandId = new CommandId("mode-resume");
        repository.recordCommand(new CommandRecord(MaterializationTestFixtures.TENANT, commandId,
                "{\"object\":\"" + OBJECT.canonicalName()
                        + "\",\"revision\":1,\"action\":\"altered\"}",
                MaterializationTestFixtures.NOW));

        MaterializationOutcome outcome = service().setRefreshMode(new SetRefreshMode(OBJECT,
                MaterializationTestFixtures.REQUESTER, RefreshMode.INCREMENTAL,
                Optional.of(StrategyId.APPEND_ONLY), commandId));

        assertThat(outcome.newlyPublished()).isFalse();
        assertThat(outcome.generation().id()).isEqualTo(created.generation().id());
        assertThat(outcome.object().refreshMode()).isEqualTo(RefreshMode.INCREMENTAL);
        assertThat(outcome.object().incrementalStrategy()).contains(StrategyId.APPEND_ONLY);
        assertThat(execution.requests).hasSize(1);
    }

    @Test
    void renameMovesTheIdentityAndKeepsGenerationsAndSchedules() {
        MaterializationOutcome created = service().create(create(
                "SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.of(new TargetLag(Duration.ofMinutes(5))), "create-1", false));

        service().rename(new RenameMaterialization(OBJECT,
                MaterializationTestFixtures.REQUESTER, RENAMED, new CommandId("rename-1")));

        assertThat(repository.storedObject(OBJECT)).isEmpty();
        MaterializationObject renamed = repository.storedObject(RENAMED).orElseThrow();
        assertThat(renamed.incarnation()).isEqualTo(created.object().incarnation());
        assertThat(renamed.definitionRevision()).isEqualTo(1);
        assertThat(renamed.publishedGeneration()).contains(created.generation().id());
        assertThat(renamed.revision()).isEqualTo(created.object().revision() + 1);
        assertThat(repository.findDefinition(RENAMED, 1).orElseThrow().object())
                .isEqualTo(RENAMED);
        assertThat(repository.findPublishedGeneration(RENAMED).orElseThrow().id())
                .isEqualTo(created.generation().id());
        assertThat(repository.findGeneration(created.generation().id()).orElseThrow().object())
                .isEqualTo(RENAMED);
        Schedule schedule = repository.findSchedule(RENAMED).orElseThrow();
        assertThat(schedule.object()).isEqualTo(RENAMED);
        assertThat(schedule.targetLag()).contains(new TargetLag(Duration.ofMinutes(5)));
        assertThat(repository.events())
                .anySatisfy(event -> {
                    assertThat(event.eventType()).isEqualTo(MaterializationEventType.OBJECT_ALTERED);
                    assertThat(event.object()).isEqualTo(RENAMED);
                    assertThat(event.detail()).contains("renamed from");
                });

        MaterializationOutcome refreshed = service().refresh(new RefreshMaterialization(RENAMED,
                MaterializationTestFixtures.REQUESTER, TriggerKind.MANUAL,
                new CommandId("refresh-1")));
        assertThat(refreshed.newlyPublished()).isTrue();
        assertThat(refreshed.generation().id()).isNotEqualTo(created.generation().id());
        assertThat(execution.requests).hasSize(2);
    }

    @Test
    void renameRejectsLiveDependents() {
        service().create(create("SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        MaterializationId dependent = MaterializationTestFixtures.objectId("orders_mv");
        repository.putObject(MaterializationObject.creating(dependent,
                        MaterializationKind.MATERIALIZED_VIEW, MaterializationTestFixtures.INCARNATION,
                        MaterializationTestFixtures.DEFINER, SecurityMode.DEFINER, RefreshMode.FULL,
                        Optional.empty(), MaterializationTestFixtures.NOW)
                .publish(MaterializationTestFixtures.GENERATION_ID, 0,
                        MaterializationTestFixtures.NOW));
        repository.putDefinition(MaterializationTestFixtures.definition(dependent, 1,
                "SELECT * FROM orders_dt", Set.of(ORDERS),
                Set.of(new Dependency(OBJECT, dependent, 1, DependencyKind.DIRECT))));

        assertThatThrownBy(() -> service().rename(new RenameMaterialization(OBJECT,
                MaterializationTestFixtures.REQUESTER, RENAMED, new CommandId("rename-2"))))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_DEPENDENCY_CONFLICT);
                    assertThat(error.getMessage()).contains("orders_mv");
                });
        assertThat(repository.storedObject(OBJECT)).isPresent();
        assertThat(repository.storedObject(RENAMED)).isEmpty();
    }

    @Test
    void renameRejectsAnExistingTargetIdentity() {
        service().create(create("SELECT id, name FROM orders", RefreshMode.FULL, Optional.empty(),
                Optional.empty(), "create-1", false));
        repository.putObject(MaterializationObject.creating(RENAMED,
                        MaterializationKind.DYNAMIC_TABLE, MaterializationTestFixtures.INCARNATION,
                        MaterializationTestFixtures.DEFINER, SecurityMode.DEFINER, RefreshMode.FULL,
                        Optional.empty(), MaterializationTestFixtures.NOW)
                .publish(MaterializationTestFixtures.GENERATION_ID, 0,
                        MaterializationTestFixtures.NOW));

        assertThatThrownBy(() -> service().rename(new RenameMaterialization(OBJECT,
                MaterializationTestFixtures.REQUESTER, RENAMED, new CommandId("rename-3"))))
                .isInstanceOfSatisfying(MaterializationException.class, error ->
                        assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED));
        assertThat(repository.storedObject(OBJECT)).isPresent();
    }

    @Test
    void dropIfExistsTreatsAMissingObjectAsASuccessfulNoOp() {
        service().drop(new DropMaterialization(OBJECT, MaterializationTestFixtures.REQUESTER,
                new CommandId("drop-missing"), false, true));

        assertThat(repository.findObject(OBJECT)).isEmpty();
        assertThat(repository.events()).isEmpty();
    }

    @Test
    void dropWithoutIfExistsStillRejectsAMissingObject() {
        assertThatThrownBy(() -> service().drop(new DropMaterialization(OBJECT,
                MaterializationTestFixtures.REQUESTER, new CommandId("drop-missing"))))
                .isInstanceOfSatisfying(MaterializationException.class, error ->
                        assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_OBJECT_NOT_FOUND));
    }

    private CreateMaterialization create(String sql, RefreshMode mode,
            Optional<StrategyId> strategy, Optional<TargetLag> targetLag, String commandId,
            boolean ifNotExists) {
        return new CreateMaterialization(OBJECT, MaterializationKind.DYNAMIC_TABLE, sql,
                MaterializationTestFixtures.REQUESTER, MaterializationTestFixtures.DEFINER,
                SecurityMode.DEFINER, mode, strategy, targetLag, Map.of(),
                new CommandId(commandId), ifNotExists);
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
                    org.pangrova.materialization.domain.PartitionAlignment.NONE);
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
            return new ExecutionFence("lifecycle-anchor", 1,
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
