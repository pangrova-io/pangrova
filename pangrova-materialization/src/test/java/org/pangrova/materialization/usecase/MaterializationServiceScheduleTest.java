package org.pangrova.materialization.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
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
import org.pangrova.materialization.domain.PolicySnapshot;
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
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.spi.materialization.SourceRef;

class MaterializationServiceScheduleTest {

    private final InMemoryMaterializationRepository repository =
            new InMemoryMaterializationRepository(() -> MaterializationTestFixtures.NOW);

    @Test
    void suspendMarksActiveScheduleSuspendedAndRepeatsAsNoOp() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW));
        MaterializationService service = service();

        service.suspend(new SuspendMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("suspend-1")));
        Schedule suspended = repository.findSchedule(object).orElseThrow();
        assertThat(suspended.state()).isEqualTo(ScheduleState.SUSPENDED);
        assertThat(suspended.targetLag())
                .contains(new TargetLag(Duration.ofMinutes(1)));

        service.suspend(new SuspendMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("suspend-2")));
        assertThat(repository.findSchedule(object).orElseThrow().version())
                .isEqualTo(suspended.version());
    }

    @Test
    void resumeActivatesSuspendedScheduleAndRepeatsAsNoOp() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW)
                .suspend());
        MaterializationService service = service();

        service.resume(new ResumeMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("resume-1")));
        Schedule active = repository.findSchedule(object).orElseThrow();
        assertThat(active.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(active.nextDueAt()).contains(MaterializationTestFixtures.NOW);

        service.resume(new ResumeMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("resume-2")));
        assertThat(repository.findSchedule(object).orElseThrow().version())
                .isEqualTo(active.version());
    }

    @Test
    void setTargetLagActivatesManualSchedule() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_mv");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.manual(object));

        service().setTargetLag(new SetTargetLag(object,
                MaterializationTestFixtures.REQUESTER, new TargetLag(Duration.ofMinutes(2)),
                new CommandId("lag-1")));

        Schedule schedule = repository.findSchedule(object).orElseThrow();
        assertThat(schedule.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(schedule.targetLag())
                .contains(new TargetLag(Duration.ofMinutes(2)));
        assertThat(schedule.nextDueAt()).contains(
                MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(2)));
    }

    @Test
    void setTargetLagRecomputesActiveDeadlineFromLastSuccess() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW)
                .recordSuccess(MaterializationTestFixtures.NOW));

        service().setTargetLag(new SetTargetLag(object,
                MaterializationTestFixtures.REQUESTER, new TargetLag(Duration.ofMinutes(10)),
                new CommandId("lag-2")));

        Schedule schedule = repository.findSchedule(object).orElseThrow();
        assertThat(schedule.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(schedule.nextDueAt()).contains(
                MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(10)));
    }

    @Test
    void setTargetLagKeepsSuspendedScheduleSuspended() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW)
                .suspend());

        service().setTargetLag(new SetTargetLag(object,
                MaterializationTestFixtures.REQUESTER, new TargetLag(Duration.ofMinutes(10)),
                new CommandId("lag-3")));

        Schedule schedule = repository.findSchedule(object).orElseThrow();
        assertThat(schedule.state()).isEqualTo(ScheduleState.SUSPENDED);
        assertThat(schedule.targetLag())
                .contains(new TargetLag(Duration.ofMinutes(10)));
        assertThat(schedule.nextDueAt()).isEmpty();
    }

    @Test
    void resumeOfManualScheduleIsRejected() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_mv");
        repository.putObject(active(object));
        repository.putSchedule(Schedule.manual(object));

        assertThatThrownBy(() -> service().resume(new ResumeMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("resume-manual"))))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getMessage()).contains("no target lag to resume");
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
                });
    }

    @Test
    void alteringDroppedObjectIsRejected() {
        MaterializationId object = MaterializationTestFixtures.objectId("orders_dt");
        MaterializationObject active = active(object);
        MaterializationObject dropping = active.transitionTo(
                ObjectState.Dropping.INSTANCE, active.revision(), MaterializationTestFixtures.NOW);
        repository.putObject(dropping.transitionTo(
                ObjectState.Dropped.INSTANCE, dropping.revision(), MaterializationTestFixtures.NOW));
        repository.putSchedule(Schedule.scheduled(object,
                new TargetLag(Duration.ofMinutes(1)), MaterializationTestFixtures.NOW));

        assertThatThrownBy(() -> service().suspend(new SuspendMaterialization(object,
                MaterializationTestFixtures.REQUESTER, new CommandId("suspend-dropped"))))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("is DROPPED");
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
        public PolicySnapshot captureSourcePolicies(Principal definer, Set<SourceRef> sources) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MaterializationWriteCapability issueWriteCapability(RefreshAttempt attempt,
                GenerationId generation) {
            throw new UnsupportedOperationException();
        }
    }
}
