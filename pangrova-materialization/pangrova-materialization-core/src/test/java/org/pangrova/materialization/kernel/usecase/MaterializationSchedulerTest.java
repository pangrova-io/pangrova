package org.pangrova.materialization.kernel.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.kernel.InMemoryMaterializationRepository;
import org.pangrova.materialization.kernel.MaterializationTestFixtures;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleLease;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.usecase.MaterializationScheduler;
import org.pangrova.materialization.usecase.ScheduledRefresh;

class MaterializationSchedulerTest {

    private static final Instant NOW = MaterializationTestFixtures.NOW;
    private static final MaterializationId OBJECT = MaterializationTestFixtures.objectId("orders_dt");
    private static final TargetLag LAG = new TargetLag(Duration.ofSeconds(30));

    private MutableClock clock;
    private InMemoryMaterializationRepository repository;
    private RecordingRefresh refresher;
    private MaterializationScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(NOW);
        repository = new InMemoryMaterializationRepository(clock);
        refresher = new RecordingRefresh();
        scheduler = new MaterializationScheduler(repository, refresher, clock,
                new MaterializationScheduler.Settings("scheduler-a", Duration.ofMinutes(2),
                        Duration.ofSeconds(10), Duration.ofHours(1), 3));
    }

    @Test
    void claimsDueScheduleAndRecordsSuccess() {
        repository.putSchedule(Schedule.scheduled(OBJECT, LAG, NOW.minusSeconds(5)));

        MaterializationScheduler.Run run = scheduler.runOnce().orElseThrow();

        assertThat(run.succeeded()).isTrue();
        assertThat(run.failure()).isNull();
        assertThat(refresher.calls).hasSize(1);
        assertThat(refresher.calls.get(0).trigger()).isEqualTo(TriggerKind.SCHEDULED);
        assertThat(refresher.calls.get(0).commandId())
                .isEqualTo(scheduledCommand(NOW.minusSeconds(5)));
        Schedule stored = repository.findSchedule(OBJECT).orElseThrow();
        assertThat(stored.version()).isEqualTo(2);
        assertThat(stored.consecutiveFailures()).isZero();
        assertThat(stored.lease()).isEmpty();
        assertThat(stored.lastSuccessAt()).contains(NOW);
        assertThat(stored.nextDueAt()).contains(NOW.plusSeconds(30));
        assertThat(stored.state()).isEqualTo(ScheduleState.ACTIVE);
    }

    @Test
    void skipsWhenNothingIsDue() {
        repository.putSchedule(Schedule.scheduled(OBJECT, LAG, NOW.plusSeconds(60)));

        assertThat(scheduler.runOnce()).isEmpty();
        assertThat(refresher.calls).isEmpty();
        assertThat(repository.findSchedule(OBJECT).orElseThrow().version()).isZero();
    }

    @Test
    void recordsFailureAndSchedulesBoundedBackoff() {
        repository.putSchedule(Schedule.scheduled(OBJECT, LAG, NOW.minusSeconds(5)));
        RuntimeException failure = new IllegalStateException("native execution failed");
        refresher.failure = failure;

        MaterializationScheduler.Run run = scheduler.runOnce().orElseThrow();

        assertThat(run.succeeded()).isFalse();
        assertThat(run.failure()).isSameAs(failure);
        Schedule stored = repository.findSchedule(OBJECT).orElseThrow();
        assertThat(stored.consecutiveFailures()).isEqualTo(1);
        assertThat(stored.version()).isEqualTo(2);
        assertThat(stored.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(stored.lease()).isEmpty();
        assertThat(stored.nextDueAt().orElseThrow())
                .isAfterOrEqualTo(NOW.plusSeconds(9))
                .isBeforeOrEqualTo(NOW.plusSeconds(11));
    }

    @Test
    void concurrentPublicationDefersWithoutCountingAFailure() {
        repository.putSchedule(Schedule.scheduled(OBJECT, LAG, NOW.minusSeconds(5)));
        refresher.failure = MaterializationException.concurrentPublication(
                "another refresh already owns the object");

        MaterializationScheduler.Run run = scheduler.runOnce().orElseThrow();

        assertThat(run.succeeded()).isFalse();
        assertThat(run.deferred()).isTrue();
        assertThat(run.failure()).isSameAs(refresher.failure);
        Schedule stored = repository.findSchedule(OBJECT).orElseThrow();
        assertThat(stored.consecutiveFailures()).isZero();
        assertThat(stored.lease()).isEmpty();
        assertThat(stored.version()).isEqualTo(2);
        assertThat(stored.nextDueAt()).contains(NOW.plusSeconds(10));
    }

    @Test
    void suspendsScheduleAfterConsecutiveFailureThreshold() {
        repository.putSchedule(new Schedule(OBJECT, Optional.of(LAG),
                Optional.of(NOW.minusSeconds(1)), Optional.empty(), 2, ScheduleState.ACTIVE,
                Optional.empty(), 0));
        refresher.failure = new IllegalStateException("boom");

        scheduler.runOnce();

        Schedule stored = repository.findSchedule(OBJECT).orElseThrow();
        assertThat(stored.consecutiveFailures()).isEqualTo(3);
        assertThat(stored.state()).isEqualTo(ScheduleState.SUSPENDED);
        assertThat(stored.lease()).isEmpty();
        assertThat(stored.version()).isEqualTo(3);
    }

    @Test
    void expiredLeaseAllowsAnotherOwnerToClaim() {
        repository.putSchedule(new Schedule(OBJECT, Optional.of(LAG),
                Optional.of(NOW.minusSeconds(60)), Optional.empty(), 0, ScheduleState.ACTIVE,
                Optional.of(new ScheduleLease("dead-owner", NOW.minusSeconds(30))), 4));

        assertThat(scheduler.runOnce()).isPresent();
        assertThat(refresher.calls).hasSize(1);
        Schedule stored = repository.findSchedule(OBJECT).orElseThrow();
        assertThat(stored.version()).isEqualTo(6);
        assertThat(stored.lastSuccessAt()).contains(NOW);
    }

    @Test
    void reclaimOfTheSameDueWindowReusesTheCommandId() {
        Instant due = NOW.minusSeconds(5);
        repository.putSchedule(Schedule.scheduled(OBJECT, LAG, due));
        refresher.failure = new IllegalStateException("lease owner crashed");
        repository.failNextScheduleUpdate();

        assertThatThrownBy(scheduler::runOnce)
                .isInstanceOf(MaterializationException.class);

        clock.advance(Duration.ofMinutes(5));
        refresher.failure = null;
        refresher.calls.clear();
        scheduler.runOnce();

        assertThat(refresher.calls).hasSize(1);
        assertThat(refresher.calls.get(0).commandId()).isEqualTo(scheduledCommand(due));
    }

    private static CommandId scheduledCommand(Instant due) {
        String seed = OBJECT.canonicalName() + ':' + due;
        return new CommandId("sched-" + UUID.nameUUIDFromBytes(
                seed.getBytes(StandardCharsets.UTF_8)));
    }

    private record RefreshCall(MaterializationId object, CommandId commandId,
            TriggerKind trigger) {
    }

    private static final class RecordingRefresh implements ScheduledRefresh {

        private final List<RefreshCall> calls = new ArrayList<>();
        private RuntimeException failure;

        @Override
        public void refresh(MaterializationId object, CommandId commandId, TriggerKind trigger) {
            calls.add(new RefreshCall(object, commandId, trigger));
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class MutableClock implements Clock {

        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public Instant now() {
            return now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }
    }
}
