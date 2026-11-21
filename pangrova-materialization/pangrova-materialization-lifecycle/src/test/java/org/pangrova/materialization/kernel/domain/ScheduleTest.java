package org.pangrova.materialization.kernel.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.LATER;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.objectId;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.ScheduleLease;
import org.pangrova.materialization.api.enums.ScheduleState;
import org.pangrova.materialization.api.domain.TargetLag;

class ScheduleTest {

    private static final MaterializationId OBJECT = objectId("orders_dt");
    private static final TargetLag LAG = new TargetLag(Duration.ofMinutes(5));

    @Test
    void manualScheduleNeverFiresAndCannotBeClaimed() {
        Schedule schedule = Schedule.manual(OBJECT);

        assertThat(schedule.state()).isEqualTo(ScheduleState.DISABLED);
        assertThat(schedule.targetLag()).isEmpty();
        assertThat(schedule.nextDueAt()).isEmpty();
        assertThat(schedule.isDueAt(NOW)).isFalse();

        MaterializationException exception = catchThrowableOfType(
                () -> schedule.claim("scheduler-1", NOW, Duration.ofSeconds(30)),
                MaterializationException.class);
        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void activeScheduleIsDueAtItsDeadline() {
        Schedule schedule = Schedule.scheduled(OBJECT, LAG, NOW);

        assertThat(schedule.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(schedule.isDueAt(NOW)).isTrue();
        assertThat(schedule.isDueAt(NOW.minusSeconds(1))).isFalse();
    }

    @Test
    void claimExcludesConcurrentSchedulersUntilTheLeaseExpires() {
        Schedule schedule = Schedule.scheduled(OBJECT, LAG, NOW);
        Schedule claimed = schedule.claim("scheduler-1", NOW, Duration.ofSeconds(30));

        assertThat(claimed.version()).isEqualTo(1);
        assertThat(claimed.lease()).isPresent();
        assertThat(claimed.lease().get().owner()).isEqualTo("scheduler-1");
        assertThat(claimed.isDueAt(NOW)).isFalse();

        MaterializationException liveLease = catchThrowableOfType(
                () -> claimed.claim("scheduler-2", NOW.plusSeconds(10), Duration.ofSeconds(30)),
                MaterializationException.class);
        assertThat(liveLease.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION);

        Schedule reclaimed = claimed.claim("scheduler-2", NOW.plusSeconds(31), Duration.ofSeconds(30));
        assertThat(reclaimed.lease().get().owner()).isEqualTo("scheduler-2");
        assertThat(reclaimed.version()).isEqualTo(2);
    }

    @Test
    void successResetsFailuresAndSchedulesTheNextDeadline() {
        Schedule claimed = Schedule.scheduled(OBJECT, LAG, NOW)
                .claim("scheduler-1", NOW, Duration.ofSeconds(30));

        Schedule succeeded = claimed.recordSuccess(LATER);

        assertThat(succeeded.consecutiveFailures()).isZero();
        assertThat(succeeded.lastSuccessAt()).contains(LATER);
        assertThat(succeeded.nextDueAt()).contains(LATER.plus(LAG.value()));
        assertThat(succeeded.lease()).isEmpty();
        assertThat(succeeded.version()).isEqualTo(2);
    }

    @Test
    void failureIncrementsBackoffCounterAndReleasesTheLease() {
        Schedule schedule = Schedule.scheduled(OBJECT, LAG, NOW)
                .claim("scheduler-1", NOW, Duration.ofSeconds(30))
                .recordFailure(NOW.plusSeconds(40));

        assertThat(schedule.consecutiveFailures()).isEqualTo(1);
        assertThat(schedule.nextDueAt()).contains(NOW.plusSeconds(40));
        assertThat(schedule.lease()).isEmpty();
        assertThat(schedule.version()).isEqualTo(2);
    }

    @Test
    void suspendAndResumeKeepTheFreshnessTarget() {
        Schedule active = Schedule.scheduled(OBJECT, LAG, NOW);
        Schedule suspended = active.suspend();

        assertThat(suspended.state()).isEqualTo(ScheduleState.SUSPENDED);
        assertThat(suspended.targetLag()).contains(LAG);
        assertThat(suspended.isDueAt(NOW.plusSeconds(60))).isFalse();

        Schedule resumed = suspended.resume(NOW.plusSeconds(60));
        assertThat(resumed.state()).isEqualTo(ScheduleState.ACTIVE);
        assertThat(resumed.nextDueAt()).contains(NOW.plusSeconds(60));
        assertThat(resumed.version()).isEqualTo(2);

        MaterializationException doubleSuspend = catchThrowableOfType(
                () -> resumed.suspend().suspend(), MaterializationException.class);
        assertThat(doubleSuspend.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void emptyTargetLagCannotBeResumed() {
        Schedule manual = Schedule.manual(OBJECT);

        MaterializationException exception = catchThrowableOfType(
                () -> manual.resume(NOW), MaterializationException.class);

        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void recordInvariantsRejectContradictorySchedules() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Schedule(
                        OBJECT, Optional.of(LAG), Optional.empty(), Optional.empty(), 0,
                        ScheduleState.ACTIVE, Optional.empty(), 0))
                .withMessageContaining("requires a target lag and next due instant");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Schedule(
                        OBJECT, Optional.of(LAG), Optional.empty(), Optional.empty(), 0,
                        ScheduleState.DISABLED, Optional.empty(), 0))
                .withMessageContaining("disabled schedule cannot carry a target lag");
    }

    @Test
    void upstreamPublicationBringsUnleasedActiveScheduleDue() {
        Schedule active = Schedule.scheduled(OBJECT, LAG, NOW.plusSeconds(300));

        Schedule advanced = active.advanceDueAt(NOW);

        assertThat(advanced.nextDueAt()).contains(NOW);
        assertThat(advanced.version()).isEqualTo(1);
        assertThat(advanced.isDueAt(NOW)).isTrue();
        assertThat(active.advanceDueAt(NOW.plusSeconds(300))).isSameAs(active);
        assertThat(Schedule.manual(OBJECT).advanceDueAt(NOW))
                .isEqualTo(Schedule.manual(OBJECT));

        Schedule leased = active.claim("scheduler-1", NOW, Duration.ofSeconds(30));
        assertThat(leased.advanceDueAt(NOW.plusSeconds(1))).isSameAs(leased);
    }

    @Test
    void targetLagRejectsZeroNegativeAndUnboundedValues() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new TargetLag(Duration.ZERO))
                .withMessageContaining("must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new TargetLag(Duration.ofSeconds(-1)))
                .withMessageContaining("must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new TargetLag(Duration.ofDays(366)))
                .withMessageContaining("must not exceed 365 days");
    }

    @Test
    void leaseRequiresMatchingOwnerAndLiveness() {
        ScheduleLease lease = new ScheduleLease("scheduler-1", NOW.plusSeconds(30));

        lease.requireHeldBy("scheduler-1", NOW);

        MaterializationException wrongOwner = catchThrowableOfType(
                () -> lease.requireHeldBy("scheduler-2", NOW), MaterializationException.class);
        MaterializationException expired = catchThrowableOfType(
                () -> lease.requireHeldBy("scheduler-1", NOW.plusSeconds(30)),
                MaterializationException.class);

        assertThat(wrongOwner.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_LEASE_EXPIRED);
        assertThat(expired.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_LEASE_EXPIRED);
    }
}
