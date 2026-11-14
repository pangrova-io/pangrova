package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit test for BUG-D13 fix: {@link ElectionScheduler#scheduleFailureDetection}
 * and {@link ElectionScheduler#scheduleLeaseRenewal} must check
 * {@code running.get()} before scheduling to prevent
 * {@link java.util.concurrent.RejectedExecutionException} on a
 * shutting-down executor.
 */
class ElectionSchedulerRunningGuardTest {

    @Test
    @DisplayName("scheduleFailureDetection is no-op when running=false (BUG-D13)")
    void scheduleFailureDetectionRejectsWhenNotRunning() {
        AtomicBoolean running = new AtomicBoolean(false);
        ElectionScheduler scheduler = new ElectionScheduler(
                testConfig(), running, new NoOpCallback());

        // Do NOT call start() — executor is null.
        // If the running guard works, scheduleFailureDetection returns immediately
        // without touching the null executor.
        assertThatCode(() -> scheduler.scheduleFailureDetection("coord-1"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("scheduleLeaseRenewal is no-op when running=false (BUG-D13)")
    void scheduleLeaseRenewalRejectsWhenNotRunning() {
        AtomicBoolean running = new AtomicBoolean(false);
        ElectionScheduler scheduler = new ElectionScheduler(
                testConfig(), running, new NoOpCallback());

        assertThatCode(() -> scheduler.scheduleLeaseRenewal("coord-1"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("scheduleFailureDetection works when running=true and executor started")
    void scheduleFailureDetectionWorksWhenRunning() {
        AtomicBoolean running = new AtomicBoolean(true);
        ElectionScheduler scheduler = new ElectionScheduler(
                testConfig(), running, new NoOpCallback());
        scheduler.start();

        assertThatCode(() -> scheduler.scheduleFailureDetection("coord-1"))
                .doesNotThrowAnyException();

        scheduler.cancelScheduledTask();
        scheduler.shutdown();
    }

    @Test
    @DisplayName("scheduleLeaseRenewal works when running=true and executor started")
    void scheduleLeaseRenewalWorksWhenRunning() {
        AtomicBoolean running = new AtomicBoolean(true);
        ElectionScheduler scheduler = new ElectionScheduler(
                testConfig(), running, new NoOpCallback());
        scheduler.start();

        assertThatCode(() -> scheduler.scheduleLeaseRenewal("coord-1"))
                .doesNotThrowAnyException();

        scheduler.cancelScheduledTask();
        scheduler.shutdown();
    }

    private static HighAvailabilityCoordinationService.CoordinationConfig testConfig() {
        return new HighAvailabilityCoordinationService.CoordinationConfig(
                15000, 5000);
    }

    static final class NoOpCallback implements ElectionScheduler.Callback {
        @Override public boolean renewLease(String coordinatorId) { return true; }
        @Override public boolean tryBecomeLeader(String coordinatorId) { return false; }
        @Override public Optional<String> currentLeader() { return Optional.of("leader-1"); }
        @Override public void onLeaseRenewalFailed(String coordinatorId) {}
        @Override public void onHeartbeatTick(String coordinatorId) {}
    }
}
