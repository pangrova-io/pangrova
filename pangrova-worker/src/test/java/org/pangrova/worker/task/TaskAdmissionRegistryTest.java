package org.pangrova.worker.task;

import org.pangrova.worker.exception.TaskAdmissionException;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real compact admission authority tests; these are not Native or distributed E2E tests. */
class TaskAdmissionRegistryTest {
    private static final Clock NOW = Clock.fixed(Instant.ofEpochMilli(1000), ZoneOffset.UTC);

    @Test
    void duplicateWaitsForActualTaskPublication() throws Exception {
        var registry = new TaskAdmissionRegistry(2, NOW);
        var original = registry.reserve(request("task", 2000), "worker-task");
        var duplicate = registry.reserve(request("task", 2000), "unused-worker-task");
        assertThat(original.first()).isTrue();
        assertThat(duplicate.first()).isFalse();
        var executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "admission-wait-test"));
        try {
            var waiting = executor.submit(duplicate::awaitAdmission);
            assertThat(waiting).isNotDone();
            original.publish(() -> {});
            assertThat(waiting).isNotDone();
            original.admitted();
            assertThat(waiting.get(10, TimeUnit.SECONDS)).isEqualTo("worker-task");
            assertThat(registry.resolve("task")).isEqualTo("worker-task");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentDuplicatesElectExactlyOneOwner() throws Exception {
        var registry = new TaskAdmissionRegistry(2, NOW);
        var barrier = new CyclicBarrier(4);
        var executor = Executors.newFixedThreadPool(4, r -> new Thread(r, "admission-race-test"));
        try {
            var futures = java.util.stream.IntStream.range(0, 4)
                    .mapToObj(index -> executor.submit(() -> {
                        barrier.await(10, TimeUnit.SECONDS);
                        return registry.reserve(request("same-task", 2000), "worker-" + index);
                    })).toList();
            var reservations = new java.util.ArrayList<TaskAdmissionRegistry.Reservation>();
            for (var future : futures) reservations.add(future.get(10, TimeUnit.SECONDS));
            assertThat(reservations.stream().filter(TaskAdmissionRegistry.Reservation::first).count()).isEqualTo(1);
            var winner = reservations.stream().filter(TaskAdmissionRegistry.Reservation::first).findFirst().orElseThrow();
            winner.publish(() -> {});
            winner.admitted();
            String actual = winner.awaitAdmission();
            for (var reservation : reservations) assertThat(reservation.awaitAdmission()).isEqualTo(actual);
            assertThat(registry.size()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sameIdentityCannotChangePayloadOrAttempt() {
        var registry = new TaskAdmissionRegistry(2, NOW);
        var original = request("task", 2000);
        registry.reserve(original, "worker-task");
        var changedPayload = original.toBuilder().getResourceGrantBuilder().setNativeMemoryBytes(99).build();
        assertConflict(() -> registry.reserve(original.toBuilder().setResourceGrant(changedPayload).build(), "other"));
        assertConflict(() -> registry.reserve(original.toBuilder()
                .setAttempt(original.getAttempt().toBuilder().setAttemptId(2)).build(), "other"));
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void protobufMapInsertionOrderDoesNotChangeCanonicalIdentity() {
        var registry = new TaskAdmissionRegistry(2, NOW);
        var first = request("task", 2000).toBuilder();
        first.getExecutionOptionsBuilder().putSessionProperties("alpha", "a").putSessionProperties("beta", "b");
        var second = request("task", 2000).toBuilder();
        second.getExecutionOptionsBuilder().putSessionProperties("beta", "b").putSessionProperties("alpha", "a");
        assertThat(registry.reserve(first.build(), "worker-task").first()).isTrue();
        assertThat(registry.reserve(second.build(), "unused").first()).isFalse();
    }

    @Test
    void expiredRequestIsRejectedBeforeRegistryMutation() {
        var registry = new TaskAdmissionRegistry(2, NOW);
        for (long deadline : List.of(0L, -1L, 999L, 1000L)) {
            assertThatThrownBy(() -> registry.reserve(request("task", deadline), "worker-task"))
                    .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("deadline has expired");
        }
        assertThat(registry.size()).isZero();
    }

    @Test
    void deadlineReachedBeforePublicationPreventsItsSideEffects() {
        var clock = new AdmissionClock(1000);
        var registry = new TaskAdmissionRegistry(1, clock);
        var reservation = registry.reserve(request("task", 2000), "worker-task");
        clock.time.set(2000);
        assertThatThrownBy(() -> reservation.publish(() -> {
            throw new AssertionError("Expired admission must not publish resources");
        })).isInstanceOf(TaskAdmissionException.class).hasMessageContaining("deadline has expired");
        assertThat(registry.resolve("task")).isNull();
    }

    @Test
    void deadlineReachedAfterPublicationPreventsExecutionHandoff() {
        var clock = new AdmissionClock(1000);
        var registry = new TaskAdmissionRegistry(1, clock);
        var task = request("task", 2000);
        var reservation = registry.reserve(task, "worker-task");
        reservation.publish(() -> {});
        clock.time.set(2000);
        assertThatThrownBy(reservation::admitted).isInstanceOf(TaskAdmissionException.class)
                .hasMessageContaining("deadline has expired");
        assertThat(registry.resolve("task")).isNull();
        assertThat(registry.seal(sealRequest(task), true).noOwnerExited()).isFalse();
        reservation.rejectedBeforeOwnership(new IllegalStateException("Resources actually refunded"));
        assertThat(registry.seal(sealRequest(task), true).noOwnerExited()).isTrue();
    }

    @Test
    void liveIdentityIsNotEvictedWhenCapacityIsReached() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var first = registry.reserve(request("task", 2000), "worker-task");
        first.publish(() -> {});
        first.admitted();
        assertThatThrownBy(() -> registry.reserve(request("another-task", 2000), "other"))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("identity limit reached");
        registry.removeExpired("task", "worker-task");
        assertThat(registry.resolve("task")).isEqualTo("worker-task");
    }

    @Test
    void failedReservationPropagatesOriginalFailureAndLeavesNoOwner() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var first = registry.reserve(request("task", 2000), "worker-task");
        var duplicate = registry.reserve(request("task", 2000), "unused");
        var failure = new IllegalStateException("actual pre-owner reservation failure");
        first.rejectedBeforeOwnership(failure);
        assertThatThrownBy(duplicate::awaitAdmission).isSameAs(failure);
        assertThat(registry.size()).isZero();
        assertThat(registry.reserve(request("task", 2000), "retry").first()).isTrue();
    }

    @Test
    void closedRegistryCannotPublishOrAcceptAReservation() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        registry.sealAndAwaitAdmissions();
        assertThatThrownBy(() -> registry.reserve(request("task", 2000), "worker-task"))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("admission is closed");
        assertThat(registry.size()).isZero();
    }

    @Test
    void sealBeforeSubmissionPreventsLateAdmission() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var task = request("task", 2000);
        var actual = registry.seal(sealRequest(task), false);
        assertThat(actual.noOwnerExited()).isTrue();
        assertThat(actual.workerTaskId()).isEmpty();
        assertThat(actual.receipt()).isEmpty();
        assertThatThrownBy(() -> registry.reserve(task, "late-worker-task"))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("admission is sealed");
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void pendingSealCannotProveExitUntilPreOwnerRollbackActuallyCompletes() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var task = request("task", 2000);
        var reserved = registry.reserve(task, "worker-task");
        assertThat(registry.seal(sealRequest(task), true).noOwnerExited()).isFalse();
        assertThatThrownBy(() -> reserved.publish(() -> {
            throw new AssertionError("A sealed pending admission must not publish TaskInfo");
        })).isInstanceOf(TaskAdmissionException.class).hasMessageContaining("admission is sealed");
        assertThat(registry.seal(sealRequest(task), true).noOwnerExited()).isFalse();
        reserved.rejectedBeforeOwnership(new IllegalStateException("pre-owner resources actually refunded"));
        assertThat(registry.seal(sealRequest(task), true).noOwnerExited()).isTrue();
        assertThat(registry.size()).isEqualTo(1);
        assertThatThrownBy(() -> registry.reserve(task, "replay"))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("admission is sealed");
    }

    @Test
    void expiredForgottenIdentityDoesNotManufactureHistoricalNoOwnerProof() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var result = registry.seal(sealRequest(request("forgotten", 999)), false);
        assertThat(result.noOwnerExited()).isFalse();
        assertThat(result.receipt()).isEmpty();
        assertThat(registry.size()).isZero();
    }

    @Test
    void sealMustMatchBothAttemptAndOriginalCanonicalPayload() {
        var registry = new TaskAdmissionRegistry(1, NOW);
        var task = request("task", 2000);
        registry.reserve(task, "worker-task");
        var seal = sealRequest(task);
        assertThatThrownBy(() -> registry.seal(seal.toBuilder()
                .setAttempt(seal.getAttempt().toBuilder().setAttemptId(9)).build(), true))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("issued task");
        assertThatThrownBy(() -> registry.seal(seal.toBuilder().setDeadlineEpochMs(3000).build(), true))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("issued task");
        assertThat(registry.reserve(task, "duplicate").first()).isFalse();
    }

    private static org.pangrova.protocol.v2.TaskAdmissionSealRequest sealRequest(TaskRequest request) {
        return org.pangrova.protocol.v2.TaskAdmissionSealRequest.newBuilder()
                .setAttempt(request.getAttempt()).setDeadlineEpochMs(request.getDeadlineEpochMs())
                .setCanonicalRequestChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.digest(request))))
                .build();
    }

    private static void assertConflict(org.assertj.core.api.ThrowableAssert.ThrowingCallable operation) {
        assertThatThrownBy(operation).isInstanceOf(TaskAdmissionException.class)
                .hasMessageContaining("different request");
    }

    private static TaskRequest request(String taskId, long deadline) {
        return TaskRequest.newBuilder().setDeadlineEpochMs(deadline)
                .setAttempt(TaskAttemptId.newBuilder().setTaskId(taskId)).build();
    }

    private static final class AdmissionClock extends Clock {
        private final java.util.concurrent.atomic.AtomicLong time;
        private final ZoneId zone;

        private AdmissionClock(long time) {
            this(new java.util.concurrent.atomic.AtomicLong(time), ZoneOffset.UTC);
        }

        private AdmissionClock(java.util.concurrent.atomic.AtomicLong time, ZoneId zone) {
            this.time = time;
            this.zone = zone;
        }

        @Override public ZoneId getZone() { return zone; }
        @Override public Clock withZone(ZoneId newZone) {
            return new AdmissionClock(time, java.util.Objects.requireNonNull(newZone));
        }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
        @Override public long millis() { return time.get(); }
    }
    @Test
    void membershipFenceClosesReservationAndPendingPublicationWithoutClaimingDrain() {
        var registry = new TaskAdmissionRegistry(4, NOW);
        var request = request("fenced-task", 2000);
        var pending = registry.reserve(request, "worker-task");
        registry.closeAdmissions();
        registry.closeAdmissions();
        var published = new java.util.concurrent.atomic.AtomicBoolean();
        assertThatThrownBy(() -> registry.reserve(request("new-task", 2000), "new-worker-task"))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("closed");
        assertThatThrownBy(() -> pending.publish(() -> published.set(true)))
                .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("closed");
        assertThat(published).isFalse();
        pending.rejectedBeforeOwnership(new IllegalStateException("membership fenced"));
        registry.sealAndAwaitAdmissions();
    }

}
