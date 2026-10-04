package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Cleanup authority is read-only and distinct from admitting an old or future attempt. */
class ControlEpochCleanupFenceTest {
    @Test
    void knownOldAttemptCanExitAfterControlPlaneHandoffButCannotBeReadmitted() {
        var fence = new ControlEpochFence();
        fence.requireHandshake();
        fence.observe(7);
        fence.observe(8);
        assertThatCode(() -> fence.verifyExistingCleanup(7L)).doesNotThrowAnyException();
        assertThatThrownBy(() -> fence.verify(7L)).isInstanceOf(ControlEpochFenceException.class)
                .satisfies(error -> org.assertj.core.api.Assertions.assertThat(
                        ((ControlEpochFenceException) error).reason())
                        .isEqualTo(ControlEpochFenceException.Reason.EPOCH_MISMATCH));
        org.assertj.core.api.Assertions.assertThat(fence.currentEpoch()).hasValue(8);
    }

    @Test
    void cleanupCannotGuessAnUnobservedFutureControlEpoch() {
        var fence = new ControlEpochFence();
        fence.requireHandshake();
        fence.observe(7);
        assertThatThrownBy(() -> fence.verifyExistingCleanup(8L))
                .isInstanceOf(ControlEpochFenceException.class)
                .satisfies(error -> org.assertj.core.api.Assertions.assertThat(
                        ((ControlEpochFenceException) error).reason())
                        .isEqualTo(ControlEpochFenceException.Reason.EPOCH_AHEAD));
        org.assertj.core.api.Assertions.assertThat(fence.currentEpoch()).hasValue(7);
    }

    @Test
    void requiredHandshakeMustActuallyCompleteBeforeCleanup() {
        var fence = new ControlEpochFence();
        fence.requireHandshake();
        assertThatThrownBy(() -> fence.verifyExistingCleanup(1L))
                .isInstanceOf(ControlEpochFenceException.class)
                .satisfies(error -> org.assertj.core.api.Assertions.assertThat(
                        ((ControlEpochFenceException) error).reason())
                        .isEqualTo(ControlEpochFenceException.Reason.CONTROL_PLANE_NOT_READY));
    }

    @Test
    void cleanupDoesNotAcceptMissingOrInvalidExactIdentityEpoch() {
        var fence = new ControlEpochFence();
        fence.requireHandshake();
        fence.observe(7);
        for (Long epoch : new Long[]{null, 0L, -1L}) {
            assertThatThrownBy(() -> fence.verifyExistingCleanup(epoch))
                    .isInstanceOf(ControlEpochFenceException.class)
                    .satisfies(error -> org.assertj.core.api.Assertions.assertThat(
                            ((ControlEpochFenceException) error).reason())
                            .isEqualTo(ControlEpochFenceException.Reason.INVALID_EPOCH));
        }
    }

    @Test
    void epochObservationCannotBeRolledBackByCleanup() {
        var fence = new ControlEpochFence();
        fence.requireHandshake();
        fence.observe(7);
        fence.observe(9);
        fence.observe(8);
        assertThatCode(() -> fence.verifyExistingCleanup(7L)).doesNotThrowAnyException();
        assertThatCode(() -> fence.verify(9L)).doesNotThrowAnyException();
        org.assertj.core.api.Assertions.assertThat(fence.currentEpoch()).hasValue(9);
    }
}
