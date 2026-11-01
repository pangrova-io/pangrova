package org.pangrova.scheduler.speculation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Routing metadata retirement boundaries only, not speculative execution or a drain receipt substitute. */
class SpeculativeAttemptPairTest {
    @Test
    void localCleanupCannotRetireAnInFlightDispatchTransaction() {
        var pair = new SpeculativeAttemptPair("original", "copy");
        pair.cleanupRequested = true;
        assertThat(pair.readyToRetire()).isFalse();
        pair.dispatching = false;
        assertThat(pair.readyToRetire()).isTrue();
    }

    @Test
    void issuedCopyRequiresExactDrainRatherThanAbortedOrCleanupFlags() {
        var pair = new SpeculativeAttemptPair("original", "copy");
        pair.copyIssued = true;
        pair.dispatching = false;
        pair.cleanupRequested = true;
        pair.aborted = true;
        assertThat(pair.readyToRetire()).isFalse();
        pair.copyDrained = true;
        assertThat(pair.readyToRetire()).isTrue();
    }

    @Test
    void ordinaryWinnerPairNeedsBothAttemptsDrainedBeforeMetadataRetirement() {
        var pair = new SpeculativeAttemptPair("original", "copy");
        pair.copyIssued = true;
        pair.dispatching = false;
        pair.cleanupRequested = true;
        pair.copyDrained = true;
        assertThat(pair.readyToRetire()).isFalse();
        pair.originalDrained = true;
        assertThat(pair.readyToRetire()).isTrue();
    }
}
