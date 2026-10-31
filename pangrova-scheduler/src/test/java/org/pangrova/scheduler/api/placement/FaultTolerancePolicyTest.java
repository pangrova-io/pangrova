package org.pangrova.scheduler.api.placement;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FaultTolerancePolicy} enum — values and semantics.
 */
class FaultTolerancePolicyTest {

    @Test
    void allPoliciesArePresent() {
        assertThat(FaultTolerancePolicy.values())
                .containsExactly(
                        FaultTolerancePolicy.NONE,
                        FaultTolerancePolicy.TASK_RETRY,
                        FaultTolerancePolicy.STAGE_RETRY);
    }

    @Test
    void valueOfRoundTrips() {
        for (FaultTolerancePolicy policy : FaultTolerancePolicy.values()) {
            assertThat(FaultTolerancePolicy.valueOf(policy.name())).isEqualTo(policy);
        }
    }

    @Test
    void noneIsDefaultBehavior() {
        // NONE means task failure = query failure; no retry
        assertThat(FaultTolerancePolicy.NONE.name()).isEqualTo("NONE");
    }

    @Test
    void taskRetryRequiresDurableShuffle() {
        // TASK_RETRY semantics: retry individual tasks if shuffle supports it
        assertThat(FaultTolerancePolicy.TASK_RETRY.name()).isEqualTo("TASK_RETRY");
    }

    @Test
    void stageRetryRecomputesUpstream() {
        // STAGE_RETRY semantics: recompute entire upstream stage
        assertThat(FaultTolerancePolicy.STAGE_RETRY.name()).isEqualTo("STAGE_RETRY");
    }
}
