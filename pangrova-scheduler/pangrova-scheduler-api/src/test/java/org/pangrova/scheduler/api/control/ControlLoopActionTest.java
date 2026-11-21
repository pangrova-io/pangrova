package org.pangrova.scheduler.api.control;

import org.pangrova.scheduler.api.enums.ControlLoopAction;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ControlLoopAction} enum.
 */
class ControlLoopActionTest {

    @Test
    void allActionsArePresent() {
        assertThat(ControlLoopAction.values())
                .containsExactly(
                        ControlLoopAction.REBALANCE,
                        ControlLoopAction.SCALE_UP,
                        ControlLoopAction.SCALE_DOWN,
                        ControlLoopAction.MIGRATE,
                        ControlLoopAction.KILL_QUERY);
    }

    @Test
    void valueOfRoundTrips() {
        for (ControlLoopAction action : ControlLoopAction.values()) {
            assertThat(ControlLoopAction.valueOf(action.name())).isEqualTo(action);
        }
    }

    @Test
    void rebalanceIsFirstAction() {
        assertThat(ControlLoopAction.REBALANCE.ordinal()).isZero();
    }
}
