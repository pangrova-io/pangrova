package org.pangrova.worker.task;

import org.pangrova.worker.exception.ExecutionOwnerFenceException;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionOwnerFenceTest {

    @Test
    void equalTokenRequiresExactOwnerIncarnation() {
        var fence = new ExecutionOwnerFence();
        fence.verify("query-1", identity("a", "a-1", 10));

        assertThatCode(() -> fence.verify(
                "query-1", identity("a", "a-1", 10))).doesNotThrowAnyException();
        assertThatThrownBy(() -> fence.verify(
                "query-1", identity("a", "a-2", 10)))
                .isInstanceOf(ExecutionOwnerFenceException.class);
    }

    @Test
    void higherTokenSupersedesOwnerAndRejectsDelayedOldTask() {
        var fence = new ExecutionOwnerFence();
        fence.verify("query-1", identity("a", "a-1", 10));
        fence.verify("query-1", identity("b", "b-1", 11));

        assertThatThrownBy(() -> fence.verify(
                "query-1", identity("a", "a-1", 10)))
                .isInstanceOf(ExecutionOwnerFenceException.class);
        assertThatCode(() -> fence.verify(
                "query-1", identity("b", "b-1", 11))).doesNotThrowAnyException();
    }

    @Test
    void differentExecutionsHaveIndependentFencingSequences() {
        var fence = new ExecutionOwnerFence();
        fence.verify("query-1", identity("a", "a-1", 100));

        assertThatCode(() -> fence.verify(
                "query-2", identity("b", "b-1", 1))).doesNotThrowAnyException();
    }

    private static ExecutionOwnerIdentity identity(
            String ownerId, String incarnation, long token) {
        return new ExecutionOwnerIdentity(ownerId, incarnation, token);
    }
}
