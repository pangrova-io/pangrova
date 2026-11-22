package org.pangrova.query.lifecycle;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.common.client.result.ResultEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

/** Observers may stop waiting, but cannot manufacture readiness for another caller. */
class QueryStateMachineReadinessTest {
    @Test
    void resultObserverCannotPublishAnEndpointOrCancelOtherObservers() {
        var state = new QueryStateMachine();
        var fake = new ResultEndpoint("unissued", 1, "fake", "flight");
        var actual = new ResultEndpoint("worker", 2, "issued", "flight");
        state.resultEndpointReady().complete(fake);
        state.resultEndpointReady().cancel(false);
        var waiting = state.resultEndpointReady();
        assertThat(waiting).isNotDone();
        assertThat(state.getResultEndpoint()).isEmpty();
        state.setResultEndpoint(actual);
        assertThat(waiting).isNotDone();
        running(state);
        assertThat(waiting).isCompletedWithValue(actual);
        assertThat(state.resultEndpointReady()).isCompletedWithValue(actual);
    }

    @Test
    void writeObserverCannotPublishDispatchOrCancelOtherObservers() {
        var state = new QueryStateMachine();
        state.writeDispatchReady().complete(null);
        state.writeDispatchReady().cancel(false);
        var waiting = state.writeDispatchReady();
        assertThat(state.isWriteDispatchReady()).isFalse();
        assertThat(waiting).isNotDone();
        state.markWriteDispatchReady();
        assertThat(waiting).isNotDone();
        running(state);
        assertThat(waiting).isNotDone();
        state.markWriteDispatchReady();
        assertThat(waiting).isCompletedWithValue(null);
        assertThat(state.isWriteDispatchReady()).isTrue();
    }

    @Test
    void statusWaitersObserveEndpointAndActualDrainOnSeparateVersions() {
        var state = new QueryStateMachine();
        running(state);
        var initial = state.status();

        var abandoned = state.awaitStatus(QueryWaitTarget.NEXT_VERSION, initial.version());
        var waiting = state.awaitStatus(QueryWaitTarget.NEXT_VERSION, initial.version());
        assertThat(abandoned.cancel(false)).isTrue();
        var endpoint = new ResultEndpoint("worker", 2, "issued", "flight");
        state.setResultEndpoint(endpoint);
        assertThat(waiting).isCompletedWithValue(
                new QueryStateMachine.Status(initial.version() + 1, QueryState.RUNNING, null, endpoint, false));
        assertThat(state.awaitStatus(QueryWaitTarget.NEXT_VERSION, initial.version()))
                .isCompletedWithValue(state.status());

        state.tryTransition(QueryState.FINISHING);
        state.tryTransition(QueryState.FINISHED);
        var terminal = state.status();
        assertThat(terminal.drained()).isFalse();
        var drained = state.awaitStatus(QueryWaitTarget.NEXT_VERSION, terminal.version());
        assertThat(state.claimPhysicalCleanup()).isTrue();
        assertThat(state.claimPhysicalCleanup()).isFalse();
        assertThat(drained).isCompletedWithValue(
                new QueryStateMachine.Status(terminal.version() + 1, QueryState.FINISHED, null, endpoint, true));
    }

    @Test
    void eventWaitsIgnoreIntermediateVersionsAndRetainPhysicalRelease() {
        var state = new QueryStateMachine();
        var ready = state.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, 0);
        var completed = state.awaitStatus(QueryWaitTarget.TERMINAL, 0);
        var released = state.awaitStatus(QueryWaitTarget.DRAINED, 0);
        running(state);
        assertThat(ready).isNotDone();
        assertThat(completed).isNotDone();
        assertThat(released).isNotDone();

        state.setResultEndpoint(new ResultEndpoint("worker", 2, "issued", "flight"));
        assertThat(ready).isCompletedWithValue(state.status());
        assertThat(completed).isNotDone();
        assertThat(released).isNotDone();

        state.tryTransition(QueryState.FINISHING);
        assertThat(completed).isNotDone();
        state.tryTransition(QueryState.FINISHED);
        assertThat(completed).isCompletedWithValue(state.status());
        assertThat(released).isNotDone();
        assertThat(state.awaitStatus(QueryWaitTarget.TERMINAL, state.status().version()))
                .isCompletedWithValue(state.status());
        state.claimPhysicalCleanup();
        assertThat(released).isCompletedWithValue(state.status());
    }

    private static void running(QueryStateMachine state) {
        assertThat(state.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(state.tryTransition(QueryState.SCHEDULING)).isTrue();
        assertThat(state.tryTransition(QueryState.RUNNING)).isTrue();
    }
}
