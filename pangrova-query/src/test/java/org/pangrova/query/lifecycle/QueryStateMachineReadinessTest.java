package org.pangrova.query.lifecycle;

import org.junit.jupiter.api.Test;
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

    private static void running(QueryStateMachine state) {
        assertThat(state.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(state.tryTransition(QueryState.SCHEDULING)).isTrue();
        assertThat(state.tryTransition(QueryState.RUNNING)).isTrue();
    }
}
