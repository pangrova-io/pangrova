package org.pangrova.spi.execution;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryProgressTest {
    @Test
    void cancellationStopsProgressPollingWithoutBecomingFailure() {
        QueryProgress progress = QueryProgress.of(new QueryId("cancelled"), QueryState.CANCELLED);
        assertThat(progress.isTerminal()).isTrue();
        assertThat(progress.state()).isEqualTo(QueryState.CANCELLED);
        assertThat(progress.failureMessage()).isEmpty();
    }

    @Test
    void onlyTerminalStatesStopProgressPolling() {
        for (QueryState state : QueryState.values()) {
            assertThat(QueryProgress.of(new QueryId(state.name()), state).isTerminal())
                    .as("terminal state %s", state)
                    .isEqualTo(state == QueryState.FINISHED || state == QueryState.FAILED
                            || state == QueryState.CANCELLED);
        }
    }
}
