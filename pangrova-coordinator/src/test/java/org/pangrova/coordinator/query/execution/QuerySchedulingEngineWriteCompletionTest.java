package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.QueryStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DML waits for the logical commit owner, which survives physical attempt retirement. */
class QuerySchedulingEngineWriteCompletionTest {
    @Test
    void successfulWriteWaitsForLogicalTerminalState() {
        var query = runningQuery();
        assertThat(query.terminalStateReady()).isNotDone();

        assertThat(query.tryTransition(QueryStateMachine.State.FINISHING)).isTrue();
        assertThat(query.terminalStateReady()).isNotDone();
        assertThat(query.tryTransition(QueryStateMachine.State.FINISHED)).isTrue();

        assertThatCode(() -> QuerySchedulingEngine.awaitWriteCompletion(
                query, "write-query", "retired-original-attempt", 1_000))
                .doesNotThrowAnyException();
        assertThat(query.terminalStateReady()).isCompletedWithValue(QueryStateMachine.State.FINISHED);
    }

    @Test
    void failedLogicalWriteCannotBeAcceptedAsPhysicalCopySuccess() {
        var query = runningQuery();
        assertThat(query.tryTransition(QueryStateMachine.State.FAILED)).isTrue();

        assertThatThrownBy(() -> QuerySchedulingEngine.awaitWriteCompletion(
                query, "write-query", "retired-original-attempt", 1_000))
                .hasMessageContaining("state=FAILED")
                .hasMessageContaining("queryId=write-query");
    }

    private static QueryStateMachine runningQuery() {
        var query = new QueryStateMachine();
        assertThat(query.tryTransition(QueryStateMachine.State.PLANNING)).isTrue();
        assertThat(query.tryTransition(QueryStateMachine.State.SCHEDULING)).isTrue();
        assertThat(query.tryTransition(QueryStateMachine.State.RUNNING)).isTrue();
        return query;
    }
}
