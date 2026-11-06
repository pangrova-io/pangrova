package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.lifecycle.QueryState;

import org.junit.jupiter.api.Test;
import org.pangrova.query.core.lifecycle.QueryStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DML waits for the logical commit owner, which survives physical attempt retirement. */
class QuerySchedulingEngineWriteCompletionTest {
    @Test
    void successfulWriteWaitsForLogicalTerminalState() {
        var query = runningQuery();
        assertThat(query.terminalStateReady()).isNotDone();

        assertThat(query.tryTransition(QueryState.FINISHING)).isTrue();
        assertThat(query.terminalStateReady()).isNotDone();
        assertThat(query.tryTransition(QueryState.FINISHED)).isTrue();

        assertThatCode(() -> QuerySchedulingEngine.awaitWriteCompletion(
                query, "write-query", "retired-original-attempt", 1_000))
                .doesNotThrowAnyException();
        assertThat(query.terminalStateReady()).isCompletedWithValue(QueryState.FINISHED);
    }

    @Test
    void failedLogicalWriteCannotBeAcceptedAsPhysicalCopySuccess() {
        var query = runningQuery();
        assertThat(query.tryTransition(QueryState.FAILED)).isTrue();

        assertThatThrownBy(() -> QuerySchedulingEngine.awaitWriteCompletion(
                query, "write-query", "retired-original-attempt", 1_000))
                .hasMessageContaining("state=FAILED")
                .hasMessageContaining("queryId=write-query");
    }

    private static QueryStateMachine runningQuery() {
        var query = new QueryStateMachine();
        assertThat(query.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(query.tryTransition(QueryState.SCHEDULING)).isTrue();
        assertThat(query.tryTransition(QueryState.RUNNING)).isTrue();
        return query;
    }
}
