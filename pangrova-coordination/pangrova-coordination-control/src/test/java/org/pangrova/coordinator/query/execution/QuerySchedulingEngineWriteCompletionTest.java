package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.enums.QueryState;

import org.junit.jupiter.api.Test;
import org.pangrova.query.lifecycle.QueryStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DML waits for the logical commit owner, which survives physical attempt retirement. */
class QuerySchedulingEngineWriteCompletionTest {
    @Test
    void successfulWriteWaitsForLogicalTerminalState() {
        var query = runningQuery();
        var commit = QuerySchedulingEngine.requireWriteCompletion(query, "write-query", "retired-original-attempt");
        assertThat(commit).isNotDone();

        assertThat(query.tryTransition(QueryState.FINISHING)).isTrue();
        assertThat(commit).isNotDone();
        assertThat(query.tryTransition(QueryState.FINISHED)).isTrue();

        assertThat(commit).isCompletedWithValue(QueryState.FINISHED);
    }

    @Test
    void failedLogicalWriteCannotBeAcceptedAsPhysicalCopySuccess() {
        var query = runningQuery();
        assertThat(query.tryTransition(QueryState.FAILED)).isTrue();

        assertThatThrownBy(() -> QuerySchedulingEngine.requireWriteCompletion(
                query, "write-query", "retired-original-attempt").join())
                .hasMessageContaining("state=FAILED")
                .hasMessageContaining("queryId=write-query");
    }

    @Test
    void controlPlaneWriteFollowsTheCommitSignalBeforePublicQueryTermination() {
        var query = runningQuery();
        query.deferTerminalUntilControlPlaneResult();
        var commit = QuerySchedulingEngine.requireWriteCompletion(query, "write-query", "original-attempt");
        assertThat(commit).isNotDone();

        assertThat(query.signalNativeWriteCompletedForControlPlane()).isTrue();
        assertThat(commit).isCompletedWithValue(QueryState.FINISHED);
        assertThat(query.terminalStateReady()).isNotDone();
        assertThat(query.getState()).isEqualTo(QueryState.RUNNING);
    }

    private static QueryStateMachine runningQuery() {
        var query = new QueryStateMachine();
        assertThat(query.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(query.tryTransition(QueryState.SCHEDULING)).isTrue();
        assertThat(query.tryTransition(QueryState.RUNNING)).isTrue();
        return query;
    }
}
