package org.pangrova.coordinator.query;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.error.ErrorDetail;
import static org.assertj.core.api.Assertions.assertThat;

/** Logical transition contracts; physical retry/resource oracles live in the real owner suites. */
class QueryManagerOnAllTasksFinishedTest {
    private QueryTerminalControlFixture fixture;

    @BeforeEach
    void createActualControlOwners() { fixture = new QueryTerminalControlFixture(null); }

    @AfterEach
    void closeActualControlOwners() { fixture.close(); }

    @Test
    void onAllTasksFinished_normalPath_transitionsToFinished() {
        String id = fixture.registerRunningQuery();
        fixture.manager.onAllTasksFinished(id);
        assertThat(fixture.manager.getQueryState(id)).contains(QueryStateMachine.State.FINISHED);
    }

    @Test
    void heldNativeWriteSignalsCommitWithoutFinishingOuterControlPlaneStatement() {
        String id = fixture.registerRunningQuery();
        var state = fixture.manager.getStateMachine(id);
        state.deferTerminalUntilControlPlaneResult();

        fixture.manager.onAllTasksFinished(id);

        assertThat(state.nativeWriteCompletionReady().getNow(null))
                .isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(state.terminalStateReady()).isNotDone();
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.RUNNING);
        state.toFinisher().run();
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        fixture.manager.onAllTasksFinished(id);
    }

    @Test
    void heldNativeWriteFailureWakesOuterStatementWithoutReportingSuccess() {
        String id = fixture.registerRunningQuery();
        var state = fixture.manager.getStateMachine(id);
        state.deferTerminalUntilControlPlaneResult();

        fixture.manager.onTaskFailed(id, new ErrorDetail("RUNTIME_INTERNAL", "execution", "write failed"));

        assertThat(state.nativeWriteCompletionReady().getNow(null))
                .isEqualTo(QueryStateMachine.State.FAILED);
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.FAILED);
    }

    @Test
    void onAllTasksFinished_alreadyFailed_bailsOut() {
        String id = fixture.registerRunningQuery();
        fixture.manager.onTaskFailed(id, new ErrorDetail("RUNTIME_INTERNAL", "execution", "task crashed"));
        assertThat(fixture.manager.getQueryState(id)).contains(QueryStateMachine.State.FAILED);
        fixture.manager.onAllTasksFinished(id);
        assertThat(fixture.manager.getQueryState(id)).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void onAllTasksFinished_alreadyCancelled_bailsOut() {
        String id = fixture.registerRunningQuery();
        assertThat(fixture.manager.cancelQuery(id)).isTrue();
        assertThat(fixture.manager.getQueryState(id)).contains(QueryStateMachine.State.CANCELLED);
        fixture.manager.onAllTasksFinished(id);
        assertThat(fixture.manager.getQueryState(id)).contains(QueryStateMachine.State.CANCELLED);
    }

    @Test
    void onAllTasksFinished_unknownQuery_noOp() {
        String id = "unknown-" + UUID.randomUUID();
        fixture.manager.onAllTasksFinished(id);
        assertThat(fixture.manager.getQueryState(id)).isEmpty();
    }

    @Test
    void onAllTasksFinished_catchesUpFromQueued() {
        String id = "queued-" + UUID.randomUUID();
        var state = new QueryStateMachine();
        fixture.manager.registerStateMachine(id, state);
        fixture.manager.onAllTasksFinished(id);
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.QUEUED);
        assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
    }
}
