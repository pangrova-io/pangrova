package org.pangrova.query.lifecycle;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.error.QueryFailure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Observers may stop waiting, but cannot manufacture readiness for another caller. */
class QueryStateMachineReadinessTest {
    @Test
    void resultObserverCannotPublishAnEndpointOrCancelOtherObservers() {
        var state = new QueryStateMachine();
        var fake = new ResultEndpoint("unissued", 1, "fake", "grpc");
        var actual = new ResultEndpoint("worker", 2, "issued", "grpc");
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
        var endpoint = new ResultEndpoint("worker", 2, "issued", "grpc");
        state.setResultEndpoint(endpoint);
        assertThat(waiting).isCompletedWithValue(
                new QueryStateMachine.Status(initial.version() + 1, QueryState.RUNNING, null,
                        ResultDisposition.STREAM, endpoint, false));
        assertThat(state.awaitStatus(QueryWaitTarget.NEXT_VERSION, initial.version()))
                .isCompletedWithValue(state.status());

        state.tryTransition(QueryState.FINISHING);
        state.tryTransition(QueryState.FINISHED);
        var terminal = state.status();
        assertThat(terminal.drained()).isFalse();
        var drained = state.awaitStatus(QueryWaitTarget.NEXT_VERSION, terminal.version());
        assertThat(state.claimPhysicalCleanup(true)).isTrue();
        assertThat(state.claimPhysicalCleanup(true)).isFalse();
        assertThat(drained).isCompletedWithValue(
                new QueryStateMachine.Status(terminal.version() + 1, QueryState.FINISHED, null,
                        ResultDisposition.STREAM, endpoint, true));
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

        state.setResultEndpoint(new ResultEndpoint("worker", 2, "issued", "grpc"));
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
        state.claimPhysicalCleanup(true);
        assertThat(released).isCompletedWithValue(state.status());
    }

    @Test
    void confirmedRetirementWakesDrainObserversAfterLocalCleanup() {
        var state = new QueryStateMachine();
        running(state);
        state.tryTransition(QueryState.FINISHING);
        state.tryTransition(QueryState.FINISHED);
        var terminal = state.status();
        var waiter = state.awaitStatus(QueryWaitTarget.DRAINED, terminal.version());
        state.refreshPhysicalRetirement(() -> { throw new AssertionError("Query is locally active"); });
        assertThat(state.claimPhysicalCleanup(false)).isTrue();
        assertThat(state.physicalCleanupApplied()).isTrue();
        assertThat(state.status()).isEqualTo(terminal);
        state.refreshPhysicalRetirement(() -> false);
        assertThat(waiter).isNotDone();
        state.refreshPhysicalRetirement(() -> true);
        assertThat(waiter).isCompletedWithValue(state.status());
        assertThat(state.status().version()).isEqualTo(terminal.version() + 1);
        assertThat(state.status().drained()).isTrue();
        state.refreshPhysicalRetirement(() -> { throw new AssertionError("Query was already retired"); });
    }

    @Test
    void initialStatusIsPendingAndDoesNotReleaseResultObservers() {
        var state = new QueryStateMachine();
        var initial = state.status();
        assertThat(initial.version()).isZero();
        assertThat(initial.state()).isEqualTo(QueryState.QUEUED);
        assertThat(initial.disposition()).isEqualTo(ResultDisposition.PENDING);
        assertThat(initial.endpoint()).isNull();
        assertThat(state.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, initial.version())).isNotDone();
    }

    @Test
    void explicitNoResultWaitsForTerminalCompletion() {
        var state = new QueryStateMachine();
        running(state);
        var ready = state.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, state.status().version());
        state.markNoResult();
        var noResult = state.status();
        assertThat(noResult.disposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(noResult.endpoint()).isNull();
        assertThat(ready).isNotDone();

        state.tryTransition(QueryState.FINISHING);
        state.tryTransition(QueryState.FINISHED);
        assertThat(ready).isCompletedWithValue(state.status());
        assertThat(state.status().disposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(state.getResultEndpoint()).isEmpty();
    }

    @Test
    void delayedEndpointPublicationAtomicallyChangesPendingToStream() {
        var state = new QueryStateMachine();
        running(state);
        state.tryTransition(QueryState.FINISHING);
        var before = state.status();
        var ready = state.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, before.version());
        assertThat(before.disposition()).isEqualTo(ResultDisposition.PENDING);
        assertThat(ready).isNotDone();

        var endpoint = new ResultEndpoint("worker", 3, "delayed", "grpc");
        state.setResultEndpoint(endpoint);
        var published = state.status();
        assertThat(published.version()).isEqualTo(before.version() + 1);
        assertThat(published.disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(published.endpoint()).isSameAs(endpoint);
        assertThat(ready).isCompletedWithValue(published);

        state.tryTransition(QueryState.FINISHED);
        assertThat(state.status().disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(state.status().endpoint()).isSameAs(endpoint);
    }

    @Test
    void resultDispositionCannotCrossBetweenNoneAndStream() {
        var noResult = new QueryStateMachine();
        noResult.markNoResult();
        assertThatThrownBy(() -> noResult.setResultEndpoint(
                new ResultEndpoint("worker", 3, "late", "grpc")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("resultless");

        var streaming = new QueryStateMachine();
        streaming.setResultEndpoint(new ResultEndpoint("worker", 3, "issued", "grpc"));
        assertThatThrownBy(streaming::markNoResult)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("streaming");
    }

    @Test
    void unfinishedPendingBecomesNoneOnEveryTerminalPath() {
        assertTerminalWithoutEndpoint(QueryState.FINISHED);
        assertTerminalWithoutEndpoint(QueryState.FAILED);
        assertTerminalWithoutEndpoint(QueryState.CANCELLED);
    }

    @Test
    void publishedStreamSurvivesFailureAndCancellation() {
        var endpoint = new ResultEndpoint("worker", 4, "issued", "grpc");
        var failed = new QueryStateMachine();
        running(failed);
        failed.setResultEndpoint(endpoint);
        failed.tryTransitionToFailed(new QueryFailure("FAILED", "INTERNAL_ERROR", "failure"));
        assertThat(failed.status().state()).isEqualTo(QueryState.FAILED);
        assertThat(failed.status().disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(failed.status().endpoint()).isSameAs(endpoint);

        var cancelled = new QueryStateMachine();
        running(cancelled);
        cancelled.setResultEndpoint(endpoint);
        cancelled.tryTransition(QueryState.CANCELLED);
        assertThat(cancelled.status().state()).isEqualTo(QueryState.CANCELLED);
        assertThat(cancelled.status().disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(cancelled.status().endpoint()).isSameAs(endpoint);
    }

    private static void assertTerminalWithoutEndpoint(QueryState terminal) {
        var state = new QueryStateMachine();
        running(state);
        if (terminal == QueryState.FINISHED) {
            state.tryTransition(QueryState.FINISHING);
            state.tryTransition(terminal);
        } else if (terminal == QueryState.FAILED) {
            state.tryTransitionToFailed(new QueryFailure("FAILED", "INTERNAL_ERROR", "failure"));
        } else {
            state.tryTransition(terminal);
        }
        var status = state.status();
        assertThat(status.state()).isEqualTo(terminal);
        assertThat(status.disposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(status.endpoint()).isNull();
    }

    private static void running(QueryStateMachine state) {
        assertThat(state.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(state.tryTransition(QueryState.SCHEDULING)).isTrue();
        assertThat(state.tryTransition(QueryState.RUNNING)).isTrue();
    }
}
