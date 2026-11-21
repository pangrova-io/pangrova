package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.enums.ScheduledTaskState;
import org.pangrova.coordinator.testinfra.QueryControlPlaneFixture;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.lifecycle.QueryStateMachine;

import static org.assertj.core.api.Assertions.assertThat;

class DeadWorkerTaskRetryIntegrationTest {
    private static final String QUERY = "worker-loss";
    private static final String OLD = "old";
    private static final String LOST = "lost-worker";

    private static QueryStateMachine running(QueryControlPlaneFixture control) {
        var state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        state.tryTransition(QueryState.RUNNING);
        control.queries().registerStateMachine(QUERY, state);
        var tracker = control.tracker();
        tracker.registerTask(QUERY, OLD);
        tracker.markScheduled(QUERY, OLD);
        tracker.assignWorker(OLD, LOST);
        tracker.updateTaskState(QUERY, OLD, "RUNNING");
        return state;
    }

    @Test
    void aLostPhysicalAttemptWithAnAdmittedReplacementDoesNotFailTheQuery() {
        try (var control = new QueryControlPlaneFixture()) {
            var query = running(control);
            CoordinatorShutdownSequence.onDeadWorker(LOST, control.tracker(), control.queries(), (q, t, w) -> {
                control.tracker().registerTask(q, "replacement");
                control.tracker().markScheduled(q, "replacement");
                control.tracker().assignWorker("replacement", "survivor");
                return true;
            });
            assertThat(control.tracker().getTaskState(QUERY, OLD)).isEqualTo(ScheduledTaskState.FAILED);
            assertThat(query.getState()).isEqualTo(QueryState.RUNNING);
            // This component fixture issued no Worker requests. Remove its logical-only registrations.
            control.tracker().unregisterSettledTask(QUERY, OLD);
            control.tracker().unregisterUndispatchedTask(QUERY, "replacement");
        }
    }

    @Test
    void unrecoverableLossFailsButAConcurrentFinalReportWins() {
        for (boolean finished : new boolean[]{true, false}) {
            try (var control = new QueryControlPlaneFixture()) {
                var query = running(control);
                CoordinatorShutdownSequence.onDeadWorker(LOST, control.tracker(), control.queries(), (q, t, w) -> {
                    if (finished) control.tracker().updateTaskState(q, t, "FINISHED");
                    return false;
                });
                assertThat(query.getState()).isEqualTo(finished ? QueryState.RUNNING : QueryState.FAILED);
                control.tracker().unregisterSettledTask(QUERY, OLD);
            }
        }
    }
}
