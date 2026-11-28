package org.pangrova.server.coordinator.query.task;


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.rest.task.TaskStatusWire;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real Native reports, authenticated Coordinator HTTP and PostgreSQL commit waits; single-JVM routing integration. */
@Tag("native-required")
@Tag("routing-integration")
class TaskStatusReceiverJournalTest extends ContainerTestBase {
    private static final String SEPARATE_PURGE_FENCE = "WITH locked AS MATERIALIZED (%FROM execution_anchor%"
            + "SELECT % FROM authorized";
    private static final String ATOMIC_RETIREMENT = "WITH owner(execution_id,%FROM removed%"
            + "DELETE FROM task_state_journal j USING removed r%"
            + "DELETE FROM query_state_journal q USING removed r%";
    @TempDir Path directory;

    @Test
    void terminalStateJournaledSynchronously() throws Exception { assertSynchronous("FINISHED"); }

    @Test
    void failedStateJournaledSynchronously() throws Exception { assertSynchronous("FAILED"); }

    @Test
    void runningProgressReachesHttpAndFlightWithOnlyAdmissionAndTerminalJournalFacts() throws Exception {
        try (var store = new TaskJournalStoreFixture();
             var fixture = new NativeReportDeliveryFixture(directory, "RUNNING", false, 0, 0, store);
             var blocker = store.blockTaskReportInserts()) {
            var queryId = fixture.queryId();
            long previousPurgeFences = store.statementCalls(SEPARATE_PURGE_FENCE);
            long previousAtomicRetirements = store.statementCalls(ATOMIC_RETIREMENT);
            try {
                fixture.startWithoutConsumingResult();
                var attempt = actualAttempt(fixture, store);
                var report = TaskStatusWire.decode(attempt.body());
                await("RUNNING HTTP acknowledgment").atMost(Duration.ofSeconds(5))
                        .until(() -> attempt.response().get() != null);
                assertThat(attempt.response().get().statusCode()).isEqualTo(200);
                assertThat(report.state()).isEqualTo("RUNNING");
                assertThat(fixture.queries().getQueryState(queryId))
                        .contains(org.pangrova.query.api.enums.QueryState.RUNNING);
                assertThat(store.actualInsertWaiting()).isFalse();
                assertThat(store.queryStates(queryId)).containsExactly("QUEUED");
                assertThat(store.taskStates(queryId)).containsExactly("SCHEDULED");
            } finally {
                blocker.rollback();
            }
            fixture.consumeResult();
            fixture.recoverAndDrain();
            assertThat(fixture.queries().getStateMachine(queryId).status().drained()).isTrue();
            assertThat(fixture.rows()).containsExactly(java.util.List.of(42));
            assertThat(store.queryStates(queryId)).containsExactly("QUEUED", "FINISHED");
            assertThat(store.taskStates(queryId)).startsWith("SCHEDULED").contains("FINISHED")
                    .allMatch(state -> state.equals("SCHEDULED") || state.equals("FINISHED"));
            assertThat(store.statementCalls(SEPARATE_PURGE_FENCE) - previousPurgeFences)
                    .as("separate journal purge fence reads").isZero();
            assertThat(store.statementCalls(ATOMIC_RETIREMENT) - previousAtomicRetirements)
                    .as("atomic journal and execution retirements").isOne();
        }
    }

    private void assertSynchronous(String state) throws Exception {
        try (var store = new TaskJournalStoreFixture();
             var fixture = new NativeReportDeliveryFixture(directory, state, false, 0, 0, store);
             var blocker = store.blockTaskReportInserts();
             var execution = new TaskJournalExecutionOwner(() -> {
                 if (state.equals("FINISHED")) fixture.finish();
                 else fixture.failDuringExecution();
             })) {
            try {
                var attempt = actualAttempt(fixture, store);
                var report = TaskStatusWire.decode(attempt.body());
                var issuedAttempt = fixture.issuedAttempt(report);
                await("Actual task INSERT waits on table lock").atMost(Duration.ofSeconds(5)).until(store::actualInsertWaiting);
                assertThat(attempt.response().get()).as("Terminal HTTP handler must await the actual journal commit").isNull();
                assertThat(report.state()).isEqualTo(state);
                assertThat(store.committed(report, issuedAttempt)).isZero();
                blocker.rollback();
                await("Terminal acknowledgment follows actual commit").atMost(Duration.ofSeconds(5))
                        .until(() -> attempt.response().get() != null);
                assertThat(attempt.response().get().statusCode()).isEqualTo(200);
                assertThat(store.committed(report, issuedAttempt)).isEqualTo(1);
            } finally { blocker.rollback(); }
            execution.awaitCompletion();
            fixture.recoverAndDrain();
            if (state.equals("FINISHED")) assertThat(fixture.rows()).containsExactly(java.util.List.of(42));
            else assertThat(fixture.resultFailure()).isNotNull();
        }
    }

    private static StatusReportHttpRelay.Attempt actualAttempt(
            NativeReportDeliveryFixture fixture, TaskJournalStoreFixture store) {
        await("Actual SQL planning commits the Worker issuance")
                .atMost(Duration.ofMillis(AsyncSubmitConfig.DEFAULT_SUBMIT_TIMEOUT_MS))
                .until(() -> store.taskStates(fixture.queryId()).contains("SCHEDULED"));
        await("Actual selected Native report reaches HTTP relay").atMost(Duration.ofSeconds(5))
                .until(() -> !fixture.relay().matchingAttempts().isEmpty());
        return fixture.relay().matchingAttempts().get(0);
    }
}
