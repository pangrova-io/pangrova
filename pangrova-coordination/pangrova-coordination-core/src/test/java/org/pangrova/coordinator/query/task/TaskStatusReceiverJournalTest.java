package org.pangrova.coordinator.query.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real Native reports, authenticated Coordinator HTTP and PostgreSQL commit waits; single-JVM routing integration. */
@Tag("native-required")
@Tag("routing-integration")
class TaskStatusReceiverJournalTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test
    void terminalStateJournaledSynchronously() throws Exception { assertSynchronous("FINISHED"); }

    @Test
    void failedStateJournaledSynchronously() throws Exception { assertSynchronous("FAILED"); }

    @Test
    void nonTerminalStateJournaledAsynchronously() throws Exception {
        try (var store = new TaskJournalStoreFixture();
             var fixture = new NativeReportDeliveryFixture(directory, "RUNNING", false, 0, 0, store.journal());
             var blocker = store.blockTaskInserts()) {
            try {
                fixture.startWithoutConsumingResult();
                var attempt = actualAttempt(fixture);
                var report = JsonMapperFactory.shared().readValue(attempt.body(), TaskStatusReport.class);
                var issuedAttempt = fixture.issuedAttempt(report);
                await("Actual task INSERT waits on table lock").atMost(Duration.ofSeconds(5)).until(store::actualInsertWaiting);
                await("RUNNING HTTP acknowledgment precedes actual commit").atMost(Duration.ofSeconds(5))
                        .until(() -> attempt.response().get() != null);
                assertThat(attempt.response().get().statusCode()).isEqualTo(200);
                assertThat(report.state()).isEqualTo("RUNNING");
                assertThat(store.committed(report, issuedAttempt)).isZero();
                blocker.rollback();
                await("Actual asynchronous journal commit").atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertThat(store.committed(report, issuedAttempt)).isEqualTo(1));
            } finally { blocker.rollback(); }
            fixture.consumeResult();
            fixture.recoverAndDrain();
            assertThat(fixture.rows()).containsExactly(java.util.List.of(42));
        }
    }

    private void assertSynchronous(String state) throws Exception {
        try (var store = new TaskJournalStoreFixture();
             var fixture = new NativeReportDeliveryFixture(directory, state, false, 0, 0, store.journal());
             var blocker = store.blockTaskInserts();
             var execution = new TaskJournalExecutionOwner(() -> {
                 if (state.equals("FINISHED")) fixture.finish();
                 else fixture.failDuringExecution();
             })) {
            try {
                var attempt = actualAttempt(fixture);
                var report = JsonMapperFactory.shared().readValue(attempt.body(), TaskStatusReport.class);
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

    private static StatusReportHttpRelay.Attempt actualAttempt(NativeReportDeliveryFixture fixture) {
        await("Actual selected Native report reaches HTTP relay").atMost(Duration.ofSeconds(5))
                .until(() -> !fixture.relay().matchingAttempts().isEmpty());
        return fixture.relay().matchingAttempts().get(0);
    }
}
