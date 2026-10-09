package org.pangrova.coordinator.query;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.security.audit.AuditEventType;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual task exit receipts, not simulated dispatch or cleanup exceptions; one real Worker process. */
@Tag("native-required")
@Tag("routing-integration")
@ResourceLock("pangrova-packaged-native-runtime")
class QueryTerminalNativeOwnershipIntegrationTest {
    @TempDir Path directory;

    @Test
    void finishedCleanupRetriesWithoutRepeatingEffects() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submit("SELECT 42");
            fixture.consume();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.FINISHED);
            fixture.releaseActualWorkerOwners();
            assertPending(fixture, "FINISHED");
            assertThat(fixture.rows()).isEqualTo(List.of(List.of(42)));
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertReleasedExactlyOnce(fixture, "FINISHED", AuditEventType.QUERY_COMPLETED);
            fixture.manager.onAllTasksFinished(fixture.queryId);
            fixture.retryPhysicalCleanup();
            assertReleasedExactlyOnce(fixture, "FINISHED", AuditEventType.QUERY_COMPLETED);
        }
    }

    @Test
    void failedCleanupDoesNotLoseTerminalEffects() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submitNativeFailure("SELECT 10 / n FROM (SELECT 0 AS n UNION ALL SELECT 1 AS n) t");
            if (fixture.endpoint != null) {
                try { fixture.consume(); }
                catch (IllegalStateException failure) {
                    assertThat(failure).hasMessageContaining("consumption");
                }
            }
            fixture.awaitLogicalTerminal(QueryStateMachine.State.FAILED);
            fixture.releaseActualWorkerOwners();
            fixture.requireActualNativeFailure("division by zero");
            assertThat(fixture.manager.getStateMachine(fixture.queryId).getError()).isPresent();
            assertPending(fixture, "FAILED");
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertReleasedExactlyOnce(fixture, "FAILED", AuditEventType.QUERY_FAILED);
            fixture.retryPhysicalCleanup();
            assertReleasedExactlyOnce(fixture, "FAILED", AuditEventType.QUERY_FAILED);
        }
    }

    @Test
    void cancelledCleanupDoesNotLoseTerminalEffects() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submit("SELECT '" + "x".repeat(8192) + "' " + cancellationSource());
            assertThat(fixture.manager.cancelQuery(fixture.queryId)).isTrue();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.CANCELLED);
            var cancellation = fixture.manager.getStateMachine(fixture.queryId).journalEvent(
                    QueryStateMachine.State.CANCELLED, () -> {
                        throw new IllegalStateException("Actual cancellation did not retain its journal identity");
                    });
            fixture.releaseActualWorkerOwners();
            assertPending(fixture, "CANCELLED");
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
            // Transition recording and terminal persistence share one immutable
            // event ID. Actual purge drains both writes before deleting the row.
            assertThat(fixture.store.terminalWrites(fixture.queryId, "CANCELLED")).isEqualTo(1);
            assertThat(fixture.store.committedEvents(fixture.queryId).stream()
                    .filter(event -> event.state().equals("CANCELLED")).toList()).containsExactly(cancellation);
            assertThat(fixture.store.queryRows(fixture.queryId)).isZero();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "CANCELLED")).isEqualTo(1);
            assertThat(fixture.manager.awaitAllQueries(1000)).isTrue();
        }
    }

    @Test
    void cancellationSourceProducesAll4096RowsInNativeExecution() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submit("SELECT COUNT(*) AS row_count " + cancellationSource());
            fixture.consume();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.FINISHED);
            assertThat(fixture.rows()).isEqualTo(List.of(List.of(4096L)));
            fixture.releaseActualWorkerOwners();
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertReleasedExactlyOnce(fixture, "FINISHED", AuditEventType.QUERY_COMPLETED);
        }
    }

    private static String cancellationSource() {
        String values = IntStream.range(0, 16).mapToObj(value -> "SELECT " + value + " AS n")
                .collect(java.util.stream.Collectors.joining(" UNION ALL "));
        // Preserve the 4096-row, 32 MiB output without a 4096-deep SQL tree.
        return "FROM (" + values + ") a CROSS JOIN (" + values + ") b CROSS JOIN (" + values + ") c";
    }

    private static void assertPending(QueryTerminalNativeFixture fixture, String state) throws Exception {
        assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
        assertThat(fixture.throttle.availableGlobalPermits()).isZero();
        assertThat(fixture.manager.awaitAllQueries(1)).isFalse();
        assertThat(fixture.manager.cleanupTerminalQueries(Duration.ZERO)).isZero();
        assertThat(fixture.store.terminalWrites(fixture.queryId, state)).isGreaterThanOrEqualTo(1);
    }

    private static void assertReleasedExactlyOnce(QueryTerminalNativeFixture fixture,
                                                  String state, AuditEventType type) throws Exception {
        assertThat(fixture.manager.getActiveQueryCount()).isZero();
        assertThat(fixture.manager.awaitAllQueries(1000)).isTrue();
        assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
        assertThat(fixture.store.terminalWrites(fixture.queryId, state)).isEqualTo(1);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(fixture.events(type)).isEqualTo(1));
    }
}
