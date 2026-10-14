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
            fixture.submitNativeFailure("SELECT 10 / n FROM (VALUES (0), (1)) AS t(n)");
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
            String rows = IntStream.range(0, 4096).mapToObj(value -> "(" + value + ")")
                    .collect(java.util.stream.Collectors.joining(","));
            // Preserve a real 4,096-row Native task without duplicating an 8 KiB
            // literal across every VALUES row in Calcite's intermediate plans.
            fixture.submit("SELECT '" + "x".repeat(64) + "' FROM (VALUES " + rows + ") AS t(n)");
            assertThat(fixture.manager.cancelQuery(fixture.queryId)).isTrue();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.CANCELLED);
            fixture.releaseActualWorkerOwners();
            assertPending(fixture, "CANCELLED");
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
            // The state listener and terminal cleanup share one durable event identity.
            assertThat(fixture.manager.awaitAllQueries(1000)).isTrue();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "CANCELLED")).isEqualTo(1);
            int writes = fixture.store.terminalWrites(fixture.queryId, "CANCELLED");
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "CANCELLED")).isEqualTo(writes);
        }
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
