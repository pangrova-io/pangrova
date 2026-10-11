package org.pangrova.coordinator.query;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Real journal failure plus actual SQL/JNI task and throttle ownership, not a fake permit release. */
@Tag("native-required")
@Tag("routing-integration")
@ResourceLock("pangrova-packaged-native-runtime")
class QueryTerminalJournalPermitIntegrationTest {
    @TempDir Path directory;

    @Test
    void terminalJournalRetryReleasesPermitExactlyOnce() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submit("SELECT 42");
            fixture.store.denyFinishedQueryInsert();
            fixture.consume();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.FINISHED);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isZero());
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isZero();
            // This real FINISHED INSERT fault prevents the original execution
            // publication ACK; the later release phase must not overtake it.
            var cleanup = assertThrows(IllegalStateException.class, fixture::cleanupActualWorkerOwners);
            assertThat(cleanup).hasMessage("Task resource cleanup sweep failed");
            assertThat(postgresFailures(cleanup)).anySatisfy(failure -> {
                assertThat(failure.getSQLState()).isEqualTo("42501");
                assertThat(failure.getMessage()).contains("Actual FINISHED journal insert denied");
            });
            fixture.requireUnacknowledgedExecutionTerminalRetries();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isZero();
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isZero();
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isZero();
            fixture.store.allowFinishedQueryInsert();
            // The retained real cleanup continuation must establish the report ACK
            // before query cleanup can retire its issued authority and return capacity.
            fixture.releaseActualWorkerOwners();
            var released = fixture.awaitActualNativeOwnersReleased();
            assertThat(TaskReport.parseFrom(released.canonicalTaskReport()).getState())
                    .isEqualTo(TaskState.TASK_STATE_FINISHED);
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isEqualTo(1);
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
        }
    }

    private static List<SQLException> postgresFailures(Throwable failure) {
        var found = new ArrayList<SQLException>();
        var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var pending = new ArrayDeque<Throwable>();
        pending.add(failure);
        while (!pending.isEmpty()) {
            var actual = pending.removeFirst();
            if (!visited.add(actual)) continue;
            if (actual instanceof SQLException sql) found.add(sql);
            if (actual.getCause() != null) pending.addLast(actual.getCause());
            Collections.addAll(pending, actual.getSuppressed());
        }
        return List.copyOf(found);
    }
}
