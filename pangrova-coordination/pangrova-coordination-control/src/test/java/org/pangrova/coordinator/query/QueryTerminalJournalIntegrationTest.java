package org.pangrova.coordinator.query;

import org.pangrova.query.api.enums.QueryState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real journal permission/transaction locks; no fake journal or Native exit evidence. */
@Tag("integration")
class QueryTerminalJournalIntegrationTest {
    @Test
    void failedTerminalJournalCanBeRetried() throws Exception {
        try (var store = new QueryTerminalJournalFixture();
                var control = new QueryTerminalControlFixture(store.journal())) {
            String id = control.registerRunningQuery();
            store.denyQueryInsert();
            assertThatThrownBy(() -> control.manager.onAllTasksFinished(id))
                    .isInstanceOf(JournalWriteException.class)
                    .satisfies(error -> assertThat(((JournalWriteException) error).admissionOutcome())
                            .isEqualTo(JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED))
                    .hasRootCauseInstanceOf(SQLException.class)
                    .rootCause()
                    .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("42501"))
                    .hasMessageContaining("permission denied for table query_state_journal");
            assertThat(control.manager.getQueryState(id)).contains(QueryState.FINISHED);
            assertThat(control.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(store.terminalWrites(id, "FINISHED")).isZero();
            assertThat(control.manager.awaitAllQueries(1)).isFalse();
            store.allowQueryInsert();
            control.retry();
            control.manager.onAllTasksFinished(id);
            assertThat(store.terminalWrites(id, "FINISHED")).isEqualTo(1);
            assertThat(store.queryRows(id)).isZero();
            assertThat(control.manager.getActiveQueryCount()).isZero();
        }
    }

    @Test
    void slowRetryRunsOffSharedTimer() throws Exception {
        try (var store = new QueryTerminalJournalFixture();
                var control = new QueryTerminalControlFixture(store.journal())) {
            String id = control.registerRunningQuery();
            var frontend = control.enter(id);
            control.manager.onAllTasksFinished(id);
            assertThat(store.terminalWrites(id, "FINISHED")).isEqualTo(1);
            try (var blocker = store.lockQueryRows(id)) {
                frontend.close();
                var timer = Executors.newSingleThreadScheduledExecutor();
                var cleanup = ManagedExecutorFactory.newBoundedThreadPool("actual-journal-cleanup", 2, 2);
                try {
                    control.clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
                    control.manager.startPeriodicCleanup(timer, cleanup, Duration.ofHours(1), Duration.ofMillis(10));
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                            .until(store::purgeWaitingOnActualLock);
                    var tick = new CountDownLatch(1);
                    timer.schedule(tick::countDown, 0, TimeUnit.MILLISECONDS);
                    assertThat(tick.await(1, TimeUnit.SECONDS)).isTrue();
                    assertThat(control.manager.getActiveQueryCount()).isEqualTo(1);
                    blocker.rollback();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                            .untilAsserted(() -> assertThat(control.manager.getActiveQueryCount()).isZero());
                    assertThat(store.terminalWrites(id, "FINISHED")).isEqualTo(1);
                } finally {
                    blocker.rollback();
                    control.manager.stopPeriodicCleanup();
                    timer.shutdownNow();
                    cleanup.shutdown();
                    assertThat(timer.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(cleanup.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }
}
