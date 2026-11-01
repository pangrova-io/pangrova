package org.pangrova.query.core;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.coordination.JournalWriteException;

import static org.assertj.core.api.Assertions.assertThat;

/** Competes the caller's close with the journal reaper on actual SQL ownership state. */
class JournalSqlOperationTest {
    @Test
    void callerAndReaperCanReturnEachSqlOwnerOnlyOnce() throws Exception {
        var owner = new JournalPendingWrites(1);
        var stopped = new AtomicBoolean();
        var executor = Executors.newSingleThreadExecutor();
        var reaper = executor.submit(() -> {
            while (!stopped.get()) {
                for (var operation : owner.sqlOwners()) operation.retryClose();
                Thread.onSpinWait();
            }
        });
        try {
            for (int i = 0; i < 100_000; i++) {
                var operation = owner.enterSql();
                try {
                    operation.close();
                } catch (JournalWriteException inFlight) {
                    assertThat(inFlight).hasMessage("Entered journal SQL close remains in flight");
                }
            }
            stopped.set(true);
            reaper.get(10, TimeUnit.SECONDS);
            for (var operation : owner.sqlOwners()) assertThat(operation.retryClose()).isTrue();
            assertThat(owner.drained()).isTrue();
        } finally {
            stopped.set(true);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
