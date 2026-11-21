package org.pangrova.server.coordinator.query.task;

import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Owns the real SQL consumer while the test observes a blocked PostgreSQL commit. */
final class TaskJournalExecutionOwner implements AutoCloseable {
    private final ExecutorService executor = ManagedExecutorFactory.newSingleThreadExecutor("journal-sql-consumer");
    private final Future<?> execution;

    TaskJournalExecutionOwner(Runnable action) {
        execution = executor.submit(action);
    }

    void awaitCompletion() throws Exception {
        try { execution.get(30, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    @Override public void close() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual SQL consumer has not exited");
            }
            awaitCompletion();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }
}
