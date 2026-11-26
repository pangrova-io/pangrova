package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.exception.CommitTimeoutException;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs Iceberg catalog commits with bounded concurrency, OCC retries and timeout
 * protection.
 *
 * <p>A timed-out commit is never retried blindly. The executor first gives the
 * in-flight action a bounded grace window to finish, then verifies the catalog
 * state: proven success returns normally, and anything that cannot be proven,
 * including an unchanged snapshot, fails closed as
 * {@link CommitStateUnknownException}.</p>
 */
final class IcebergCommitExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitExecutor.class);

    private static final int MAX_THREADS = 8;
    private static final int QUEUE_CAPACITY = 256;
    private static final long KEEP_ALIVE_SECONDS = 60L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    private final RetryingCommitWrapper retry = RetryingCommitWrapper.withDefaults(
            e -> e instanceof CommitFailedException);
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();
    private final long timeoutMs;
    private final long inFlightGraceMs;
    private final CommitVerifier verifier;
    private final ExecutorService executor;

    enum Completion {
        TASK_COMPLETED,
        VERIFIED_AFTER_TIMEOUT
    }

    /** Verifies the catalog state of a commit that exceeded its timeout. */
    @FunctionalInterface
    interface CommitVerifier {
        CommitStatusVerifier.VerificationResult verify(Table table, long preCommitSnapshotId,
                DistributedWriteCommitContext context, String tableId);
    }

    IcebergCommitExecutor(String catalogName, long timeoutMs) {
        this(timeoutMs, newCommitPool(catalogName));
    }

    IcebergCommitExecutor(long timeoutMs, ExecutorService executor) {
        this(timeoutMs, executor, CommitStatusVerifier.VERIFICATION_GRACE_MS,
                CommitStatusVerifier::verify);
    }

    IcebergCommitExecutor(long timeoutMs, ExecutorService executor,
            long inFlightGraceMs, CommitVerifier verifier) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive: " + timeoutMs);
        }
        if (inFlightGraceMs < 0) {
            throw new IllegalArgumentException(
                    "inFlightGraceMs must be non-negative: " + inFlightGraceMs);
        }
        this.timeoutMs = timeoutMs;
        this.inFlightGraceMs = inFlightGraceMs;
        this.verifier = Objects.requireNonNull(verifier, "verifier must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    private static ThreadPoolExecutor newCommitPool(String catalogName) {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                MAX_THREADS, MAX_THREADS, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                runnable -> daemonThread(runnable, catalogName),
                new ThreadPoolExecutor.AbortPolicy());
        threadPool.allowCoreThreadTimeOut(true);
        return threadPool;
    }

    void execute(Runnable action, String tableId) {
        execute(null, null, action, tableId);
    }

    /**
     * Executes a commit and, on timeout, verifies whether it actually landed.
     *
     * @param table   the table being committed, or null when the caller supplies no state
     * @param context durable commit context used to prove our snapshot when available
     */
    Completion execute(Table table, DistributedWriteCommitContext context,
            Runnable action, String tableId) {
        long startMs = System.nanoTime() / NANOS_PER_MILLISECOND;
        long preCommitSnapshotId = currentSnapshotId(table);
        FutureTask<Void> task = new FutureTask<>(action, null);
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                    "Catalog commit admission rejected before execution for table " + tableId, e);
        }
        try {
            task.get(timeoutMs, TimeUnit.MILLISECONDS);
            return Completion.TASK_COMPLETED;
        } catch (TimeoutException e) {
            long elapsed = elapsedMs(startMs);
            return awaitInFlightCompletionBeforeVerification(task, table, context,
                    preCommitSnapshotId, tableId, elapsed, e);
        } catch (ExecutionException e) {
            throw classifyExecutionFailure(e, tableId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitTimeoutException(tableId, 0, timeoutMs, e);
        }
    }

    /**
     * A timed-out commit may still be in flight. Give it a bounded grace window so a
     * commit that completes just after the deadline is reported as success instead of
     * an ambiguous failure, then cancel and verify the catalog.
     */
    private Completion awaitInFlightCompletionBeforeVerification(FutureTask<Void> task,
            Table table, DistributedWriteCommitContext context, long preCommitSnapshotId,
            String tableId, long elapsedMs, TimeoutException cause) {
        try {
            task.get(inFlightGraceMs, TimeUnit.MILLISECONDS);
            LOG.info("event=commit_completed_after_timeout table={} elapsedMs={} timeoutMs={}",
                    tableId, elapsedMs, timeoutMs);
            return Completion.TASK_COMPLETED;
        } catch (TimeoutException stillRunning) {
            task.cancel(true);
            resolveTimedOutCommit(table, context, preCommitSnapshotId, tableId, elapsedMs, cause);
            return Completion.VERIFIED_AFTER_TIMEOUT;
        } catch (ExecutionException completedWithFailure) {
            throw classifyExecutionFailure(completedWithFailure, tableId);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CommitTimeoutException(tableId, elapsedMs, timeoutMs, interrupted);
        }
    }

    private void resolveTimedOutCommit(Table table, DistributedWriteCommitContext context,
            long preCommitSnapshotId, String tableId, long elapsedMs, TimeoutException cause) {
        CommitStatusVerifier.VerificationResult result =
                verifier.verify(table, preCommitSnapshotId, context, tableId);
        switch (result.outcome()) {
            case SUCCEEDED -> LOG.info(
                    "event=commit_timeout_resolved table={} outcome=SUCCEEDED detail={}",
                    tableId, result.detail());
            case UNKNOWN -> throw new CommitStateUnknownException(
                    new IllegalStateException(
                            "Commit outcome for table %s is unknown after timeout: %s"
                                    .formatted(tableId, result.detail()), cause));
            default -> throw new IllegalStateException(
                    "Unhandled commit verification outcome: " + result.outcome());
        }
    }

    void retry(Runnable action) {
        retry.execute(action);
    }

    void executeIdempotently(
            Table table, DistributedWriteCommitContext context, Runnable action) {
        retry.execute(() -> {
            table.refresh();
            if (IcebergDistributedCommitMarker.isPublished(table, context)) {
                return;
            }
            IcebergDistributedCommitMarker.requireCompleteHistorySinceBase(table, context);
            action.run();
        });
    }

    private static long currentSnapshotId(Table table) {
        if (table == null) {
            return -1L;
        }
        Snapshot snapshot = table.currentSnapshot();
        return snapshot != null ? snapshot.snapshotId() : -1L;
    }

    private static long elapsedMs(long startMs) {
        return (System.nanoTime() / NANOS_PER_MILLISECOND) - startMs;
    }

    private RuntimeException classifyExecutionFailure(ExecutionException failure, String tableId) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        Exception classifiedCause = cause instanceof Exception exception
                ? exception : new RuntimeException(cause);
        return exceptionClassifier.handleAndClassify(classifiedCause, "commit", tableId);
    }

    private static Thread daemonThread(Runnable runnable, String catalogName) {
        Thread thread = new Thread(runnable, "iceberg-commit-" + catalogName);
        thread.setDaemon(true);
        return thread;
    }
}
