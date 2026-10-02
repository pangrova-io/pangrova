package org.pangrova.connector.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs Iceberg catalog commits with bounded concurrency, OCC retries and timeout protection. */
final class IcebergCommitExecutor {

    private static final int MAX_THREADS = 8;
    private static final int QUEUE_CAPACITY = 256;
    private static final long KEEP_ALIVE_SECONDS = 60L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    private final RetryingCommitWrapper retry = RetryingCommitWrapper.withDefaults(
            e -> e instanceof CommitFailedException);
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();
    private final long timeoutMs;
    private final ExecutorService executor;

    IcebergCommitExecutor(String catalogName, long timeoutMs) {
        this(timeoutMs, newCommitPool(catalogName));
    }

    IcebergCommitExecutor(long timeoutMs, ExecutorService executor) {
        this.timeoutMs = timeoutMs;
        this.executor = java.util.Objects.requireNonNull(executor, "executor must not be null");
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
        long startMs = System.nanoTime() / NANOS_PER_MILLISECOND;
        try {
            CompletableFuture.runAsync(action, executor).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                    "Catalog commit admission rejected before execution for table " + tableId, e);
        } catch (TimeoutException e) {
            long elapsed = (System.nanoTime() / NANOS_PER_MILLISECOND) - startMs;
            throw new CommitTimeoutException(tableId, elapsed, timeoutMs, e);
        } catch (ExecutionException e) {
            handleExecutionFailure(e, tableId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitTimeoutException(tableId, 0, timeoutMs, e);
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

    private void handleExecutionFailure(ExecutionException failure, String tableId) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        Exception classifiedCause = cause instanceof Exception exception
                ? exception : new RuntimeException(cause);
        throw exceptionClassifier.handleAndClassify(classifiedCause, "commit", tableId);
    }

    private static Thread daemonThread(Runnable runnable, String catalogName) {
        Thread thread = new Thread(runnable, "iceberg-commit-" + catalogName);
        thread.setDaemon(true);
        return thread;
    }
}
