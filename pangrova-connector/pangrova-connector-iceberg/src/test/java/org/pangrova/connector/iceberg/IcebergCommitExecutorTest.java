package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.enums.IcebergCommitOutcome;

import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class IcebergCommitExecutorTest {

    @Test
    void saturatedPoolRejectsCommitWithoutExecutingItOnTheCaller() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.AbortPolicy());
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            pool.execute(() -> {
                running.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();
            pool.execute(() -> {});

            AtomicBoolean commitRan = new AtomicBoolean();
            IcebergCommitExecutor commits = new IcebergCommitExecutor(2_000, pool);
            Throwable failure = catchThrowable(() -> commits.execute(
                    () -> commitRan.set(true), "lake.orders"));
            assertThat(failure)
                    .isInstanceOf(ConnectorException.class)
                    .hasMessageContaining("admission rejected before execution");
            assertThat(((ConnectorException) failure).getErrorCode())
                    .isEqualTo(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR);
            assertThat(commitRan).isFalse();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void commitFinishingWithinGraceAfterTimeoutIsNotReportedAsFailure() {
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            AtomicBoolean verified = new AtomicBoolean();
            IcebergCommitExecutor commits = new IcebergCommitExecutor(10, pool, 2_000,
                    (table, snapshotId, context, tableId) -> {
                        verified.set(true);
                        return new CommitStatusVerifier.VerificationResult(
                                IcebergCommitOutcome.UNKNOWN, "unexpected");
                    });

            Throwable failure = catchThrowable(() -> commits.execute(null, null, () -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "lake.orders"));

            assertThat(failure).isNull();
            assertThat(verified)
                    .as("A commit that completes inside the grace window needs no verification")
                    .isFalse();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void timedOutCommitWithProvenSuccessReturnsNormally() throws Exception {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean commitRan = new AtomicBoolean();
        try {
            IcebergCommitExecutor commits = new IcebergCommitExecutor(10, pool, 10,
                    (table, snapshotId, context, tableId) -> {
                        assertThat(tableId).isEqualTo("lake.orders");
                        assertThat(snapshotId).isEqualTo(-1L);
                        return new CommitStatusVerifier.VerificationResult(
                                IcebergCommitOutcome.SUCCEEDED, "commit marker");
                    });

            var completion = commits.execute(null, null, () -> {
                commitRan.set(true);
                awaitUninterruptibly(release);
            }, "lake.orders");

            assertThat(completion).isEqualTo(IcebergCommitExecutor.Completion.VERIFIED_AFTER_TIMEOUT);
            assertThat(commitRan).isTrue();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void timedOutCommitWithUnprovableOutcomeFailsClosedAndIsNotRetryable() {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch release = new CountDownLatch(1);
        try {
            IcebergCommitExecutor commits = new IcebergCommitExecutor(10, pool, 10,
                    (table, snapshotId, context, tableId) ->
                            new CommitStatusVerifier.VerificationResult(
                                    IcebergCommitOutcome.UNKNOWN,
                                    "snapshot advanced without marker"));

            Throwable failure = catchThrowable(() -> commits.execute(null, null,
                    () -> awaitUninterruptibly(release), "lake.orders"));

            assertThat(failure)
                    .isInstanceOf(CommitStateUnknownException.class)
                    .hasMessageContaining("unknown")
                    .hasMessageContaining("snapshot advanced without marker");
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void timedOutCommitWithoutSnapshotAdvancementRemainsUnknown() {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch release = new CountDownLatch(1);
        try {
            IcebergCommitExecutor commits = new IcebergCommitExecutor(10, pool, 10,
                    (table, snapshotId, context, tableId) ->
                            new CommitStatusVerifier.VerificationResult(
                                    IcebergCommitOutcome.UNKNOWN,
                                    "snapshot unchanged"));

            Throwable failure = catchThrowable(() -> commits.execute(null, null,
                    () -> awaitUninterruptibly(release), "lake.orders"));

            assertThat(failure)
                    .isInstanceOf(CommitStateUnknownException.class)
                    .hasMessageContaining("lake.orders");
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void interruptionAfterSubmissionDoesNotMakeTheCommitRetryable() {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try {
            IcebergCommitExecutor commits = new IcebergCommitExecutor(2_000, pool);
            Thread.currentThread().interrupt();
            Throwable failure = catchThrowable(() -> commits.execute(
                    () -> awaitUninterruptibly(release), "lake.orders"));

            assertThat(failure).isInstanceOf(CommitStateUnknownException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            release.countDown();
            pool.shutdownNow();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException ignored) {
                // A timed-out commit is cancelled with an interrupt; the test action keeps
                // running until the executor has finished verifying the outcome.
            }
        }
    }
}
