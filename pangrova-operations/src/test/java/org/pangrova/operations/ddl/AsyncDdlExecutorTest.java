package org.pangrova.operations.ddl;

import org.pangrova.operations.enums.DdlJobState;
import org.pangrova.operations.enums.DdlJobType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AsyncDdlExecutor} verifying job routing, synchronous execution,
 * asynchronous scheduling, and cancellation.
 */
class AsyncDdlExecutorTest {

    @Test
    void metadataOnlyJob_executesSynchronously() {
        var store = new InMemoryDdlJobStore();
        var executed = new AtomicBoolean(false);

        var executor = new AsyncDdlExecutor(store, job -> executed.set(true));

        DdlJob job = newMetadataOnlyJob("sync-1");
        DdlJob result = executor.submit(job);

        assertThat(executed.get()).isTrue();
        assertThat(result.state()).isEqualTo(DdlJobState.COMPLETED);

        DdlJob stored = store.findById("sync-1").orElseThrow();
        assertThat(stored.state()).isEqualTo(DdlJobState.COMPLETED);
    }

    @Test
    void metadataOnlyJob_failureTransitionsToFailed() {
        var store = new InMemoryDdlJobStore();

        var executor = new AsyncDdlExecutor(store, job -> {
            throw new RuntimeException("simulated failure");
        });

        assertThatThrownBy(() -> executor.submit(newMetadataOnlyJob("sync-fail")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("simulated failure");

        DdlJob failed = store.findById("sync-fail").orElseThrow();
        assertThat(failed.state()).isEqualTo(DdlJobState.FAILED);
    }

    @Test
    void dataRewriteJob_schedulesAsynchronously() {
        var store = new InMemoryDdlJobStore();
        var callbackInvoked = new AtomicBoolean(false);

        // Use a gated executor: the worker thread blocks until the gate opens,
        // ensuring assertions run before the background task executes.
        var gate = new CountDownLatch(1);
        ExecutorService gatedExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                r.run();
            }, "async-ddl-test");
            t.setDaemon(true);
            return t;
        });
        var executor = new AsyncDdlExecutor(store, job -> callbackInvoked.set(true),
                gatedExecutor);

        DdlJob job = newDataRewriteJob("rewrite-1");
        DdlJob result = executor.submit(job);

        // DATA_REWRITE jobs transition to RUNNING but are not yet executed (gated)
        assertThat(result.state()).isEqualTo(DdlJobState.RUNNING);
        assertThat(callbackInvoked.get()).isFalse();

        DdlJob stored = store.findById("rewrite-1").orElseThrow();
        assertThat(stored.state()).isEqualTo(DdlJobState.RUNNING);

        // Release the gate and clean up
        gate.countDown();
        gatedExecutor.shutdown();
    }

    @Test
    void cancel_transitionsRunningJobToCancelled() {
        var store = new InMemoryDdlJobStore();

        // Use a gated executor to prevent the background task from completing
        // before cancel() is called — eliminates the race condition.
        var gate = new CountDownLatch(1);
        ExecutorService gatedExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                r.run();
            }, "async-ddl-cancel-test");
            t.setDaemon(true);
            return t;
        });
        var executed = new AtomicBoolean();
        var executor = new AsyncDdlExecutor(store, job -> executed.set(true), gatedExecutor);

        // Submit a DATA_REWRITE job which stays in RUNNING state (gated)
        executor.submit(newDataRewriteJob("rewrite-cancel"));

        executor.cancel("rewrite-cancel");

        DdlJob cancelled = store.findById("rewrite-cancel").orElseThrow();
        assertThat(cancelled.state()).isEqualTo(DdlJobState.CANCELLED);

        gate.countDown();
        executor.close();
        assertThat(executed).isFalse();
        assertThat(store.findById("rewrite-cancel").orElseThrow().state()).isEqualTo(DdlJobState.CANCELLED);
    }

    @Test
    void cancelledInFlightCallbackCannotOverwriteTheTerminalDecision() throws Exception {
        var store = new InMemoryDdlJobStore();
        var started = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        try (var executor = new AsyncDdlExecutor(store, job -> {
            started.countDown();
            try {
                finish.await();
            } catch (InterruptedException failure) {
                throw new IllegalStateException(failure);
            }
        })) {
            executor.submit(newDataRewriteJob("running"));
            try {
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                executor.cancel("running");
                assertThatThrownBy(() -> executor.submit(newDataRewriteJob("running")))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
            } finally {
                finish.countDown();
            }
        }
        assertThat(store.findById("running").orElseThrow().state()).isEqualTo(DdlJobState.CANCELLED);
    }

    @Test
    void schedulingFailureCannotLeaveJobRunning() {
        var store = new InMemoryDdlJobStore();
        var background = Executors.newSingleThreadExecutor();
        background.shutdown();
        try (var executor = new AsyncDdlExecutor(store, job -> {}, background)) {
            assertThatThrownBy(() -> executor.submit(newDataRewriteJob("rejected")))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
        }
        assertThat(store.findById("rejected").orElseThrow().state()).isEqualTo(DdlJobState.FAILED);
    }

    @Test
    void cancel_doesNothingForNonRunningJob() {
        var store = new InMemoryDdlJobStore();

        var executor = new AsyncDdlExecutor(store, job -> {});

        // Submit a METADATA_ONLY job which completes synchronously
        executor.submit(newMetadataOnlyJob("sync-done"));
        assertThat(store.findById("sync-done").orElseThrow().state())
                .isEqualTo(DdlJobState.COMPLETED);

        // Cancelling a completed job should have no effect
        executor.cancel("sync-done");
        assertThat(store.findById("sync-done").orElseThrow().state())
                .isEqualTo(DdlJobState.COMPLETED);
    }

    @Test
    void cancel_doesNothingForNonExistentJob() {
        var store = new InMemoryDdlJobStore();
        var executor = new AsyncDdlExecutor(store, job -> {});

        // Should not throw for missing job ID
        executor.cancel("nonexistent");
    }

    @Test
    void listJobs_returnsAllSubmittedJobs() {
        var store = new InMemoryDdlJobStore();
        var executor = new AsyncDdlExecutor(store, job -> {});

        executor.submit(newMetadataOnlyJob("j1"));
        executor.submit(newDataRewriteJob("j2"));

        List<DdlJob> jobs = executor.listJobs();
        assertThat(jobs).hasSize(2);
    }

    @Test
    void multipleMetadataOnlyJobs_executeIndependently() {
        var store = new InMemoryDdlJobStore();
        var executionOrder = new AtomicInteger(0);

        var executor = new AsyncDdlExecutor(store, job -> executionOrder.incrementAndGet());

        executor.submit(newMetadataOnlyJob("m-a"));
        executor.submit(newMetadataOnlyJob("m-b"));
        executor.submit(newMetadataOnlyJob("m-c"));

        assertThat(executionOrder.get()).isEqualTo(3);
        assertThat(store.findById("m-a").orElseThrow().state()).isEqualTo(DdlJobState.COMPLETED);
        assertThat(store.findById("m-b").orElseThrow().state()).isEqualTo(DdlJobState.COMPLETED);
        assertThat(store.findById("m-c").orElseThrow().state()).isEqualTo(DdlJobState.COMPLETED);
    }

    @Test
    void tableLockJob_executedSynchronously() {
        var store = new InMemoryDdlJobStore();
        var executed = new AtomicBoolean(false);

        var executor = new AsyncDdlExecutor(store, job -> executed.set(true));

        DdlJob job = newTableLockJob("lock-1");
        DdlJob result = executor.submit(job);

        assertThat(executed.get()).isTrue();
        assertThat(result.state()).isEqualTo(DdlJobState.COMPLETED);
    }

    @Test
    void close_awaitsInFlightDataRewriteJobBeforeTerminating() throws InterruptedException {
        var store = new InMemoryDdlJobStore();
        var started = new CountDownLatch(1);
        var executor = new AsyncDdlExecutor(store, job -> {
            started.countDown();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted mid-rewrite", e);
            }
        });

        executor.submit(newDataRewriteJob("rewrite-await"));
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // F-OPS-001: close() must block until the in-flight rewrite finishes, leaving it
        // COMPLETED rather than abandoned. Without awaitTermination this races to RUNNING.
        executor.close();

        assertThat(store.findById("rewrite-await").orElseThrow().state())
                .isEqualTo(DdlJobState.COMPLETED);
    }

    @Test
    void close_forcesShutdownWhenGraceExceeded() throws InterruptedException {
        var store = new InMemoryDdlJobStore();
        var started = new CountDownLatch(1);
        var interrupted = new AtomicBoolean(false);
        var executor = new AsyncDdlExecutor(store, job -> {
            started.countDown();
            try {
                Thread.sleep(10_000);   // far longer than the grace window
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });
        executor.setShutdownGraceSeconds(1);

        executor.submit(newDataRewriteJob("rewrite-slow"));
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        long startNanos = System.nanoTime();
        executor.close();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        // Returned after the ~1s grace via shutdownNow, not after the full 10s sleep.
        assertThat(elapsedMs).isLessThan(9_000);
        Thread.sleep(200);
        assertThat(interrupted.get())
                .as("grace exceeded must force shutdownNow, interrupting the blocked worker")
                .isTrue();
    }

    @Test
    void shutdownStillInterruptsWorkersWhenRecordingCancellationFails() throws Exception {
        var unavailable = new IllegalStateException("job store unavailable");
        var store = new InMemoryDdlJobStore() {
            @Override
            public boolean compareAndUpdateState(String id, DdlJobState expected, DdlJobState next) {
                if (next == DdlJobState.CANCELLED) {
                    throw unavailable;
                }
                return super.compareAndUpdateState(id, expected, next);
            }
        };
        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var workerRelease = new CountDownLatch(1);
        var executor = new AsyncDdlExecutor(store, job -> {
            started.countDown();
            try {
                workerRelease.await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                interrupted.countDown();
                throw new IllegalStateException("interrupted", failure);
            }
        });
        executor.setShutdownGraceSeconds(0);
        try {
            executor.submit(newDataRewriteJob("closing-with-store-down"));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(executor::close).isSameAs(unavailable);
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            workerRelease.countDown();
            executor.close();
        }
    }

    // --- Helpers ---

    private static DdlJob newDataRewriteJob(String id) {
        Instant now = Instant.now();
        return new DdlJob(id, DdlJobType.DATA_REWRITE,
                "ALTER TABLE t SET TBLPROPERTIES('format-version'='2')",
                DdlJobState.PENDING, 0.0, now, now, null);
    }

    private static DdlJob newMetadataOnlyJob(String id) {
        Instant now = Instant.now();
        return new DdlJob(id, DdlJobType.METADATA_ONLY,
                "ALTER TABLE t ADD COLUMN c INT",
                DdlJobState.PENDING, 0.0, now, now, null);
    }

    private static DdlJob newTableLockJob(String id) {
        Instant now = Instant.now();
        return new DdlJob(id, DdlJobType.TABLE_LOCK,
                "LOCK TABLE t",
                DdlJobState.PENDING, 0.0, now, now, null);
    }

    /** Simple in-memory DdlJobStore for testing. */
    private static class InMemoryDdlJobStore implements DdlJobStore {
        private final ConcurrentMap<String, DdlJob> jobs = new ConcurrentHashMap<>();

        @Override
        public void insert(DdlJob job) {
            if (jobs.putIfAbsent(job.jobId(), job) != null) {
                throw new IllegalArgumentException("Duplicate DDL job " + job.jobId());
            }
        }

        @Override
        public Optional<DdlJob> findById(String jobId) {
            return Optional.ofNullable(jobs.get(jobId));
        }

        @Override
        public List<DdlJob> findAll() {
            return new ArrayList<>(jobs.values());
        }

        @Override
        public boolean compareAndUpdateState(String jobId, DdlJobState expectedState,
                                              DdlJobState newState) {
            AtomicBoolean updated = new AtomicBoolean(false);
            jobs.computeIfPresent(jobId, (id, existing) -> {
                if (existing.state() == expectedState) {
                    updated.set(true);
                    return existing.withState(newState);
                }
                return existing;
            });
            return updated.get();
        }
    }
}
