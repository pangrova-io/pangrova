package org.pangrova.indexing.build;

import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.config.IndexingConfig;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class IndexBuildCancellationTest {
    @Test
    void cancellationDuringSnapshotPinCannotProducePublicationSuccess() throws Exception {
        try (var harness = new Harness()) {
            var result = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.pin.awaitEntry();
            harness.builder.cancelBuild("index-1");
            harness.pin.release();
            assertThat(result.get(5, TimeUnit.SECONDS).isFullySuccessful()).isFalse();
        }
    }

    @Test
    void resubmittingSameIndexDoesNotResurrectItsCancelledBuild() throws Exception {
        try (var harness = new Harness()) {
            var old = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.pin.awaitEntry();
            harness.builder.cancelBuild("index-1");
            var replacement = harness.builder.submitBuildTask(descriptor(""), List.of(), null, null);
            assertThat(replacement.get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
            harness.pin.release();
            assertThat(old.get(5, TimeUnit.SECONDS).isFullySuccessful()).isFalse();
        }
    }

    @Test
    void closeCancelsActiveBuildWithoutClosingBorrowedExecutor() throws Exception {
        try (var harness = new Harness()) {
            var result = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.pin.awaitEntry();
            harness.builder.close();
            assertThat(harness.executor.submit(() -> "still shared").get(5, TimeUnit.SECONDS))
                    .isEqualTo("still shared");
            harness.pin.release();
            assertThat(result.get(5, TimeUnit.SECONDS).isFullySuccessful()).isFalse();
            assertThat(harness.builder.submitBuildTask(descriptor(""), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS).isFullySuccessful()).isFalse();
        }
    }

    @Test
    void cancelledPausedRequestsNeverPinAndDoNotCancelLaterSubmissions() throws Exception {
        try (var harness = new Harness()) {
            harness.builder.pauseBuilds();
            var cancelled = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.builder.cancelBuild("index-1");
            assertThat(cancelled.get(5, TimeUnit.SECONDS).outcome()).isEqualTo(IndexBuildResult.Outcome.CANCELLED);
            var replacement = harness.builder.submitBuildTask(descriptor(""), List.of(), null, null);
            harness.builder.resumePendingBuilds();
            assertThat(replacement.get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
            assertThat(harness.pin.calls.get()).isZero();
        }
    }

    @Test
    void futureCancellationRemovesPausedWorkBeforeResume() throws Exception {
        try (var harness = new Harness()) {
            harness.builder.pauseBuilds();
            var cancelled = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            assertThat(cancelled.cancel(false)).isTrue();
            harness.builder.resumePendingBuilds();
            assertThat(harness.builder.submitBuildTask(descriptor(""), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
            assertThat(harness.pin.calls.get()).isZero();
            assertThat(cancelled.isCancelled()).isTrue();
        }
    }

    @RepeatedTest(20)
    void closeRacingSubmissionLeavesNoUnfinishedPausedFuture() throws Exception {
        try (var harness = new Harness()) {
            harness.builder.pauseBuilds();
            var barrier = new CyclicBarrier(2);
            var submitted = harness.executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            });
            barrier.await(5, TimeUnit.SECONDS);
            harness.builder.close();
            assertThat(submitted.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS).isFullySuccessful()).isFalse();
            assertThat(harness.pin.calls.get()).isZero();
        }
    }

    @Test
    void simultaneousCompletionCallbacksCanCancelEachOtherWithoutHoldingAttemptLocks() throws Exception {
        try (var harness = new Harness()) {
            var first = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            var second = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            var callbacks = new CyclicBarrier(2);
            var firstCallback = first.thenRun(() -> cancelFromCallback(harness.builder, callbacks));
            var secondCallback = second.thenRun(() -> cancelFromCallback(harness.builder, callbacks));
            harness.pin.release();
            CompletableFuture.allOf(firstCallback, secondCallback).get(5, TimeUnit.SECONDS);
            assertThat(first.get().isFullySuccessful()).isTrue();
            assertThat(second.get().isFullySuccessful()).isTrue();
        }
    }

    private static void cancelFromCallback(IndexBuildCoordinator builder, CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
            builder.cancelBuild("index-1");
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    @Test
    void activeCancellationReturnsOnlyAfterClaimRelease() throws Exception {
        var claims = new RecordingClaims();
        try (var harness = new Harness(claims)) {
            var result = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.pin.awaitEntry();
            harness.builder.cancelBuild("index-1");
            assertThat(result.isDone()).isFalse();
            assertThat(claims.releases.get()).isZero();
            harness.pin.release();
            assertThat(result.get(5, TimeUnit.SECONDS).outcome()).isEqualTo(IndexBuildResult.Outcome.CANCELLED);
            assertThat(claims.releases.get()).isEqualTo(1);
        }
    }

    @Test
    void cancellingActiveFutureStillReleasesClaimWhenBorrowedOperationReturns() throws Exception {
        var claims = new RecordingClaims();
        try (var harness = new Harness(claims)) {
            var result = harness.builder.submitBuildTask(descriptor("42"), List.of(), null, null);
            harness.pin.awaitEntry();
            assertThat(result.cancel(false)).isTrue();
            assertThat(claims.releases.get()).isZero();
            harness.pin.release();
            assertThat(claims.released.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(claims.releases.get()).isEqualTo(1);
            assertThat(result.isCancelled()).isTrue();
        }
    }

    private static final class RecordingClaims implements IndexBuildClaimRepository {
        private final AtomicInteger releases = new AtomicInteger();
        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public boolean tryClaim(String index, int generation, String owner, Instant now) {
            return true;
        }

        @Override
        public boolean release(String index, int generation, String owner) {
            releases.incrementAndGet();
            released.countDown();
            return true;
        }

        @Override
        public boolean heartbeat(String index, int generation, String owner, Instant now) { return true; }

        @Override
        public int expireBefore(Instant cutoff) { throw new AssertionError("Unexpected expiry"); }
    }

    private static IndexDescriptor descriptor(String snapshot) {
        Instant now = Instant.now();
        return new IndexDescriptor("index-1", "idx", "cat", "sch", "tbl", List.of("col"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING, null, 0,
                snapshot, snapshot, now, now, "tenant-a");
    }

    private static final class Harness implements AutoCloseable {
        private final ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "index-cancellation-test");
            thread.setDaemon(true);
            return thread;
        });
        private final PinBarrier pin = new PinBarrier();
        private final IndexBuildCoordinator builder;

        private Harness() {
            this(null);
        }

        private Harness(IndexBuildClaimRepository claims) {
            builder = new IndexBuildCoordinator(new IndexBuildContext(
                    new IndexingConfig("test-index", false, null, null),
                    null, null, null, claims, null, pin, executor), null);
        }

        @Override
        public void close() throws InterruptedException {
            pin.release();
            builder.close();
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static final class PinBarrier implements SnapshotPinProvider {
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void awaitEntry() throws InterruptedException {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        }

        void release() {
            released.countDown();
        }

        @Override
        public void pinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
            calls.incrementAndGet();
            entered.countDown();
            try {
                if (!released.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Snapshot pin barrier timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        }

        @Override
        public void unpinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {}

        @Override
        public List<Long> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }
    }
}
