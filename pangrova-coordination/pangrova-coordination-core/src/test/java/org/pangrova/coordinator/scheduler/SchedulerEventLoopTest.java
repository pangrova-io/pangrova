package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.admission.SchedulerEventLoop;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * G-SCHED-08: Validates the {@link SchedulerEventLoop} interface contract
 * and the event-driven query completion pattern that replaces busy-polling.
 */
class SchedulerEventLoopTest {

    @Test
    void whenQueryComplete_completesOnStageFinished() throws Exception {
        var impl = new TestSchedulerEventLoop();
        impl.registerQuery("q-1", 2);

        CompletableFuture<Boolean> future = impl.whenQueryComplete("q-1");
        assertThat(future.isDone()).isFalse();

        impl.onStageCompleted("q-1", 0);
        assertThat(future.isDone())
                .as("Should not complete until all stages finish")
                .isFalse();

        impl.onStageCompleted("q-1", 1);
        assertThat(future.get(1, TimeUnit.SECONDS))
                .as("Should complete with true when all stages finish")
                .isTrue();
    }

    @Test
    void whenQueryComplete_completesWithFalseOnFailure() throws Exception {
        var impl = new TestSchedulerEventLoop();
        impl.registerQuery("q-2", 2);

        CompletableFuture<Boolean> future = impl.whenQueryComplete("q-2");

        impl.onStageFailed("q-2", 0, "test failure");
        assertThat(future.get(1, TimeUnit.SECONDS))
                .as("Should complete with false when a stage fails")
                .isFalse();
    }

    @Test
    void whenQueryComplete_unknownQueryReturnsCompletedFuture() throws Exception {
        var impl = new TestSchedulerEventLoop();

        CompletableFuture<Boolean> future = impl.whenQueryComplete("q-unknown");
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isTrue();
    }

    @Test
    void cleanup_removesTrackedState() {
        var impl = new TestSchedulerEventLoop();
        impl.registerQuery("q-3", 1);
        impl.whenQueryComplete("q-3");

        impl.cleanup("q-3");

        // After cleanup, whenQueryComplete should return already-completed
        CompletableFuture<Boolean> after = impl.whenQueryComplete("q-3");
        assertThat(after.isDone()).isTrue();
    }

    @Test
    void callbackFiredOnFutureExecutor() throws Exception {
        var impl = new TestSchedulerEventLoop();
        impl.registerQuery("q-4", 1);

        AtomicBoolean callbackFired = new AtomicBoolean(false);
        impl.whenQueryComplete("q-4")
                .thenAccept(success -> callbackFired.set(true));

        assertThat(callbackFired.get()).isFalse();

        impl.onStageCompleted("q-4", 0);

        // Callback runs synchronously on the completing thread
        assertThat(callbackFired.get())
                .as("Callback should fire when query completes")
                .isTrue();
    }

    /**
     * Minimal implementation of {@link SchedulerEventLoop} for testing.
     * Tracks per-query stage counts and completion futures.
     */
    private static final class TestSchedulerEventLoop implements SchedulerEventLoop {

        private final ConcurrentHashMap<String, Integer> totalStages = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Integer> completedStages = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, CompletableFuture<Boolean>> futures = new ConcurrentHashMap<>();

        void registerQuery(String queryId, int stageCount) {
            totalStages.put(queryId, stageCount);
            completedStages.put(queryId, 0);
        }

        @Override
        public void onStageCompleted(String queryId, int stageId) {
            int completed = completedStages.merge(queryId, 1, Integer::sum);
            Integer total = totalStages.get(queryId);
            if (total != null && completed >= total) {
                CompletableFuture<Boolean> f = futures.remove(queryId);
                if (f != null) {
                    f.complete(true);
                }
            }
        }

        @Override
        public void onStageFailed(String queryId, int stageId, String reason) {
            CompletableFuture<Boolean> f = futures.remove(queryId);
            if (f != null) {
                f.complete(false);
            }
        }

        @Override
        public CompletableFuture<Boolean> whenQueryComplete(String queryId) {
            if (!totalStages.containsKey(queryId)) {
                return CompletableFuture.completedFuture(true);
            }
            Integer total = totalStages.get(queryId);
            Integer completed = completedStages.getOrDefault(queryId, 0);
            if (total != null && completed >= total) {
                return CompletableFuture.completedFuture(true);
            }
            return futures.computeIfAbsent(queryId, k -> new CompletableFuture<>());
        }

        @Override
        public void cleanup(String queryId) {
            totalStages.remove(queryId);
            completedStages.remove(queryId);
            CompletableFuture<Boolean> f = futures.remove(queryId);
            if (f != null && !f.isDone()) {
                f.complete(false);
            }
        }
    }
}
