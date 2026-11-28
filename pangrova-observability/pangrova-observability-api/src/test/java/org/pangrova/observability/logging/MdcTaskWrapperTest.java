package org.pangrova.observability.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Verifies {@link MdcTaskWrapper} propagates MDC context (trace_id, queryId, taskId)
 * across async thread boundaries for Runnable, Callable, Executor, and ExecutorService.
 */
class MdcTaskWrapperTest {

    private final MdcTaskWrapper taskWrapping = new MdcTaskWrapper();
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "mdc-test-worker");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() {
        LogContext.clear();
        executor.shutdownNow();
    }

    @Test
    void wrapRunnable_propagatesMdcToAsyncThread() throws Exception {
        LogContext.set("trace-abc", "span-123");
        LogContext.setQueryId("query-42");

        AtomicReference<String> capturedTraceId = new AtomicReference<>();
        AtomicReference<String> capturedQueryId = new AtomicReference<>();

        Runnable wrapped = taskWrapping.wrap((Runnable) () -> {
            capturedTraceId.set(MDC.get(LogContext.KEY_TRACE_ID));
            capturedQueryId.set(MDC.get(LogContext.KEY_QUERY_ID));
        });

        Thread thread = new Thread(wrapped);
        thread.start();
        thread.join(5000);

        assertThat(capturedTraceId.get()).isEqualTo("trace-abc");
        assertThat(capturedQueryId.get()).isEqualTo("query-42");
    }

    @Test
    void wrapRunnable_clearsMdcAfterExecution() throws Exception {
        LogContext.set("trace-xyz", "span-456");

        AtomicReference<String> afterExecution = new AtomicReference<>("not-cleared");

        Runnable wrapped = taskWrapping.wrap((Runnable) () -> {
            // MDC should be set during execution
        });

        Thread thread = new Thread(() -> {
            wrapped.run();
            afterExecution.set(MDC.get(LogContext.KEY_TRACE_ID));
        });
        thread.start();
        thread.join(5000);

        assertThat(afterExecution.get()).isNull();
    }

    @Test
    void wrapRunnable_clearsMdcEvenOnException() throws Exception {
        LogContext.set("trace-err", "span-err");

        AtomicReference<String> afterException = new AtomicReference<>("not-cleared");

        Runnable wrapped = taskWrapping.wrap((Runnable) () -> {
            throw new RuntimeException("intentional");
        });

        Thread thread = new Thread(() -> {
            try {
                wrapped.run();
            } catch (RuntimeException ignored) {
                // expected
            }
            afterException.set(MDC.get(LogContext.KEY_TRACE_ID));
        });
        thread.start();
        thread.join(5000);

        assertThat(afterException.get()).isNull();
    }

    @Test
    void wrapCallable_propagatesMdcAndReturnsResult() throws Exception {
        LogContext.set("trace-call", "span-call");

        Callable<String> wrapped = taskWrapping.wrap(
                (Callable<String>) () -> MDC.get(LogContext.KEY_TRACE_ID));

        Future<String> future = executor.submit(wrapped);
        assertThat(future.get()).isEqualTo("trace-call");
    }

    @Test
    void wrapCallable_clearsMdcAfterExecution() throws Exception {
        LogContext.set("trace-clean", "span-clean");

        Callable<String> wrapped = taskWrapping.wrap((Callable<String>) () -> "done");

        // Submit and wait for completion
        executor.submit(wrapped).get();

        // Submit another task to check MDC is clean on the same thread
        Future<String> checkFuture = executor.submit(() -> MDC.get(LogContext.KEY_TRACE_ID));
        assertThat(checkFuture.get()).isNull();
    }

    @Test
    void wrapExecutor_propagatesMdcForEveryTask() throws Exception {
        LogContext.set("trace-exec", "span-exec");

        var wrappedExecutor = taskWrapping.wrapExecutor(executor);

        AtomicReference<String> captured = new AtomicReference<>();
        wrappedExecutor.execute(() -> captured.set(MDC.get(LogContext.KEY_TRACE_ID)));

        // Wait for task completion
        executor.submit(() -> {}).get();

        assertThat(captured.get()).isEqualTo("trace-exec");
    }

    @Test
    void wrapExecutorService_submitRunnable_propagatesMdc() throws Exception {
        LogContext.set("trace-svc", "span-svc");

        ExecutorService wrappedService = taskWrapping.wrapExecutorService(executor);

        AtomicReference<String> captured = new AtomicReference<>();
        wrappedService.submit((Runnable) () ->
                captured.set(MDC.get(LogContext.KEY_TRACE_ID))).get();

        assertThat(captured.get()).isEqualTo("trace-svc");
    }

    @Test
    void wrapExecutorService_submitCallable_propagatesMdc() throws Exception {
        LogContext.set("trace-svc2", "span-svc2");

        ExecutorService wrappedService = taskWrapping.wrapExecutorService(executor);

        Future<String> future = wrappedService.submit(
                (Callable<String>) () -> MDC.get(LogContext.KEY_TRACE_ID));

        assertThat(future.get()).isEqualTo("trace-svc2");
    }

    @Test
    void wrapExecutorService_invokeAll_propagatesMdc() throws Exception {
        LogContext.set("trace-all", "span-all");

        ExecutorService wrappedService = taskWrapping.wrapExecutorService(executor);

        List<Future<String>> futures = wrappedService.invokeAll(List.of(
                () -> MDC.get(LogContext.KEY_TRACE_ID),
                () -> MDC.get(LogContext.KEY_QUERY_ID)
        ));

        assertThat(futures.get(0).get()).isEqualTo("trace-all");
        assertThat(futures.get(1).get()).isNull(); // queryId not set
    }

    @Test
    void wrapExecutorService_delegatesLifecycleMethods() {
        ExecutorService wrappedService = taskWrapping.wrapExecutorService(executor);

        assertThat(wrappedService.isShutdown()).isFalse();
        assertThat(wrappedService.isTerminated()).isFalse();

        wrappedService.shutdown();
        assertThat(wrappedService.isShutdown()).isTrue();
    }

    @Test
    void wrapRunnable_rejectsNull() {
        assertThatThrownBy(() -> taskWrapping.wrap((Runnable) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("task must not be null");
    }

    @Test
    void wrapCallable_rejectsNull() {
        assertThatThrownBy(() -> taskWrapping.wrap((Callable<?>) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("task must not be null");
    }

    @Test
    void wrapExecutor_rejectsNull() {
        assertThatThrownBy(() -> taskWrapping.wrapExecutor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate must not be null");
    }

    @Test
    void wrapExecutorService_rejectsNull() {
        assertThatThrownBy(() -> taskWrapping.wrapExecutorService(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate must not be null");
    }

    @Test
    void parentThreadMdcUnaffectedByAsyncExecution() throws Exception {
        LogContext.set("parent-trace", "parent-span");

        Runnable wrapped = taskWrapping.wrap((Runnable) () -> {
            assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("parent-trace");
        });

        Thread thread = new Thread(wrapped);
        thread.start();
        thread.join(5000);

        // Parent thread MDC unchanged
        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("parent-trace");
        assertThat(MDC.get(LogContext.KEY_SPAN_ID)).isEqualTo("parent-span");
    }

    @Test
    void emptyMdcPropagatesAsEmpty() throws Exception {
        LogContext.clear();

        AtomicReference<String> captured = new AtomicReference<>("should-be-null");

        Runnable wrapped = taskWrapping.wrap(
                (Runnable) () -> captured.set(MDC.get(LogContext.KEY_TRACE_ID)));

        Thread thread = new Thread(wrapped);
        thread.start();
        thread.join(5000);

        assertThat(captured.get()).isNull();
    }

    @Test
    void nestedInlineExecutionRestoresTheExecutingContextEvenOnFailure() throws Exception {
        org.slf4j.MDC.put("queryId", "captured");
        Runnable failure = taskWrapping.wrap((Runnable) () -> {
            assertThat(org.slf4j.MDC.get("queryId")).isEqualTo("captured");
            org.slf4j.MDC.put("queryId", "mutated");
            throw new IllegalStateException("task failed");
        });
        java.util.concurrent.Callable<String> callable = taskWrapping.wrap(
                (java.util.concurrent.Callable<String>) () -> org.slf4j.MDC.get("queryId"));
        org.slf4j.MDC.put("queryId", "outer");
        org.assertj.core.api.Assertions.assertThatThrownBy(failure::run)
                .isInstanceOf(IllegalStateException.class).hasMessage("task failed");
        assertThat(org.slf4j.MDC.get("queryId")).isEqualTo("outer");
        assertThat(callable.call()).isEqualTo("captured");
        assertThat(org.slf4j.MDC.get("queryId")).isEqualTo("outer");
        taskWrapping.wrapExecutor(Runnable::run).execute(() -> org.slf4j.MDC.put("queryId", "inner"));
        assertThat(org.slf4j.MDC.get("queryId")).isEqualTo("outer");
    }
}
