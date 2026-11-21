package org.pangrova.server.coordinator.bootstrap.lifecycle;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;

/** Accepted callback ownership; cancellation of a scheduling future is not a callback's exit. */
@ThreadSafe
public final class CoordinatorCallbackLifetime {
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private int operations;
    @GuardedBy("this")
    private final Map<Thread, Integer> entered = new HashMap<>();
    @GuardedBy("this")
    private CompletableFuture<Void> exit;

    /**
     * Claims an actual accepted callback before submitting it to an executor.
     * @return one operation returned only on rejection before transfer or actual callback exit
     * @throws IllegalStateException if admission has been sealed
     */
    public synchronized Operation enter() {
        if (sealed) throw new IllegalStateException("Coordinator callback admission is closed");
        Operation operation = new Operation();
        operations++;
        return operation;
    }

    /**
     * Stops admission and waits for every accepted callback's actual finally-exit.
     * @param timeoutMs positive bounded join duration
     * @throws IllegalArgumentException if the timeout is not positive
     * @throws IllegalStateException if an entrant joins itself, is interrupted, or remains live
     */
    public void close(long timeoutMs) {
        if (timeoutMs <= 0) throw new IllegalArgumentException("Callback drain timeout must be positive");
        CompletableFuture<Void> completion;
        synchronized (this) {
            sealed = true;
            if (entered.containsKey(Thread.currentThread())) {
                throw new IllegalStateException("Callback cannot wait for its own exit");
            }
            if (operations == 0) return;
            if (exit == null) exit = new CompletableFuture<>();
            completion = exit;
        }
        try {
            completion.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted joining actual coordinator callbacks", interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            throw new IllegalStateException("Coordinator callbacks have not actually exited", failure);
        }
    }

    /** One accepted operation, transferable to its actual executor thread. */
    @ThreadSafe
    public final class Operation implements AutoCloseable {
        private final AtomicInteger state = new AtomicInteger();

        private Operation() {}

        /**
         * Executes the admitted callback and publishes exit only from its actual finally block.
         * @param body actual admitted operation; never invoked under the ownership monitor
         * @throws NullPointerException if body is null, without entering this operation
         * @throws IllegalStateException if the operation was already entered or returned
         */
        public void run(Runnable body) {
            java.util.Objects.requireNonNull(body, "body");
            if (!state.compareAndSet(0, 1)) {
                throw new IllegalStateException("Callback operation was already entered or released");
            }
            Thread thread = Thread.currentThread();
            boolean published = false;
            try {
                synchronized (CoordinatorCallbackLifetime.this) {
                    entered.merge(thread, 1, Integer::sum);
                }
                published = true;
                body.run();
            } finally {
                if (published) {
                    synchronized (CoordinatorCallbackLifetime.this) {
                        entered.compute(thread, (key, count) -> count == 1 ? null : count - 1);
                    }
                }
                if (state.compareAndSet(1, 2)) exited();
            }
        }

        /** Returns an operation rejected before executor transfer or exited from its actual callback. */
        @Override public void close() {
            if (state.compareAndSet(0, 2)) {
                exited();
                return;
            }
            if (state.get() == 1) throw new IllegalStateException("Running callback has not exited");
        }

        private void exited() {
            CompletableFuture<Void> completion;
            synchronized (CoordinatorCallbackLifetime.this) {
                operations--;
                completion = operations == 0 ? exit : null;
            }
            if (completion != null) completion.complete(null);
        }
    }
}
