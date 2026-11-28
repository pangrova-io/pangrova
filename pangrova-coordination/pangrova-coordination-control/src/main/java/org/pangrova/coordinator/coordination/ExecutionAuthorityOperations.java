package org.pangrova.coordinator.coordination;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;

/** Process authority entry/exit barrier. Backend work and joining both occur outside this monitor. */
@ThreadSafe
final class ExecutionAuthorityOperations {
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private int active;
    @GuardedBy("this")
    private final Map<Thread, Integer> entered = new HashMap<>();
    @GuardedBy("this")
    private CompletableFuture<Void> exit;

    synchronized Operation enter() {
        if (sealed) throw new IllegalStateException("Coordinator execution authority is closed");
        Thread thread = Thread.currentThread();
        Operation operation = new Operation(thread);
        entered.merge(thread, 1, Integer::sum);
        active++;
        return operation;
    }

    synchronized void seal() { sealed = true; }

    void awaitExit(long timeoutMs) {
        CompletableFuture<Void> completion;
        synchronized (this) {
            if (!sealed) throw new IllegalStateException("Authority admission is not sealed");
            if (entered.containsKey(Thread.currentThread())) {
                throw new IllegalStateException("Authority operation cannot join itself");
            }
            if (active == 0) return;
            if (exit == null) exit = new CompletableFuture<>();
            completion = exit;
        }
        try {
            completion.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted awaiting actual authority operations", interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            throw new IllegalStateException("Authority operations have not actually exited", failure);
        }
    }

    final class Operation implements AutoCloseable {
        private final Thread thread;
        @GuardedBy("ExecutionAuthorityOperations.this")
        private boolean returned;

        private Operation(Thread thread) { this.thread = thread; }

        @Override public void close() {
            CompletableFuture<Void> completion;
            synchronized (ExecutionAuthorityOperations.this) {
                if (returned) return;
                if (thread != Thread.currentThread()) throw new IllegalStateException("Authority entry owner changed");
                returned = true;
                entered.compute(thread, (key, count) -> count == 1 ? null : count - 1);
                active--;
                completion = active == 0 ? exit : null;
            }
            if (completion != null) completion.complete(null);
        }
    }
}
