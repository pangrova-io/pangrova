package org.pangrova.common.concurrent;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Actual executor termination barrier; cancelling submitted futures does not establish exit. */
@ThreadSafe
public final class ExecutorDrain {
    private static final ThreadLocal<Set<ExecutorService>> ENTERED = ThreadLocal.withInitial(
            () -> Collections.newSetFromMap(new IdentityHashMap<>()));

    private ExecutorDrain() {}

    /**
     * Runs the owning executor's actual callback with a self-join boundary on that same thread.
     * This declaration does not transfer executor ownership or replace its shutdown barrier.
     *
     * @param owner executor actually running this callback
     * @param operation callback body, whose original exception is propagated
     * @throws NullPointerException if either argument is null, before entering callback ownership
     */
    public static void runOwned(ExecutorService owner, Runnable operation) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(operation, "operation");
        Set<ExecutorService> entered = ENTERED.get();
        boolean added = entered.add(owner);
        try {
            operation.run();
        } finally {
            if (added) entered.remove(owner);
            if (entered.isEmpty()) ENTERED.remove();
        }
    }

    /**
     * Seals the real executor and awaits actual thread termination; retained owners may retry after failure.
     * Does not discard queued cleanup, clear references, or translate an interruption into success.
     *
     * @param executor actual owned executor, retained by the caller until this method returns normally
     * @param timeoutMs strictly positive termination wait in milliseconds
     * @throws NullPointerException if the executor is null, without changing admission
     * @throws IllegalArgumentException if the timeout is not positive, without changing admission
     * @throws IllegalStateException on self-join, timeout, or interruption; interruption is restored
     */
    public static void shutdownAndAwait(ExecutorService executor, long timeoutMs) {
        Objects.requireNonNull(executor, "executor");
        if (timeoutMs <= 0) throw new IllegalArgumentException("Executor drain timeout must be positive");
        executor.shutdown();
        Set<ExecutorService> entered = ENTERED.get();
        boolean self = entered.contains(executor);
        if (entered.isEmpty()) ENTERED.remove();
        if (self) {
            throw new IllegalStateException("Executor callback cannot join its own owner");
        }
        if (executor.isTerminated()) return;
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Owned executor has not actually terminated");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted awaiting actual executor termination", interrupted);
        }
    }
}
