package org.pangrova.shuffle.service.transport;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/** Serializes one stream's tasks while sharing a bounded executor. */
public final class SerialExecutor {
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    private final Executor executor;
    private final Consumer<RuntimeException> failureHandler;
    private boolean running;

    public SerialExecutor(Executor executor, Consumer<RuntimeException> failureHandler) {
        this.executor = executor;
        this.failureHandler = failureHandler;
    }

    /** Enqueues a task, returning false when the bounded shared executor rejects admission. */
    public synchronized boolean execute(Runnable command) {
        tasks.add(command);
        if (running) {
            return true;
        }
        running = true;
        try {
            executor.execute(this::drain);
            return true;
        } catch (RejectedExecutionException rejected) {
            running = false;
            tasks.clear();
            return false;
        }
    }

    private void drain() {
        while (true) {
            Runnable task;
            synchronized (this) {
                task = tasks.poll();
                if (task == null) {
                    running = false;
                    return;
                }
            }
            try {
                task.run();
            } catch (RuntimeException failure) {
                synchronized (this) {
                    tasks.clear();
                    running = false;
                }
                failureHandler.accept(failure);
                return;
            }
        }
    }
}
