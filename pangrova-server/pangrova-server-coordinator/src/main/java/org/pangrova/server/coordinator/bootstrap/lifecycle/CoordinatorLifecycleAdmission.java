package org.pangrova.server.coordinator.bootstrap.lifecycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;

/** One-shot process admission and retryable, actual-return shutdown ownership. No callback runs under its lock. */
@ThreadSafe
public final class CoordinatorLifecycleAdmission {
    @GuardedBy("this")
    private boolean startEntered;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean stopping;
    @GuardedBy("this")
    private boolean stopped;
    @GuardedBy("this")
    private Thread starter;
    @GuardedBy("this")
    private Thread closer;
    @GuardedBy("this")
    private CompletableFuture<Void> startExit;
    @GuardedBy("this")
    private CompletableFuture<Void> closeExit;
    @GuardedBy("this")
    private final Map<String, Step> steps = new HashMap<>();
    private final java.util.concurrent.CountDownLatch shutdownCompleted =
            new java.util.concurrent.CountDownLatch(1);

    /** Claims the sole start attempt; a stopped or failed-start process cannot be restarted. */
    public synchronized void beginStart() {
        if (startEntered || stopping) throw new IllegalStateException("Coordinator lifecycle is one-shot");
        CompletableFuture<Void> exited = new CompletableFuture<>();
        startEntered = true;
        starter = Thread.currentThread();
        startExit = exited;
    }

    /**
     * Publishes actual startup exit, regardless of whether startup succeeded.
     * @param succeeded whether the sole startup body completed successfully
     * @throws IllegalStateException if the caller is not the admitted startup thread
     */
    public void endStart(boolean succeeded) {
        CompletableFuture<Void> exited;
        synchronized (this) {
            if (starter != Thread.currentThread()) throw new IllegalStateException("Startup owner changed");
            running = succeeded && !stopping;
            starter = null;
            exited = startExit;
        }
        exited.complete(null);
    }

    /** Returns admission state only; false does not prove dependent resources have exited. */
    public synchronized boolean isRunning() { return running; }

    /**
     * Closes actual owners once at a time, retaining a failed attempt for an explicit retry.
     * @param cleanup actual dependency-ordered cleanup, invoked outside this monitor
     * @throws NullPointerException if cleanup is null, without mutating admission
     * @throws IllegalStateException if a startup or shutdown thread attempts self-join
     */
    public void close(Runnable cleanup) {
        Objects.requireNonNull(cleanup, "cleanup");
        CompletableFuture<Void> started;
        synchronized (this) {
            if (!startEntered && !stopping) return;
            if (starter == Thread.currentThread()) throw new IllegalStateException("Startup cannot join itself");
            stopping = true;
            running = false;
            started = startExit;
        }
        if (started != null) started.join();
        CompletableFuture<Void> attempt;
        boolean claimed;
        synchronized (this) {
            if (stopped) return;
            if (closer == Thread.currentThread()) throw new IllegalStateException("Shutdown cannot join itself");
            claimed = closeExit == null;
            if (claimed) {
                closeExit = new CompletableFuture<>();
                closer = Thread.currentThread();
            }
            attempt = closeExit;
        }
        if (!claimed) {
            attempt.join();
            return;
        }
        try {
            cleanup.run();
            synchronized (this) {
                stopped = true;
            }
            shutdownCompleted.countDown();
            attempt.complete(null);
        } catch (RuntimeException | Error failure) {
            attempt.completeExceptionally(failure);
            throw failure;
        } finally {
            synchronized (this) {
                closeExit = null;
                closer = null;
            }
        }
    }

    /**
     * Retains successful dependency exits; failed steps may be retried without re-closing exited owners.
     * @param name stable identity of the actual dependency owner
     * @param operation actual close/join action, invoked outside this monitor
     * @throws NullPointerException if either argument is null, without adding a step
     * @throws IllegalArgumentException if name is blank
     * @throws IllegalStateException if a dependency attempts to join its own unfinished step
     */
    public void step(String name, Runnable operation) {
        Objects.requireNonNull(name, "step name");
        Objects.requireNonNull(operation, "operation");
        if (name.isBlank()) throw new IllegalArgumentException("Shutdown step name must not be blank");
        Step step;
        boolean claimed;
        synchronized (this) {
            step = steps.get(name);
            if (step != null && !step.exit.isDone() && step.owner == Thread.currentThread()) {
                throw new IllegalStateException("Shutdown dependency cannot join itself: " + name);
            }
            claimed = step == null || step.exit.isCompletedExceptionally();
            if (claimed) {
                step = new Step(Thread.currentThread(), new CompletableFuture<>());
                steps.put(name, step);
            }
        }
        if (!claimed) {
            step.exit.join();
            return;
        }
        try {
            operation.run();
            step.exit.complete(null);
        } catch (RuntimeException | Error failure) {
            step.exit.completeExceptionally(failure);
            throw failure;
        }
    }

    /** Waits for actual successful owner cleanup; failed cleanup remains retryable. */
    public void awaitShutdown() throws InterruptedException {
        shutdownCompleted.await();
    }

    private record Step(Thread owner, CompletableFuture<Void> exit) { }
}
