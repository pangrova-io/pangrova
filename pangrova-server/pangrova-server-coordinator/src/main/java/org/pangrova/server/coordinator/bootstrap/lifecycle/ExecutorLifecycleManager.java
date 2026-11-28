package org.pangrova.server.coordinator.bootstrap.lifecycle;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Manages the lifecycle of all {@link ScheduledExecutorService} instances
 * within the Coordinator. Executors are registered with a logical name and
 * shut down in LIFO (reverse registration) order during {@link #close()}.
 *
 * <p>LIFO ordering ensures that higher-level schedulers (e.g. MetricReporter)
 * stop before lower-level ones (e.g. LeaseRenewal) they may depend on.</p>
 *
 * <p>Concurrency strategy: registration uses a {@link CopyOnWriteArrayList}
 * for safe concurrent reads during shutdown. Registration is expected to
 * happen only during bootstrap (low contention).</p>
 */
@ThreadSafe
public final class ExecutorLifecycleManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorLifecycleManager.class);
    private static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

    private final CopyOnWriteArrayList<RegisteredExecutor> executors = new CopyOnWriteArrayList<>();
    private final Duration terminationTimeout;

    public ExecutorLifecycleManager() {
        this(DEFAULT_TERMINATION_TIMEOUT);
    }

    public ExecutorLifecycleManager(Duration terminationTimeout) {
        this.terminationTimeout = checkNotNull(terminationTimeout, "terminationTimeout must not be null");
    }

    /**
     * Registers a {@link ScheduledExecutorService} with a logical name.
     * Registration order determines shutdown order (LIFO).
     *
     * @param name     a human-readable name identifying the executor (e.g. "LeaseRenewal")
     * @param executor the executor to manage
     * @return the same executor for fluent wiring
     */
    public ScheduledExecutorService register(String name, ScheduledExecutorService executor) {
        checkNotNull(name, "name must not be null");
        checkNotNull(executor, "executor must not be null");
        executors.add(new RegisteredExecutor(name, executor));
        LOG.info("event=executor_registered name={} totalManaged={}", name, executors.size());
        return executor;
    }

    /**
     * Shuts down all registered executors in LIFO order.
     * Each executor is given {@code shutdownNow()} followed by
     * {@code awaitTermination(timeout)}. Idempotent: subsequent calls are no-ops
     * for already-terminated executors.
     */
    @Override
    public void close() {
        List<RegisteredExecutor> snapshot = new ArrayList<>(executors);
        Collections.reverse(snapshot);

        LOG.info("event=executor_lifecycle_shutdown_start count={}", snapshot.size());

        for (RegisteredExecutor entry : snapshot) {
            shutdownSingle(entry);
        }

        LOG.info("event=executor_lifecycle_shutdown_complete");
    }

    /**
     * Returns the number of currently registered executors.
     */
    public int registeredCount() {
        return executors.size();
    }

    private void shutdownSingle(RegisteredExecutor entry) {
        long startNanos = System.nanoTime();
        try {
            entry.executor().shutdownNow();
            boolean terminated = entry.executor().awaitTermination(
                    terminationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            if (terminated) {
                LOG.info("event=executor_shutdown_complete name={} elapsedMs={}", entry.name(), elapsedMs);
            } else {
                LOG.warn("event=executor_shutdown_timeout name={} elapsedMs={} timeoutMs={}",
                        entry.name(), elapsedMs, terminationTimeout.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            LOG.warn("event=executor_shutdown_interrupted name={} elapsedMs={}", entry.name(), elapsedMs);
        }
    }

    private record RegisteredExecutor(String name, ScheduledExecutorService executor) {}
}
