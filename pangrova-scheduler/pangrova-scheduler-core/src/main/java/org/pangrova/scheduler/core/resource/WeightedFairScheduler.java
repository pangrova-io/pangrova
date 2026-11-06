package org.pangrova.scheduler.core.resource;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Weighted fair scheduler that distributes resources across queue types by weight.
 *
 * <p>A single dispatcher thread continuously dequeues tasks in proportion to each
 * queue type's weight. When all queues are empty the dispatcher parks on a semaphore
 * until the next {@link #submitTask} call signals availability.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for task queues,
 * ConcurrentLinkedQueue for per-type task storage, Semaphore for
 * dispatcher wake-up. All public methods are safe for concurrent access.</p>
 */
@ThreadSafe
public final class WeightedFairScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WeightedFairScheduler.class);

    private static final long SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    private final ConcurrentHashMap<QueueType, Queue<Runnable>> taskQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<QueueType, AtomicLong> cpuTimeNanos = new ConcurrentHashMap<>();
    private final Semaphore taskAvailable = new Semaphore(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong dispatchedCount = new AtomicLong(0);
    private final ExecutorService dispatcherExecutor;
    private final Object submissionLock = new Object();
    private volatile Throwable dispatcherFailure;

    /**
     * Creates a new scheduler and starts the dispatcher thread.
     */
    public WeightedFairScheduler() {
        this.dispatcherExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "pangrova-weighted-fair-dispatcher");
            t.setDaemon(true);
            return t;
        });
        this.dispatcherExecutor.submit(() -> ExecutorDrain.runOwned(dispatcherExecutor, this::runDispatcher));
        LOG.info("WeightedFairScheduler started");
    }

    /**
     * Computes the weighted fair share for a queue type among the active queues.
     */
    public double computeShare(QueueType type, Set<QueueType> activeQueues) {
        if (activeQueues.isEmpty() || !activeQueues.contains(type)) {
            return 0.0;
        }
        double totalWeight = activeQueues.stream().mapToDouble(QueueType::getWeight).sum();
        return type.getWeight() / totalWeight;
    }

    /**
     * Submits a task to the specified queue type for weighted fair dispatch.
     *
     * @param task      the task to execute
     * @param queueType the queue type determining scheduling priority
     * @throws IllegalStateException if the scheduler has been shut down
     */
    public void submitTask(Runnable task, QueueType queueType) {
        synchronized (submissionLock) {
            if (!running.get()) {
                throw new IllegalStateException("WeightedFairScheduler is shut down");
            }
            taskQueues
                    .computeIfAbsent(queueType, k -> new ConcurrentLinkedQueue<>())
                    .add(task);
            taskAvailable.release();
        }
        LOG.debug("Task submitted to queue: {}", queueType);
    }

    /**
     * Returns the queue types that currently have pending tasks.
     */
    public Set<QueueType> getActiveQueues() {
        return taskQueues.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public Queue<Runnable> getQueue(QueueType queueType) {
        return new ConcurrentLinkedQueue<>(
                taskQueues.getOrDefault(queueType, new ConcurrentLinkedQueue<>()));
    }

    /**
     * Returns a metrics snapshot of the scheduler's current state.
     * Intended for observability integration.
     */
    public SchedulerMetrics getMetrics() {
        int totalPending = taskQueues.values().stream().mapToInt(Queue::size).sum();
        int activeCount = (int) taskQueues.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .count();
        return new SchedulerMetrics(totalPending, activeCount, dispatchedCount.get());
    }

    /**
     * Returns the accumulated CPU time in nanoseconds for the given queue type.
     * Returns 0 if the queue type has never been scheduled.
     */
    public long getCpuTimeNanos(QueueType queueType) {
        AtomicLong counter = cpuTimeNanos.get(queueType);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Returns a snapshot of per-group CPU time accumulators.
     */
    public Map<QueueType, Long> getCpuTimeSnapshot() {
        return cpuTimeNanos.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().get()));
    }

    @Override
    public void close() {
        synchronized (submissionLock) {
            if (running.getAndSet(false)) {
                taskAvailable.release(); // unblock dispatcher if parked
            }
        }
        ExecutorDrain.shutdownAndAwait(dispatcherExecutor, SHUTDOWN_TIMEOUT_MILLIS);
        if (!getActiveQueues().isEmpty()) {
            throw new IllegalStateException("Owned dispatcher exited with accepted callbacks still pending",
                    dispatcherFailure);
        }
        if (dispatcherFailure != null) {
            throw new IllegalStateException("Owned dispatcher failed before completing its accepted callback",
                    dispatcherFailure);
        }
        LOG.info("WeightedFairScheduler shut down, dispatched {} tasks total", dispatchedCount.get());
    }

    /**
     * The dispatcher loop that consumes tasks from queues in weighted fair order.
     *
     * <p>Each iteration computes the active queues and their shares, then dispatches
     * tasks proportionally. Higher-weight queues get more tasks per round. Each queue
     * gets at least 1 task per round if it has pending work.</p>
     */
    private void dispatchLoop() {
        LOG.debug("Dispatcher thread started");
        while (running.get() || !getActiveQueues().isEmpty()) {
            try {
                if (!taskAvailable.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    // Even without permits, check if queues have residual tasks
                    if (!getActiveQueues().isEmpty()) {
                        dispatchBatch(Integer.MAX_VALUE);
                    }
                    continue;
                }
                // Drain all available permits to batch-process
                int additionalPermits = taskAvailable.drainPermits();
                int totalToDispatch = 1 + additionalPermits;

                dispatchBatch(totalToDispatch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Owned dispatcher was interrupted outside a completed callback", e);
            } catch (Exception e) {
                LOG.error("Unexpected error in dispatcher loop", e);
            }
        }
        LOG.debug("Dispatcher thread exiting");
    }

    private void runDispatcher() {
        try {
            dispatchLoop();
        } catch (RuntimeException | Error failure) {
            synchronized (submissionLock) {
                dispatcherFailure = failure;
                running.set(false);
            }
            LOG.error("event=owned_dispatcher_failed component=WeightedFairScheduler", failure);
            throw failure;
        } finally {
            synchronized (submissionLock) {
                running.set(false);
            }
        }
    }

    /**
     * Dispatches up to {@code totalToDispatch} tasks using deficit-weighted fair
     * queuing: each slot is granted to the active queue that has received the least
     * CPU service per unit weight so far ({@code cpuTimeNanos / weight}). Because
     * {@link #executeTask} updates the accumulator after every task, an over-served
     * queue is progressively deprioritized within the same batch, so a long-running
     * queue cannot starve lighter ones and steady-state service converges to the
     * configured weight ratio. This closes the gap where {@code cpuTimeNanos} was
     * accumulated but never influenced scheduling.
     */
    private void dispatchBatch(int totalToDispatch) {
        int dispatched = 0;
        for (int slot = 0; slot < totalToDispatch; slot++) {
            Set<QueueType> activeQueues = getActiveQueues();
            if (activeQueues.isEmpty()) {
                break;
            }
            QueueType selected = selectUnderservedQueue(activeQueues, getCpuTimeSnapshot());
            if (selected == null) {
                break;
            }
            Queue<Runnable> queue = taskQueues.get(selected);
            Runnable task = queue != null ? queue.poll() : null;
            if (task == null) {
                continue; // raced empty; re-evaluate active queues on the next slot
            }
            executeTask(task, selected);
            dispatched++;
        }
        if (dispatched > 0) {
            dispatchedCount.addAndGet(dispatched);
            LOG.debug("Dispatched {} tasks in batch (deficit-weighted)", dispatched);
        }
    }

    /**
     * Selects the active queue with the lowest normalized service
     * ({@code cpuTimeNanos / weight}) — the queue furthest behind its fair share.
     * Ties (e.g. all queues idle) break toward the higher-weight queue so weight is
     * honored from the first dispatch. Package-private and pure for deterministic
     * unit testing.
     *
     * @return the selected queue, or {@code null} if {@code activeQueues} is empty
     */
    static QueueType selectUnderservedQueue(Set<QueueType> activeQueues,
                                            Map<QueueType, Long> cpuTimeByQueue) {
        QueueType best = null;
        double bestNormalized = Double.MAX_VALUE;
        for (QueueType type : activeQueues) {
            double weight = type.getWeight();
            long cpu = cpuTimeByQueue.getOrDefault(type, 0L);
            double normalized = weight > 0 ? cpu / weight : Double.MAX_VALUE;
            boolean better = normalized < bestNormalized
                    || (Double.compare(normalized, bestNormalized) == 0
                        && best != null && weight > best.getWeight());
            if (better) {
                bestNormalized = normalized;
                best = type;
            }
        }
        return best;
    }

    private void executeTask(Runnable task, QueueType queueType) {
        long startNanos = System.nanoTime();
        try {
            task.run();
        } catch (Exception e) {
            LOG.error("Task execution failed in queue {}", queueType, e);
        } finally {
            long elapsed = System.nanoTime() - startNanos;
            cpuTimeNanos.computeIfAbsent(queueType, k -> new AtomicLong(0)).addAndGet(elapsed);
            // This dedicated dispatcher owns its thread, not the completed callback.
            // As with a thread-pool task boundary, one callback's cancellation
            // must not interrupt another accepted callback or the queue wait.
            // Owner shutdown is signalled by running; close still preserves the
            // interrupt status of its separate calling thread.
            if (Thread.interrupted()) {
                LOG.debug("event=completed_callback_interrupt_cleared queueType={}", queueType);
            }
        }
    }

    /**
     * Immutable snapshot of scheduler metrics.
     *
     * @param totalPendingTasks total tasks across all queues
     * @param activeQueueCount  number of non-empty queues
     * @param totalDispatched   total tasks dispatched since creation
     */
    public record SchedulerMetrics(int totalPendingTasks, int activeQueueCount, long totalDispatched) {}
}
