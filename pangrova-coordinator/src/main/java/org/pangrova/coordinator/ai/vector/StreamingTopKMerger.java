package org.pangrova.coordinator.ai.vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Top-K merger for distributed vector search with early termination.
 *
 * <p>Unlike the batch-mode {@link GlobalTopKMerger} which waits for ALL worker
 * results before merging, this streaming variant processes results as they arrive
 * and supports early termination when sufficient high-quality results have been
 * collected.</p>
 *
 * <h3>Early Termination Strategy</h3>
 * <p>When a worker returns its local Top-K sorted by distance, and we already have
 * K results with maximum distance D, any subsequent worker whose minimum possible
 * distance exceeds D cannot contribute to the final result. This allows skipping
 * slow workers without sacrificing result quality.</p>
 *
 * <h3>Distributed Scenario Support</h3>
 * <ul>
 *   <li><b>Shard-parallel</b>: Each worker searches its own index shard; results
 *       stream in as CompletableFutures complete</li>
 *   <li><b>Scatter-gather</b>: All workers search independently; merge on coordinator</li>
 *   <li><b>Confidence threshold</b>: Terminate when heapMaxDistance drops below a configurable
 *       threshold relative to the first result (indicating convergence)</li>
 * </ul>
 *
 * <p>Thread-safe: designed for concurrent result submission from multiple worker
 * response handlers.</p>
 */
public final class StreamingTopKMerger {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingTopKMerger.class);

    /** Interval between completion checks while waiting for worker results. */
    private static final long POLL_INTERVAL_MS = 10;

    private final int topK;
    private final int expectedWorkerCount;
    private final long earlyTerminationTimeoutMs;
    private final double convergenceRatio;

    // Thread-safe state
    private final PriorityQueue<GlobalTopKMerger.ScoredRow> heap;
    private final Set<String> seen;
    private final AtomicInteger completedWorkers = new AtomicInteger(0);
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Object heapLock = new Object();

    // Metrics
    private long totalScanned = 0;
    private int bruteForceCount = 0;

    /**
     * Configuration for the streaming merger.
     *
     * @param topK                      global top-K to retain
     * @param expectedWorkerCount       total workers expected to respond
     * @param earlyTerminationTimeoutMs max time to wait for all workers (0 = no timeout)
     * @param convergenceRatio          terminate early if heapMaxDist/heapMinDist < ratio (0 = disabled)
     */
    public record Config(int topK, int expectedWorkerCount,
                         long earlyTerminationTimeoutMs, double convergenceRatio) {
        public Config {
            if (topK <= 0) throw new IllegalArgumentException("topK must be positive");
            if (expectedWorkerCount <= 0) throw new IllegalArgumentException("expectedWorkerCount must be positive");
        }

        /** Default config: no early termination timeout, no convergence ratio. */
        public static Config of(int topK, int expectedWorkerCount) {
            return new Config(topK, expectedWorkerCount, 0, 0.0);
        }

        /** Config with early termination after the given timeout. */
        public static Config withTimeout(int topK, int expectedWorkerCount, long timeoutMs) {
            return new Config(topK, expectedWorkerCount, timeoutMs, 0.0);
        }

        /** Config with convergence-based early termination. */
        public static Config withConvergence(int topK, int expectedWorkerCount, double ratio) {
            return new Config(topK, expectedWorkerCount, 0, ratio);
        }
    }

    /**
     * Streaming merge result.
     *
     * @param rows             final Top-K results sorted by distance ascending
     * @param totalScanned     total rows scanned across all workers that responded
     * @param respondedWorkers number of workers that submitted results
     * @param earlyTerminated  whether early termination was triggered
     * @param terminationReason reason for early termination (null if completed normally)
     */
    public record StreamingResult(
            List<GlobalTopKMerger.ScoredRow> rows,
            long totalScanned,
            int respondedWorkers,
            boolean earlyTerminated,
            String terminationReason) {

        public StreamingResult {
            rows = List.copyOf(rows);
        }
    }

    /** Creates a merger configured for the given top-K and early-termination policy. */
    public StreamingTopKMerger(Config config) {
        this.topK = config.topK();
        this.expectedWorkerCount = config.expectedWorkerCount();
        this.earlyTerminationTimeoutMs = config.earlyTerminationTimeoutMs();
        this.convergenceRatio = config.convergenceRatio();
        this.heap = new PriorityQueue<>(
                Math.max(topK, 1),
                Comparator.comparingDouble(GlobalTopKMerger.ScoredRow::distance).reversed());
        this.seen = new HashSet<>();
    }

    /**
     * Submits a single worker's results into the streaming merge.
     *
     * <p>Thread-safe: may be called concurrently from multiple async response handlers.</p>
     *
     * @param workerResults results from one worker, sorted by distance ascending
     * @return true if more results are needed; false if early termination triggered
     */
    public boolean submitWorkerResult(List<GlobalTopKMerger.ScoredRow> workerResults) {
        if (terminated.get()) {
            return false;
        }

        synchronized (heapLock) {
            mergeRows(workerResults);
        }

        return completeWorker();
    }

    private void mergeRows(List<GlobalTopKMerger.ScoredRow> workerResults) {
        for (GlobalTopKMerger.ScoredRow row : workerResults) {
            totalScanned++;
            if (row.fromBruteForce()) {
                bruteForceCount++;
            }
            String dedupKey = row.filePath() + ":" + row.rowId();
            if (!seen.add(dedupKey)) {
                continue;
            }

            // Early termination: if heap is full and this row's distance exceeds
            // the current heap max, AND it's sorted, remaining rows can't help either.
            if (heap.size() >= topK && row.distance() > heap.peek().distance()) {
                // All subsequent rows from this worker are worse; stop processing.
                break;
            }

            heap.offer(row);
            if (heap.size() > topK) {
                heap.poll();
            }
        }
    }

    private boolean completeWorker() {
        int completed = completedWorkers.incrementAndGet();

        if (tryConvergenceTermination(completed)) {
            return false;
        }

        // Check if all workers have responded
        if (completed >= expectedWorkerCount) {
            terminated.set(true);
            return false;
        }

        return true;  // more results needed
    }

    /**
     * Checks convergence-based early termination and signals if triggered.
     */
    private boolean tryConvergenceTermination(int completed) {
        if (convergenceRatio <= 0 || heap.size() < topK) {
            return false;
        }
        if (!checkConvergence()) {
            return false;
        }
        terminated.set(true);
        LOG.info("event=streaming_topk_early_terminated reason=convergence "
                + "completed={}/{} topK={}", completed, expectedWorkerCount, topK);
        return true;
    }

    /**
     * Performs streaming merge across multiple worker futures with early termination.
     *
     * <p>Waits for worker results to arrive as CompletableFutures complete. Applies
     * early termination when convergence is detected or timeout is reached.</p>
     *
     * @param workerFutures futures producing per-worker result lists
     * @return merged streaming result
     */
    public StreamingResult mergeAsync(
            List<CompletableFuture<List<GlobalTopKMerger.ScoredRow>>> workerFutures) {
        Objects.requireNonNull(workerFutures, "workerFutures required");

        // Register completion callbacks for each worker future
        for (CompletableFuture<List<GlobalTopKMerger.ScoredRow>> future : workerFutures) {
            registerWorkerCallback(future);
        }

        // Wait for completion or early termination
        long deadline = earlyTerminationTimeoutMs > 0
                ? System.currentTimeMillis() + earlyTerminationTimeoutMs
                : Long.MAX_VALUE;
        return awaitWorkers(deadline);
    }

    private void registerWorkerCallback(CompletableFuture<List<GlobalTopKMerger.ScoredRow>> future) {
        future.thenAccept(this::submitWorkerResult)
                .exceptionally(ex -> {
                    completedWorkers.incrementAndGet();
                    LOG.warn("event=worker_vector_result_failed error={}", ex.getMessage());
                    return null;
                });
    }

    private StreamingResult awaitWorkers(long deadline) {
        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "streaming-topk-poller");
            thread.setDaemon(true);
            return thread;
        });
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<String> reason = new AtomicReference<>();
        poller.scheduleAtFixedRate(() -> checkCompletion(deadline, finished, reason),
                0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        try {
            finished.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            terminated.set(true);
            return buildResult("interrupted");
        } finally {
            poller.shutdownNow();
        }
        return buildResult(reason.get());
    }

    private void checkCompletion(long deadline, CountDownLatch finished, AtomicReference<String> reason) {
        if (terminated.get() || completedWorkers.get() >= expectedWorkerCount) {
            finished.countDown();
            return;
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            terminated.set(true);
            LOG.info("event=streaming_topk_timeout completed={}/{} topK={}",
                    completedWorkers.get(), expectedWorkerCount, topK);
            reason.set("timeout");
            finished.countDown();
        }
    }

    /**
     * Builds the final result from the current heap state.
     * Can be called at any time (e.g., after early termination).
     */
    public StreamingResult buildResult(String terminationReason) {
        List<GlobalTopKMerger.ScoredRow> merged;
        long scanned;
        int responded;

        synchronized (heapLock) {
            merged = new ArrayList<>(heap);
            merged.sort(Comparator.comparingDouble(GlobalTopKMerger.ScoredRow::distance));
            scanned = totalScanned;
        }
        responded = completedWorkers.get();
        boolean earlyTerminated = terminationReason != null
                || responded < expectedWorkerCount;

        LOG.debug("event=streaming_topk_result topK={} resultSize={} scanned={} "
                + "responded={}/{} earlyTerminated={}",
                topK, merged.size(), scanned, responded, expectedWorkerCount, earlyTerminated);

        return new StreamingResult(
                merged, scanned, responded, earlyTerminated, terminationReason);
    }

    /**
     * Checks if the heap has converged (max distance / min distance < convergenceRatio).
     * When converged, additional workers are unlikely to contribute better results.
     */
    private boolean checkConvergence() {
        synchronized (heapLock) {
            if (heap.size() < topK) return false;

            // Peek max (heap is max-heap, peek = largest distance)
            double maxDist = heap.peek().distance();

            // Find min distance in heap
            double minDist = Double.MAX_VALUE;
            for (GlobalTopKMerger.ScoredRow row : heap) {
                if (row.distance() < minDist) {
                    minDist = row.distance();
                }
            }

            if (minDist <= 0) return false;
            double ratio = maxDist / minDist;
            return ratio <= (1.0 + convergenceRatio);
        }
    }

    /** Returns true if early termination has been triggered. */
    public boolean isTerminated() {
        return terminated.get();
    }
}
