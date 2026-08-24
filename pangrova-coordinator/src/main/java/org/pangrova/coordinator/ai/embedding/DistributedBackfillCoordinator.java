package org.pangrova.coordinator.ai.embedding;

import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillRequest;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillResult;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillState;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed batch processing coordinator for embedding backfill.
 *
 * <p>Orchestrates the full backfill lifecycle: snapshot acquisition, row scanning,
 * work partitioning, Worker dispatch, incremental commit, and checkpoint persistence.
 * Supports cancel/resume via checkpoint-based recovery.</p>
 *
 * <p>Rate limiting is enforced via a semaphore controlling concurrent Worker dispatches.
 * Failed rows are skipped (not retried in the same run) and counted for reporting.</p>
 */
@ThreadSafe
public final class DistributedBackfillCoordinator implements BackfillCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedBackfillCoordinator.class);

    private final BackfillRowScanner rowScanner;
    private final BackfillPartitioner partitioner;
    private final BackfillWorkerDispatcher workerDispatcher;
    private final BackfillCheckpointStore checkpointStore;
    private final ExecutorService executor;
    private final ConcurrentMap<String, BackfillJob> activeJobs = new ConcurrentHashMap<>();

    /**
     * @param rowScanner       scans rows matching the backfill filter at a given snapshot
     * @param partitioner      partitions scanned rows into Worker-sized batches
     * @param workerDispatcher dispatches embedding work to Workers and collects results
     * @param checkpointStore  persists and retrieves backfill checkpoints
     */
    public DistributedBackfillCoordinator(
            BackfillRowScanner rowScanner,
            BackfillPartitioner partitioner,
            BackfillWorkerDispatcher workerDispatcher,
            BackfillCheckpointStore checkpointStore) {
        if (rowScanner == null) {
            throw new IllegalArgumentException("rowScanner required");
        }
        if (partitioner == null) {
            throw new IllegalArgumentException("partitioner required");
        }
        if (workerDispatcher == null) {
            throw new IllegalArgumentException("workerDispatcher required");
        }
        if (checkpointStore == null) {
            throw new IllegalArgumentException("checkpointStore required");
        }
        this.rowScanner = rowScanner;
        this.partitioner = partitioner;
        this.workerDispatcher = workerDispatcher;
        this.checkpointStore = checkpointStore;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "backfill-coordinator-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public CompletableFuture<BackfillResult> start(BackfillRequest request) {
        String jobKey = jobKey(request.tableName(), request.columnName());
        BackfillJob job = new BackfillJob(request);

        activeJobs.compute(jobKey, (k, prev) -> {
            if (prev != null && prev.isActive()) {
                throw new BackfillAlreadyRunningException(request.tableName(), request.columnName());
            }
            return job;
        });

        LOG.info("Backfill started: table={}, column={}, batchSize={}, concurrency={}",
                request.tableName(), request.columnName(), request.batchSize(), request.concurrency());

        return CompletableFuture.supplyAsync(() -> executeBackfill(job), executor);
    }

    @Override
    public boolean cancel(String tableName, String columnName) {
        String jobKey = jobKey(tableName, columnName);
        BackfillJob job = activeJobs.get(jobKey);
        if (job == null || !job.isActive()) {
            return false;
        }
        job.requestCancel();
        LOG.info("Backfill cancel requested: table={}, column={}", tableName, columnName);
        return true;
    }

    @Override
    public Optional<BackfillStatus> getStatus(String tableName) {
        return activeJobs.values().stream()
                .filter(job -> job.request.tableName().equals(tableName))
                .findFirst()
                .map(BackfillJob::toStatus);
    }

    private BackfillResult executeBackfill(BackfillJob job) {
        BackfillRequest request = job.request;
        try {
            job.transitionTo(BackfillState.SCANNING);
            long snapshotId = rowScanner.acquireSnapshot(request.tableName());
            job.snapshotId = snapshotId;

            ResumeState resume = loadResumeState(request);
            job.processedRows.set(resume.processedRows);
            job.failedRows.set(resume.failedRows);

            List<BackfillPartition> partitions = partitioner.partition(
                    request.tableName(), request.columnName(),
                    request.filter(), snapshotId, request.batchSize());
            job.totalRows = partitions.stream().mapToLong(BackfillPartition::rowCount).sum();

            dispatchPartitions(job, partitions, resume.startPartition, snapshotId, resume.backfillId);
            return buildResult(job, snapshotId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            job.transitionTo(BackfillState.FAILED);
            throw new BackfillExecutionException(request.tableName(), request.columnName(),
                    "Backfill interrupted", e);
        } catch (BackfillExecutionException e) {
            throw e;
        } catch (Exception e) {
            job.transitionTo(BackfillState.FAILED);
            LOG.error("Backfill failed: table={}, column={}, error={}",
                    request.tableName(), request.columnName(), e.getMessage(), e);
            throw new BackfillExecutionException(request.tableName(), request.columnName(),
                    e.getMessage(), e);
        }
    }

    private void dispatchPartitions(BackfillJob job, List<BackfillPartition> partitions,
                                    int startPartition, long snapshotId, String backfillId)
            throws InterruptedException {
        BackfillRequest request = job.request;
        job.transitionTo(BackfillState.RUNNING);
        Semaphore concurrencyLimit = new Semaphore(request.concurrency());

        for (int i = startPartition; i < partitions.size(); i++) {
            if (job.isCancelled()) {
                job.transitionTo(BackfillState.CANCELLING);
                break;
            }
            concurrencyLimit.acquire();
            try {
                processPartition(job, partitions.get(i), i, partitions.size(),
                        snapshotId, backfillId);
            } finally {
                concurrencyLimit.release();
            }
        }
    }

    private void processPartition(BackfillJob job, BackfillPartition partition,
                                  int index, int totalPartitions,
                                  long snapshotId, String backfillId) {
        BackfillRequest request = job.request;
        BackfillBatchResult batchResult = workerDispatcher.dispatch(
                request.tableName(), request.columnName(),
                request.modelName(), partition, snapshotId);

        job.processedRows.addAndGet(batchResult.processedRows());
        job.failedRows.addAndGet(batchResult.failedRows());

        BackfillCheckpoint checkpoint = new BackfillCheckpoint(
                backfillId, request.tableName(), request.columnName(),
                snapshotId, index, partition.rowCount(),
                job.processedRows.get(), job.failedRows.get(), Instant.now());
        checkpointStore.save(checkpoint);
        job.lastCheckpoint = backfillId + ":" + index;

        LOG.debug("Backfill partition committed: table={}, column={}, partition={}/{}, processed={}, failed={}",
                request.tableName(), request.columnName(),
                index + 1, totalPartitions,
                job.processedRows.get(), job.failedRows.get());
    }

    private ResumeState loadResumeState(BackfillRequest request) {
        Optional<BackfillCheckpoint> resumePoint =
                checkpointStore.load(request.tableName(), request.columnName());
        int startPartition = resumePoint.map(cp -> cp.partitionIndex() + 1).orElse(0);
        long priorProcessed = resumePoint.map(BackfillCheckpoint::processedRows).orElse(0L);
        long priorFailed = resumePoint.map(BackfillCheckpoint::failedRows).orElse(0L);
        String backfillId = resumePoint.map(BackfillCheckpoint::backfillId)
                .orElse(UUID.randomUUID().toString());
        return new ResumeState(startPartition, priorProcessed, priorFailed, backfillId);
    }

    private BackfillResult buildResult(BackfillJob job, long snapshotId) {
        job.transitionTo(BackfillState.COMPLETED);
        checkpointStore.remove(job.request.tableName(), job.request.columnName());

        Duration elapsed = Duration.between(job.startTime, Instant.now());
        BackfillResult result = new BackfillResult(
                job.request.tableName(), job.request.columnName(),
                job.totalRows, job.processedRows.get(), job.failedRows.get(),
                elapsed, job.isCancelled(), snapshotId);

        LOG.info("Backfill completed: table={}, column={}, total={}, processed={}, failed={}, elapsed={}, cancelled={}",
                result.tableName(), result.columnName(),
                result.totalRows(), result.processedRows(), result.failedRows(),
                elapsed, result.cancelled());
        return result;
    }

    private static String jobKey(String tableName, String columnName) {
        return tableName + "::" + columnName;
    }

    private record ResumeState(int startPartition, long processedRows, long failedRows, String backfillId) {}

    /**
     * Mutable state holder for an in-progress backfill job.
     * Access is thread-safe via atomic fields and volatile state.
     */
    private static final class BackfillJob {
        final BackfillRequest request;
        final Instant startTime;
        final AtomicLong processedRows = new AtomicLong();
        final AtomicLong failedRows = new AtomicLong();
        final AtomicReference<BackfillState> state = new AtomicReference<>(BackfillState.SCANNING);
        volatile long totalRows;
        volatile long snapshotId;
        volatile String lastCheckpoint = "";
        private volatile boolean cancelRequested;

        BackfillJob(BackfillRequest request) {
            this.request = request;
            this.startTime = Instant.now();
        }

        void transitionTo(BackfillState newState) {
            state.set(newState);
        }

        void requestCancel() {
            cancelRequested = true;
        }

        boolean isCancelled() {
            return cancelRequested;
        }

        boolean isActive() {
            BackfillState current = state.get();
            return current == BackfillState.SCANNING || current == BackfillState.RUNNING;
        }

        BackfillStatus toStatus() {
            return new BackfillStatus(
                    request.tableName(), request.columnName(),
                    state.get(), totalRows, processedRows.get(), failedRows.get(),
                    startTime, lastCheckpoint, snapshotId);
        }
    }
}
