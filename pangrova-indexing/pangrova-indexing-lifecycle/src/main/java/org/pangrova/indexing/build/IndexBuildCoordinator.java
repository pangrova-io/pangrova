package org.pangrova.indexing.build;

import org.pangrova.indexing.enums.IndexBuildError;
import org.pangrova.indexing.enums.IndexBuildOutcome;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IndexBuildCoordinator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IndexBuildCoordinator.class);

    private final IndexBuildContext context;
    private final IndexBuildMetrics metrics;
    private final IndexFileBuilder fileBuilder;
    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final int maxConcurrentBuilds;
    private final int maxConcurrentBuildsPerTenant;
    private final java.util.Map<String, Integer> activeBuildsByTenant = new java.util.HashMap<>();
    private final int batchFlushSize;
    private final AtomicInteger activeBuilds = new AtomicInteger();
    private final Object lifecycleLock = new Object();
    private final Queue<BuildAttempt> pendingBuilds = new ConcurrentLinkedQueue<>();
    private final Set<BuildAttempt> builds = new HashSet<>();
    private volatile boolean paused;
    private volatile boolean closed;

    /** Creates a coordinator from immutable build services and limits. */
    public IndexBuildCoordinator(IndexBuildContext context, IndexBuildMetrics metrics) {
        if (context == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        this.context = context;
        this.metrics = metrics;
        fileBuilder = context.canBuild() ? new IndexFileBuilder(context) : null;
        ownsExecutor = context.buildExecutor() == null;
        executor = ownsExecutor
                ? ManagedExecutorFactory.newFixedThreadPool("index-build", context.config().buildThreadPoolSize())
                : context.buildExecutor();
        maxConcurrentBuilds = context.config().buildParallelism();
        maxConcurrentBuildsPerTenant = context.config().runtime().build().maxConcurrentPerTenant();
        batchFlushSize = context.config().batchFlushSize();
    }

    /** Submits a build without a mapping callback. */
    public CompletableFuture<IndexBuildResult> submitBuildTask(
            IndexDescriptor descriptor,
            List<DataFileInfo> dataFiles,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader) {
        return submitBuildTask(descriptor, dataFiles, fileSystemAccessor, reader, null);
    }

    /** Submits a build and streams completed mapping batches to the supplied callback. */
    public CompletableFuture<IndexBuildResult> submitBuildTask(
            IndexDescriptor descriptor,
            List<DataFileInfo> dataFiles,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader,
            Consumer<List<BuildFileMapping>> mappingConsumer) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        BuildRequest request = new BuildRequest(
                descriptor,
                dataFiles == null ? List.of() : List.copyOf(dataFiles),
                fileSystemAccessor,
                reader,
                mappingConsumer);
        BuildAttempt attempt = new BuildAttempt(request);
        attempt.future.whenComplete((result, failure) -> {
            if (attempt.future.isCancelled()) {
                cancelAttempt(attempt);
            }
        });
        synchronized (lifecycleLock) {
            if (closed) {
                return rejectedFuture(descriptor.indexId(), "coordinator_closed");
            }
            builds.add(attempt);
            if (paused || !hasCapacity(attempt.request.descriptor().tenantId())) {
                pendingBuilds.add(attempt);
                LOG.info("event=index_build_deferred indexId={} tenant={} reason=paused_or_at_capacity",
                        descriptor.indexId(), descriptor.tenantId());
            } else {
                attempt.markStarted();
                attempt.markCapacityReserved();
                reserveCapacity(attempt.request.descriptor().tenantId());
                dispatch(attempt);
            }
            return attempt.future;
        }
    }

    /** Returns whether a new build can be dispatched immediately. */
    public boolean canAcceptBuild() {
        return !closed && !paused && activeBuilds.get() < maxConcurrentBuilds;
    }

    /** Returns whether a tenant build can be dispatched immediately. */
    public boolean canAcceptBuild(String tenantId) {
        synchronized (lifecycleLock) {
            return canAcceptBuild() && activeBuildsByTenant.getOrDefault(tenantId, 0)
                    < maxConcurrentBuildsPerTenant;
        }
    }

    private boolean hasCapacity(String tenantId) {
        return activeBuilds.get() < maxConcurrentBuilds
                && activeBuildsByTenant.getOrDefault(tenantId, 0) < maxConcurrentBuildsPerTenant;
    }

    private void reserveCapacity(String tenantId) {
        synchronized (lifecycleLock) {
            activeBuilds.incrementAndGet();
            activeBuildsByTenant.merge(tenantId, 1, Integer::sum);
        }
    }

    private void releaseCapacity(BuildAttempt attempt) {
        synchronized (attempt.lifecycleLock) {
            if (!attempt.capacityReserved) {
                return;
            }
            attempt.capacityReserved = false;
        }
        releaseCapacity(attempt.request.descriptor().tenantId());
    }

    private void releaseCapacity(String tenantId) {
        synchronized (lifecycleLock) {
            activeBuilds.decrementAndGet();
            int remaining = activeBuildsByTenant.getOrDefault(tenantId, 0) - 1;
            if (remaining <= 0) {
                activeBuildsByTenant.remove(tenantId);
            } else {
                activeBuildsByTenant.put(tenantId, remaining);
            }
        }
    }

    /** Cancels queued work and marks active work for cooperative cancellation. */
    public void cancelBuild(String indexId) {
        if (indexId == null) {
            return;
        }
        List<BuildAttempt> selected;
        synchronized (lifecycleLock) {
            selected = builds.stream()
                    .filter(attempt -> attempt.request.descriptor().indexId().equals(indexId))
                    .toList();
        }
        selected.forEach(this::cancelAttempt);
        dispatchPendingBuilds();
        LOG.info("event=index_build_cancelled indexId={}", indexId);
    }

    /** Pauses dispatch while retaining queued build requests. */
    public void pauseBuilds() {
        synchronized (lifecycleLock) {
            if (!closed) {
                paused = true;
                LOG.info("event=index_build_paused pendingBuilds={}", pendingBuilds.size());
            }
        }
    }

    /** Resumes dispatch of the requests already queued when this call acquires the lifecycle lock. */
    public void resumePendingBuilds() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            paused = false;
        }
        dispatchPendingBuilds();
        LOG.info("event=index_build_resumed pendingBuilds={}", pendingBuilds.size());
    }

    private void dispatchPendingBuilds() {
        List<BuildAttempt> ready = new ArrayList<>();
        synchronized (lifecycleLock) {
            int pending = pendingBuilds.size();
            for (int index = 0; index < pending; index++) {
                BuildAttempt attempt = pendingBuilds.poll();
                if (attempt == null) break;
                if (attempt.future.isDone() || attempt.stopped()) {
                    forget(attempt);
                } else if (!paused && hasCapacity(attempt.request.descriptor().tenantId())) {
                    attempt.markStarted();
                    attempt.markCapacityReserved();
                    reserveCapacity(attempt.request.descriptor().tenantId());
                    ready.add(attempt);
                } else {
                    pendingBuilds.add(attempt);
                }
            }
        }
        ready.forEach(this::dispatch);
    }

    @Override
    public void close() {
        List<BuildAttempt> selected;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            selected = List.copyOf(builds);
        }
        selected.forEach(this::cancelAttempt);
        if (!ownsExecutor) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException interruptedException) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void cancelAttempt(BuildAttempt attempt) {
        if (attempt.cancel()) {
            if (attempt.waiting()) {
                releaseCapacity(attempt);
                forget(attempt);
            }
        }
        if (!closed) {
            dispatchPendingBuilds();
        }
    }

    private void forget(BuildAttempt attempt) {
        synchronized (lifecycleLock) {
            builds.remove(attempt);
            pendingBuilds.remove(attempt);
        }
    }

    private void dispatch(BuildAttempt attempt) {
        if (attempt.future.isDone()) {
            releaseCapacity(attempt);
            forget(attempt);
            dispatchPendingBuilds();
            return;
        }
        try {
            executor.execute(() -> runAndComplete(attempt));
        } catch (RejectedExecutionException rejectedExecutionException) {
            releaseCapacity(attempt);
            recordError(0, IndexBuildError.REJECTED);
            LOG.warn("event=index_build_rejected indexId={} reason=executor_rejected",
                    attempt.request.descriptor().indexId());
            attempt.complete(IndexBuildResult.rejected());
            forget(attempt);
            dispatchPendingBuilds();
        }
    }

    private void runAndComplete(BuildAttempt attempt) {
        long startedAt = System.currentTimeMillis();
        buildStarted();
        try {
            IndexBuildResult result = runBuild(attempt, startedAt);
            if (result.outcome() == IndexBuildOutcome.COMPLETED
                    || result.outcome() == IndexBuildOutcome.CANCELLED) {
                recordBuildCompletion(attempt, result, startedAt);
            }
            attempt.complete(Thread.currentThread().isInterrupted() ? IndexBuildResult.cancelled() : result);
        } catch (RuntimeException runtimeException) {
            LOG.error(
                    "event=index_build_failed indexId={} error={}",
                    attempt.request.descriptor().indexId(),
                    runtimeException.getMessage(),
                    runtimeException);
            recordError(System.currentTimeMillis() - startedAt, IndexBuildError.UNKNOWN);
            attempt.fail(runtimeException);
        } finally {
            buildFinished();
            releaseCapacity(attempt);
            forget(attempt);
            dispatchPendingBuilds();
        }
    }

    private IndexBuildResult runBuild(BuildAttempt attempt, long startedAt) {
        BuildRequest request = attempt.request;
        IndexDescriptor descriptor = request.descriptor();
        if (attempt.stopped()) {
            return IndexBuildResult.cancelled();
        }
        if (!tryClaim(attempt)) {
            LOG.info(
                    "event=index_build_claim_lost indexId={} generation={}",
                    descriptor.indexId(),
                    descriptor.generation());
            recordError(System.currentTimeMillis() - startedAt, IndexBuildError.CLAIM_FAILED);
            return IndexBuildResult.claimFailed();
        }
        SnapshotPinOwner pinOwner = null;
        boolean pinned = false;
        boolean sourceReadsStarted = false;
        IndexBuildResult result = null;
        Throwable failure = null;
        try {
            if (attempt.stopped()) {
                return IndexBuildResult.cancelled();
            }
            if (!isGenerationCurrent(descriptor)) {
                LOG.info(
                        "event=index_build_stale_generation indexId={} generation={}",
                        descriptor.indexId(),
                        descriptor.generation());
                recordError(System.currentTimeMillis() - startedAt, IndexBuildError.STALE_GENERATION);
                return IndexBuildResult.staleGeneration();
            }
            pinOwner = snapshotPinOwner(attempt);
            if (pinOwner != null) {
                pinBuildSnapshot(attempt, pinOwner);
                pinned = true;
            }
            sourceReadsStarted = true;
            result = buildFiles(attempt);
        } catch (RuntimeException | Error buildFailure) {
            failure = buildFailure;
        } finally {
            try {
                finishSnapshotPinAttempt(attempt, pinOwner, pinned, sourceReadsStarted, result, failure);
            } catch (RuntimeException releaseFailure) {
                if (failure == null) {
                    failure = releaseFailure;
                } else if (failure != releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            try {
                releaseClaim(attempt);
            } catch (RuntimeException releaseFailure) {
                if (failure == null) {
                    failure = releaseFailure;
                } else if (failure != releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return result;
    }

    private void finishSnapshotPinAttempt(BuildAttempt attempt, SnapshotPinOwner owner,
            boolean pinned, boolean sourceReadsStarted, IndexBuildResult result, Throwable failure) {
        if (owner == null) {
            return;
        }
        boolean successful = failure == null && result != null && result.isFullySuccessful();
        if (context.snapshotPinProvider() instanceof ScalarSnapshotPinLifecycle lifecycle) {
            // A failed/partial read may leave connector iterators open. Never infer
            // drain from claim expiry or an exceptional reading stack.
            if (!sourceReadsStarted || successful) {
                lifecycle.markBuildDrained(owner, attempt.request.descriptor().buildSnapshotId());
            }
        } else if (pinned && !successful) {
            releaseBuildSnapshot(attempt, owner);
        }
    }

    private void recordBuildCompletion(BuildAttempt attempt, IndexBuildResult result, long startedAt) {
        BuildRequest request = attempt.request;
        long durationMs = System.currentTimeMillis() - startedAt;
        if (metrics != null) {
            metrics.recordBuild(durationMs, result.successCount(), result.failureCount());
        }
        LOG.info(
                "event=index_build_completed indexId={} succeeded={} failed={} durationMs={} outcome={}",
                request.descriptor().indexId(),
                result.successCount(),
                result.failureCount(),
                durationMs,
                result.outcome());
    }

    private IndexBuildResult buildFiles(BuildAttempt attempt) {
        BuildRequest request = attempt.request;
        if (attempt.stopped()) {
            return IndexBuildResult.cancelled();
        }
        if (request.dataFiles().isEmpty()) {
            LOG.info("event=index_build_no_files indexId={}", request.descriptor().indexId());
            return IndexBuildResult.success(0);
        }
        if (fileBuilder == null) {
            LOG.warn(
                    "event=index_build_unavailable indexId={} files={} reason=incomplete_context",
                    request.descriptor().indexId(),
                    request.dataFiles().size());
            return new IndexBuildResult(0, request.dataFiles().size());
        }
        BuildTally tally = new BuildTally();
        List<BuildFileMapping> batch = new ArrayList<>(batchFlushSize);
        for (DataFileInfo file : request.dataFiles()) {
            if (attempt.stopped()) {
                LOG.info("event=index_build_stopped indexId={} reason=cancelled", request.descriptor().indexId());
                return IndexBuildResult.cancelled();
            }
            buildOneFile(attempt, file, batch, tally);
            if (attempt.stopped()) {
                return IndexBuildResult.cancelled();
            }
            flushIfFull(request.mappingConsumer(), batch);
        }
        if (attempt.stopped()) {
            return IndexBuildResult.cancelled();
        }
        flush(request.mappingConsumer(), batch);
        return attempt.stopped() ? IndexBuildResult.cancelled()
                : new IndexBuildResult(tally.succeeded, tally.failed);
    }

    private void buildOneFile(
            BuildAttempt attempt, DataFileInfo file, List<BuildFileMapping> batch, BuildTally tally) {
        BuildRequest request = attempt.request;
        BuildFileMapping mapping;
        try {
            mapping = fileBuilder.build(
                    request.descriptor(), attempt.attemptId, file, request.fileSystemAccessor(),
                    request.reader(), attempt::stopped);
        } catch (Exception exception) {
            tally.failed++;
            LOG.warn(
                    "event=index_build_file_failed indexId={} file={} error={}",
                    request.descriptor().indexId(), file.filePath(), exception.getMessage());
            return;
        }
        if (attempt.stopped()) {
            return;
        }
        // Ownership failure is terminal, not a recoverable per-file build failure.
        heartbeat(attempt);
        batch.add(mapping);
        tally.succeeded++;
    }

    private void flushIfFull(Consumer<List<BuildFileMapping>> consumer, List<BuildFileMapping> batch) {
        if (batch.size() >= batchFlushSize) {
            flush(consumer, batch);
        }
    }

    private static void flush(Consumer<List<BuildFileMapping>> consumer, List<BuildFileMapping> batch) {
        if (consumer != null && !batch.isEmpty()) {
            consumer.accept(List.copyOf(batch));
        }
        batch.clear();
    }

    private boolean tryClaim(BuildAttempt attempt) {
        IndexBuildClaimRepository repository = context.claimRepository();
        IndexDescriptor descriptor = attempt.request.descriptor();
        return repository == null || repository.tryClaim(
                descriptor.indexId(), descriptor.generation(), attempt.ownerId, Instant.now());
    }

    private void releaseClaim(BuildAttempt attempt) {
        IndexBuildClaimRepository repository = context.claimRepository();
        IndexDescriptor descriptor = attempt.request.descriptor();
        if (repository != null && !repository.release(
                descriptor.indexId(), descriptor.generation(), attempt.ownerId)) {
            throw new IllegalStateException("Index build claim lost before release: " + descriptor.indexId());
        }
    }

    private void heartbeat(BuildAttempt attempt) {
        IndexBuildClaimRepository repository = context.claimRepository();
        IndexDescriptor descriptor = attempt.request.descriptor();
        if (repository != null && !repository.heartbeat(
                descriptor.indexId(), descriptor.generation(), attempt.ownerId, Instant.now())) {
            throw new IllegalStateException("Index build claim lost before mapping: " + descriptor.indexId());
        }
    }

    private boolean isGenerationCurrent(IndexDescriptor descriptor) {
        GenerationChecker checker = context.generationChecker();
        return checker == null || checker.isCurrentGeneration(descriptor.indexId(), descriptor.generation());
    }

    private SnapshotPinOwner snapshotPinOwner(BuildAttempt attempt) {
        IndexDescriptor descriptor = attempt.request.descriptor();
        return context.snapshotPinProvider() == null || descriptor.buildSnapshotId() == null
                || descriptor.buildSnapshotId().isEmpty() ? null : new SnapshotPinOwner(
                descriptor.indexId(), descriptor.generation(), attempt.attemptId.toString());
    }

    private void pinBuildSnapshot(BuildAttempt attempt, SnapshotPinOwner owner) {
        IndexDescriptor descriptor = attempt.request.descriptor();
        try {
            context.snapshotPinProvider().pinSnapshot(
                    descriptor.catalogName(),
                    descriptor.schemaName(),
                    descriptor.tableName(),
                    descriptor.buildSnapshotId(),
                    owner);
        } catch (RuntimeException runtimeException) {
            LOG.error(
                    "event=index_build_pin_failed indexId={} snapshotId={} error={}",
                    descriptor.indexId(),
                    descriptor.buildSnapshotId(),
                    runtimeException.getMessage(), runtimeException);
            throw runtimeException;
        }
    }

    private void releaseBuildSnapshot(BuildAttempt attempt, SnapshotPinOwner owner) {
        IndexDescriptor descriptor = attempt.request.descriptor();
        try {
            context.snapshotPinProvider().unpinSnapshot(
                    descriptor.catalogName(),
                    descriptor.schemaName(),
                    descriptor.tableName(),
                    descriptor.buildSnapshotId(),
                    owner);
        } catch (RuntimeException releaseFailure) {
            LOG.error("event=index_build_pin_release_failed indexId={} snapshotId={} owner={} error={}",
                    descriptor.indexId(), descriptor.buildSnapshotId(), owner.id(),
                    releaseFailure.getMessage(), releaseFailure);
            throw releaseFailure;
        }
    }

    private void buildStarted() {
        if (metrics != null) {
            metrics.buildStarted();
        }
    }

    private void buildFinished() {
        if (metrics != null) {
            metrics.buildFinished();
        }
    }

    private void recordError(long durationMs, IndexBuildError type) {
        if (metrics != null) {
            metrics.recordError(durationMs, type);
        }
    }

    private static CompletableFuture<IndexBuildResult> rejectedFuture(String indexId, String reason) {
        LOG.info("event=index_build_rejected indexId={} reason={}", indexId, reason);
        return CompletableFuture.completedFuture(IndexBuildResult.rejected());
    }

    private record BuildRequest(
            IndexDescriptor descriptor,
            List<DataFileInfo> dataFiles,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader,
            Consumer<List<BuildFileMapping>> mappingConsumer) {}

    /** Owns cancellation intent and serializes it with this submission's terminal result. */
    private static final class BuildAttempt {
        private final BuildRequest request;
        private final UUID attemptId = UUID.randomUUID();
        private final String ownerId = attemptId.toString();
        private final CompletableFuture<IndexBuildResult> future = new CompletableFuture<>();
        private final Object lifecycleLock = new Object();
        private boolean started;
        private boolean cancelled;
        private boolean terminalChosen;
        private boolean capacityReserved;

        private BuildAttempt(BuildRequest request) {
            this.request = request;
        }

        void markStarted() {
            synchronized (lifecycleLock) {
                started = true;
            }
        }

        boolean cancel() {
            synchronized (this) {
                if (cancelled || future.isDone()) {
                    return false;
                }
                cancelled = true;
                if (!started) {
                    terminalChosen = true;
                }
            }
            if (!started) {
                future.complete(IndexBuildResult.cancelled());
            }
            return true;
        }

        void markCapacityReserved() {
            synchronized (lifecycleLock) {
                capacityReserved = true;
            }
        }

        boolean waiting() {
            synchronized (lifecycleLock) {
                return !started;
            }
        }

        synchronized boolean stopped() {
            return cancelled || future.isDone() || Thread.currentThread().isInterrupted();
        }

        void complete(IndexBuildResult result) {
            IndexBuildResult terminal;
            synchronized (this) {
                if (terminalChosen) {
                    return;
                }
                terminalChosen = true;
                terminal = cancelled ? IndexBuildResult.cancelled() : result;
            }
            future.complete(terminal);
        }

        void fail(RuntimeException failure) {
            boolean cancellation;
            synchronized (this) {
                if (terminalChosen) {
                    return;
                }
                terminalChosen = true;
                cancellation = cancelled;
            }
            if (cancellation) {
                future.complete(IndexBuildResult.cancelled());
            } else {
                future.completeExceptionally(failure);
            }
        }
    }

    private static final class BuildTally {
        private int succeeded;
        private int failed;
    }
}
