package org.pangrova.indexing.build;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.indexing.metrics.IndexBuildMetrics;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.IndexDescriptor;
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
    private final int batchFlushSize;
    private final String ownerId;
    private final AtomicInteger activeBuilds = new AtomicInteger();
    private final Queue<PendingBuild> pendingBuilds = new ConcurrentLinkedQueue<>();
    private final Set<String> cancelledIndexes = ConcurrentHashMap.newKeySet();
    private volatile boolean paused;
    private volatile boolean closed;

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
        batchFlushSize = context.config().batchFlushSize();
        ownerId = "builder-" + UUID.randomUUID();
    }

    public CompletableFuture<IndexBuildResult> submitBuildTask(
            IndexDescriptor descriptor,
            List<DataFileInfo> dataFiles,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader) {
        return submitBuildTask(descriptor, dataFiles, fileSystemAccessor, reader, null);
    }

    public CompletableFuture<IndexBuildResult> submitBuildTask(
            IndexDescriptor descriptor,
            List<DataFileInfo> dataFiles,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader,
            Consumer<List<BuildFileMapping>> mappingConsumer) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (closed) {
            return rejectedFuture(descriptor.indexId(), "coordinator_closed");
        }
        BuildRequest request = new BuildRequest(
                descriptor,
                dataFiles == null ? List.of() : List.copyOf(dataFiles),
                fileSystemAccessor,
                reader,
                mappingConsumer);
        CompletableFuture<IndexBuildResult> future = new CompletableFuture<>();
        cancelledIndexes.remove(descriptor.indexId());
        if (paused) {
            pendingBuilds.add(new PendingBuild(request, future));
            LOG.info("event=index_build_deferred indexId={} reason=paused", descriptor.indexId());
            return future;
        }
        dispatch(request, future);
        return future;
    }

    public boolean canAcceptBuild() {
        return !closed && !paused && activeBuilds.get() < maxConcurrentBuilds;
    }

    public void cancelBuild(String indexId) {
        if (indexId == null) {
            return;
        }
        cancelledIndexes.add(indexId);
        pendingBuilds.removeIf(pending -> {
            boolean matches = pending.request().descriptor().indexId().equals(indexId);
            if (matches) {
                pending.future().complete(IndexBuildResult.rejected());
            }
            return matches;
        });
        LOG.info("event=index_build_cancelled indexId={}", indexId);
    }

    public void pauseBuilds() {
        paused = true;
        LOG.info("event=index_build_paused pendingBuilds={}", pendingBuilds.size());
    }

    public void resumePendingBuilds() {
        paused = false;
        int resumed = 0;
        PendingBuild pending;
        while ((pending = pendingBuilds.poll()) != null) {
            dispatch(pending.request(), pending.future());
            resumed++;
        }
        LOG.info("event=index_build_resumed resumedBuilds={}", resumed);
    }

    @Override
    public void close() {
        closed = true;
        PendingBuild pending;
        while ((pending = pendingBuilds.poll()) != null) {
            pending.future().complete(IndexBuildResult.rejected());
        }
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

    private void dispatch(BuildRequest request, CompletableFuture<IndexBuildResult> future) {
        activeBuilds.incrementAndGet();
        try {
            executor.execute(() -> runAndComplete(request, future));
        } catch (RejectedExecutionException rejectedExecutionException) {
            activeBuilds.decrementAndGet();
            recordError(0, IndexBuildMetrics.ErrorType.REJECTED);
            LOG.warn("event=index_build_rejected indexId={} reason=executor_rejected", request.descriptor().indexId());
            future.complete(IndexBuildResult.rejected());
        }
    }

    private void runAndComplete(BuildRequest request, CompletableFuture<IndexBuildResult> future) {
        long startedAt = System.currentTimeMillis();
        buildStarted();
        try {
            future.complete(runBuild(request, startedAt));
        } catch (RuntimeException runtimeException) {
            LOG.error(
                    "event=index_build_failed indexId={} error={}",
                    request.descriptor().indexId(),
                    runtimeException.getMessage(),
                    runtimeException);
            recordError(System.currentTimeMillis() - startedAt, IndexBuildMetrics.ErrorType.UNKNOWN);
            future.completeExceptionally(runtimeException);
        } finally {
            buildFinished();
            activeBuilds.decrementAndGet();
        }
    }

    private IndexBuildResult runBuild(BuildRequest request, long startedAt) {
        IndexDescriptor descriptor = request.descriptor();
        if (!tryClaim(descriptor)) {
            LOG.info(
                    "event=index_build_claim_lost indexId={} generation={}",
                    descriptor.indexId(),
                    descriptor.generation());
            recordError(System.currentTimeMillis() - startedAt, IndexBuildMetrics.ErrorType.CLAIM_FAILED);
            return IndexBuildResult.claimFailed();
        }
        try {
            if (!isGenerationCurrent(descriptor)) {
                LOG.info(
                        "event=index_build_stale_generation indexId={} generation={}",
                        descriptor.indexId(),
                        descriptor.generation());
                recordError(System.currentTimeMillis() - startedAt, IndexBuildMetrics.ErrorType.STALE_GENERATION);
                return IndexBuildResult.staleGeneration();
            }
            return runClaimedBuild(request, startedAt);
        } finally {
            releaseClaim(descriptor);
        }
    }

    private IndexBuildResult runClaimedBuild(BuildRequest request, long startedAt) {
        IndexBuildResult result = buildFiles(request);
        long durationMs = System.currentTimeMillis() - startedAt;
        if (metrics != null) {
            metrics.recordBuild(durationMs, result.successCount(), result.failureCount());
        }
        if (result.isFullySuccessful()) {
            pinBuildSnapshot(request.descriptor());
        }
        LOG.info(
                "event=index_build_completed indexId={} succeeded={} failed={} durationMs={}",
                request.descriptor().indexId(),
                result.successCount(),
                result.failureCount(),
                durationMs);
        return result;
    }

    private IndexBuildResult buildFiles(BuildRequest request) {
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
            if (cancelledIndexes.contains(request.descriptor().indexId())) {
                LOG.info("event=index_build_stopped indexId={} reason=cancelled", request.descriptor().indexId());
                break;
            }
            buildOneFile(request, file, batch, tally);
            flushIfFull(request.mappingConsumer(), batch);
        }
        flush(request.mappingConsumer(), batch);
        return new IndexBuildResult(tally.succeeded, tally.failed);
    }

    private void buildOneFile(
            BuildRequest request, DataFileInfo file, List<BuildFileMapping> batch, BuildTally tally) {
        try {
            batch.add(fileBuilder.build(
                    request.descriptor(), file, request.fileSystemAccessor(), request.reader()));
            tally.succeeded++;
            heartbeat(request.descriptor());
        } catch (Exception exception) {
            tally.failed++;
            LOG.warn(
                    "event=index_build_file_failed indexId={} file={} error={}",
                    request.descriptor().indexId(),
                    file.filePath(),
                    exception.getMessage());
        }
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

    private boolean tryClaim(IndexDescriptor descriptor) {
        BuildClaimHandler handler = context.claimHandler();
        return handler == null
                || handler.tryClaim(descriptor.indexId(), descriptor.generation(), ownerId, Instant.now());
    }

    private void releaseClaim(IndexDescriptor descriptor) {
        BuildClaimHandler handler = context.claimHandler();
        if (handler == null) {
            return;
        }
        try {
            handler.release(descriptor.indexId(), descriptor.generation());
        } catch (RuntimeException runtimeException) {
            LOG.warn(
                    "event=index_build_claim_release_failed indexId={} error={}",
                    descriptor.indexId(),
                    runtimeException.getMessage());
        }
    }

    private void heartbeat(IndexDescriptor descriptor) {
        BuildClaimHandler handler = context.claimHandler();
        if (handler == null) {
            return;
        }
        try {
            handler.heartbeat(descriptor.indexId(), descriptor.generation(), Instant.now());
        } catch (RuntimeException runtimeException) {
            LOG.debug(
                    "event=index_build_heartbeat_failed indexId={} error={}",
                    descriptor.indexId(),
                    runtimeException.getMessage());
        }
    }

    private boolean isGenerationCurrent(IndexDescriptor descriptor) {
        GenerationChecker checker = context.generationChecker();
        return checker == null || checker.isCurrentGeneration(descriptor.indexId(), descriptor.generation());
    }

    private void pinBuildSnapshot(IndexDescriptor descriptor) {
        if (context.snapshotPinProvider() == null || descriptor.buildSnapshotId() == null) {
            return;
        }
        try {
            context.snapshotPinProvider().pinSnapshot(
                    descriptor.catalogName(),
                    descriptor.schemaName(),
                    descriptor.tableName(),
                    Long.parseLong(descriptor.buildSnapshotId()),
                    descriptor.indexId());
        } catch (NumberFormatException numberFormatException) {
            LOG.debug(
                    "event=index_build_pin_skipped indexId={} snapshotId={} reason=non_numeric",
                    descriptor.indexId(),
                    descriptor.buildSnapshotId());
        } catch (RuntimeException runtimeException) {
            LOG.warn(
                    "event=index_build_pin_failed indexId={} snapshotId={} error={}",
                    descriptor.indexId(),
                    descriptor.buildSnapshotId(),
                    runtimeException.getMessage());
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

    private void recordError(long durationMs, IndexBuildMetrics.ErrorType type) {
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

    private record PendingBuild(BuildRequest request, CompletableFuture<IndexBuildResult> future) {}

    private static final class BuildTally {
        private int succeeded;
        private int failed;
    }
}