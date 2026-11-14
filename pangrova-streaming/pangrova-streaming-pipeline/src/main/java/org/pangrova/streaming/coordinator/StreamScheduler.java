package org.pangrova.streaming.coordinator;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.cdc.CdcPipelineConfig;
import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.streaming.cdc.CdcPipelineListener;
import org.pangrova.streaming.cdc.CdcPipelineLoop;
import org.pangrova.streaming.cdc.CdcPipelineLoopContext;
import org.pangrova.streaming.cdc.CdcPipelineRuntimeHooks;
import org.pangrova.streaming.cdc.CdcQuotaEnforcer;
import org.pangrova.streaming.query.ContinuousQueryConsumer;
import org.pangrova.streaming.query.ContinuousQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of CDC pipeline threads.
 *
 * <p>Each stream runs on a dedicated thread in a named thread pool.
 * Supports start, stop, resumeAll (HA leader election), and stopAll
 * (leadership loss).</p>
 *
 * <p>Concurrency strategy: {@code activePipelines} is a ConcurrentHashMap.
 * The thread pool is created once and shared across all pipelines.</p>
 */
@ThreadSafe
public final class StreamScheduler implements AutoCloseable {
    private static final long CDC_POLL_INTERVAL_MS = 500L;
    private static final int CDC_RETRY_MAX_DELAY_MS = 30_000;

    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    /** One reserved stream slot; queued cancellation and execution have exclusive resource ownership. */
    private final class PipelineEntry implements Runnable {
        private final String streamName;
        private final String targetTable;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private CdcSourceReader reader;
        private CdcWriteSink sink;
        private volatile CdcPipelineLoop loop;
        private volatile StreamState state;

        PipelineEntry(StreamState state) {
            streamName = state.streamName();
            targetTable = state.targetTable();
        }

        void install(CdcPipelineLoop newLoop, CdcSourceReader newReader, CdcWriteSink newSink) {
            reader = newReader;
            sink = newSink;
            loop = newLoop;
        }

        void stop() {
            CdcPipelineLoop installed = loop;
            if (installed != null) {
                installed.stop();
                if (claimed.compareAndSet(false, true)) {
                    closeUnstarted(null, reader, sink);
                }
            }
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            try {
                loop.run();
            } catch (RuntimeException | Error failure) {
                pipelineFailed(this, state, failure);
                throw failure;
            } finally {
                activePipelines.remove(streamName, this);
                removeUnusedWatermark(targetTable);
            }
        }
    }

    private final StreamSchedulerContext deps;
    private final ConcurrentHashMap<String, PipelineEntry> activePipelines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Duration> continuousQueries = new ConcurrentHashMap<>();
    private final ExecutorService pipelineExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** BUG-D16 fix: prevents new pipelines from starting between stopAll and resume. */
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final String coordinatorId;
    private final Object lifecycleLock = new Object();
    /** Optional consumer that drives execution of registered continuous queries. */
    private volatile ContinuousQueryConsumer continuousQueryConsumer;

    /** Creates a scheduler with the durable identity of its coordinator process. */
    public StreamScheduler(StreamSchedulerContext deps, String coordinatorId) {
        this(deps, requireCoordinatorId(coordinatorId), ManagedExecutorFactory.newFixedThreadPool(
                "pangrova-cdc-pipeline", deps.config().pipelineThreadPoolSize()));
    }

    public StreamScheduler(StreamSchedulerContext deps, String coordinatorId, ExecutorService executor) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.coordinatorId = requireCoordinatorId(coordinatorId);
        this.pipelineExecutor = Objects.requireNonNull(executor, "executor must not be null");
    }

    private static String requireCoordinatorId(String coordinatorId) {
        if (coordinatorId == null || coordinatorId.isBlank()) {
            throw new IllegalArgumentException("coordinatorId must not be blank");
        }
        return coordinatorId;
    }

    /**
     * Starts the continuous query consumer that polls the registration map and
     * activates refresh loops. Idempotent — calling more than once has no effect.
     *
     * @param executor      executor that performs the actual incremental refresh per CQ
     * @param scanInterval  how often to scan the registration map for new entries
     */
    public synchronized void startContinuousQueryConsumer(
            ContinuousQueryExecutor executor, Duration scanInterval) {
        if (closed.get()) {
            throw new IllegalStateException("Stream scheduler is closed");
        }
        if (continuousQueryConsumer != null) {
            return;
        }
        Objects.requireNonNull(executor, "executor must not be null");
        Objects.requireNonNull(scanInterval, "scanInterval must not be null");
        continuousQueryConsumer = new ContinuousQueryConsumer(
                continuousQueries, executor, scanInterval);
        continuousQueryConsumer.start();
        LOG.info("event=continuous_query_consumer_started scanInterval={}", scanInterval);
    }

    /** Returns the active continuous query consumer for inspection/testing. */
    public ContinuousQueryConsumer continuousQueryConsumer() {
        return continuousQueryConsumer;
    }

    /** Reserves a local slot before claiming a new durable attempt or allocating source resources. */
    public void start(StreamState state) {
        PipelineEntry entry = new PipelineEntry(state);
        synchronized (lifecycleLock) {
            if (closed.get() || paused.get()
                    || activePipelines.putIfAbsent(state.streamName(), entry) != null) {
                return;
            }
        }
        StreamState claimed = state.withReacquisition(coordinatorId);
        try {
            deps.stateStore().saveStateIfEpoch(claimed, state.epoch());
        } catch (org.pangrova.common.exception.StaleEpochException stale) {
            activePipelines.remove(state.streamName(), entry);
            LOG.info("event=stream_claim_lost stream={}", state.streamName());
            return;
        } catch (RuntimeException | Error failure) {
            activePipelines.remove(state.streamName(), entry);
            throw failure;
        }
        entry.state = claimed;
        try {
            preparePipeline(entry, claimed);
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, entry::stop, () -> pipelineFailed(entry, claimed, failure));
            throw failure;
        }
    }

    private void preparePipeline(PipelineEntry entry, StreamState effectiveState) {
        String streamName = effectiveState.streamName();
        CdcRecordConverter converter = deps.converterRegistry().get(effectiveState.sourceFormat());
        CdcSourceReader reader = null;
        CdcWriteSink writeSink = null;
        boolean transferred = false;
        Throwable primary = null;
        try {
            reader = deps.sourceReaderFactory().apply(effectiveState);
            writeSink = deps.writeSinkFactory().apply(effectiveState);
            Map<TopicPartitionOffset, Long> offsets = deps.offsetStore().loadOffsets(streamName);
            CdcPipelineLoop loop = createLoop(entry, effectiveState, converter, reader, writeSink, offsets);
            synchronized (lifecycleLock) {
                if (closed.get() || paused.get() || activePipelines.get(streamName) != entry) {
                    return;
                }
                entry.install(loop, reader, writeSink);
                transferred = true;
                pipelineExecutor.execute(entry);
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (!transferred) {
                closeUnstarted(primary, reader, writeSink);
            }
        }
    }

    void requireSupportedFormat(String format) {
        deps.converterRegistry().get(format);
    }

    private CdcPipelineLoop createLoop(PipelineEntry entry, StreamState effectiveState, CdcRecordConverter converter,
            CdcSourceReader reader, CdcWriteSink writeSink, Map<TopicPartitionOffset, Long> offsets) {
        String streamName = effectiveState.streamName();
        StreamConfig config = deps.config();

        int batchSize = parseIntProperty(effectiveState.streamProperties(), "batch_size", config.defaultBatchSize());
        int maxFailures = parseIntProperty(effectiveState.streamProperties(), "max_consecutive_failures",
                config.defaultMaxConsecutiveFailures());

        CdcPipelineConfig pipelineConfig = new CdcPipelineConfig(
                streamName,
                new SourceId(effectiveState.checkpointId()),
                effectiveState.targetTable(),
                batchSize,
                Duration.ofSeconds(1),
                CDC_POLL_INTERVAL_MS,
                maxFailures,
                5,
                Duration.parse(config.defaultFlushInterval()));

        CdcPipelineListener listener = failure -> pipelineFailed(entry, effectiveState, failure);

        CdcQuotaEnforcer quotaEnforcer = new CdcQuotaEnforcer(
                config.defaultMaxConcurrentWrites(), config.defaultMaxPendingBytes());

        CdcPipelineLoopContext loopDeps = new CdcPipelineLoopContext(
                reader, converter, writeSink, quotaEnforcer,
                deps.watermarkTracker(), deps.maintenanceScheduler(),
                new CdcPipelineRuntimeHooks(
                        listener, BackoffStrategy.retry(100, CDC_RETRY_MAX_DELAY_MS), deps.commitCoordinator()));

        return new CdcPipelineLoop(pipelineConfig, loopDeps, offsets);
    }

    private void pipelineFailed(PipelineEntry entry, StreamState state, Throwable failure) {
        if (activePipelines.get(state.streamName()) != entry) {
            return;
        }
        try {
            StreamState failed = state.withError(failure.getMessage()).toBuilder()
                    .epoch(Math.incrementExact(state.epoch())).build();
            deps.stateStore().saveStateIfEpoch(failed, state.epoch());
        } catch (org.pangrova.common.exception.StaleEpochException stale) {
            LOG.info("event=stream_failure_callback_stale stream={}", state.streamName());
        } catch (RuntimeException persistenceFailure) {
            failure.addSuppressed(persistenceFailure);
            LOG.error("event=stream_failure_persistence_failed stream={}", state.streamName(), persistenceFailure);
        } finally {
            activePipelines.remove(state.streamName(), entry);
            removeUnusedWatermark(entry.targetTable);
        }
    }

    /** Stops the exact locally registered attempt, including one still being prepared or queued. */
    public void stop(String streamName) {
        PipelineEntry entry;
        synchronized (lifecycleLock) {
            entry = activePipelines.remove(streamName);
        }
        if (entry != null) {
            stopEntry(entry);
        }
    }

    /** Reconciles remote DDL changes and discovers RUNNING streams while locally active. */
    public void reconcile() {
        List<PipelineEntry> entries;
        synchronized (lifecycleLock) {
            if (closed.get() || paused.get()) return;
            entries = List.copyOf(activePipelines.values());
        }
        for (PipelineEntry entry : entries) {
            StreamState attempt = entry.state;
            if (attempt == null) continue;
            var current = deps.stateStore().loadState(entry.streamName);
            boolean unchanged = current.filter(state -> state.lifecycleState() == StreamLifecycleState.RUNNING
                    && state.epoch() == attempt.epoch()
                    && state.checkpointId().equals(attempt.checkpointId())
                    && state.tenantId().equals(attempt.tenantId())).isPresent();
            if (!unchanged && activePipelines.remove(entry.streamName, entry)) {
                stopEntry(entry);
            }
        }
        resumeAll();
    }

    private void stopEntry(PipelineEntry entry) {
        ResourceCloser.closeAll(null, entry::stop, () -> removeUnusedWatermark(entry.targetTable));
    }

    private void removeUnusedWatermark(String targetTable) {
        synchronized (lifecycleLock) {
            if (activePipelines.values().stream().noneMatch(entry -> entry.targetTable.equals(targetTable))) {
                deps.watermarkTracker().removeTable(targetTable);
            }
        }
    }

    private static void closeUnstarted(Throwable primary, CdcSourceReader reader, CdcWriteSink sink) {
        ResourceCloser.closeAll(primary, ResourceCloser.present(sink, CdcWriteSink::close),
                ResourceCloser.present(reader, CdcSourceReader::close));
    }

    /**
     * Resumes all RUNNING streams from the state store.
     * Called on HA leader election.
     */
    public void resumeAll() {
        if (closed.get() || paused.get()) return;
        List<StreamState> runningStreams = deps.stateStore().listByState(StreamLifecycleState.RUNNING);
        LOG.info("event=stream_resume_all count={}", runningStreams.size());
        for (StreamState state : runningStreams) {
            try {
                start(state);
            } catch (Exception e) {
                LOG.error("event=stream_resume_failed stream={} error={}",
                        state.streamName(), e.getMessage(), e);
            }
        }
    }

    /**
     * Stops all running pipelines. Called on leadership loss.
     * BUG-D16 fix: sets paused flag to prevent concurrent start() from adding
     * new pipelines between the stop loop and clear().
     * BUG-D22 fix: also removes watermark tracking state for each target table.
     */
    public void stopAll() {
        List<PipelineEntry> entries;
        synchronized (lifecycleLock) {
            paused.set(true);
            entries = List.copyOf(activePipelines.values());
            activePipelines.clear();
        }
        ResourceCloser.closeAll(null, entries.stream()
                .map(entry -> (Runnable) () -> stopEntry(entry))
                .toArray(Runnable[]::new));
    }

    /** Accepts new work again after leadership is regained. */
    public void resume() {
        synchronized (lifecycleLock) {
            if (!closed.get()) {
                paused.set(false);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ContinuousQueryConsumer consumer;
        synchronized (this) {
            consumer = continuousQueryConsumer;
            continuousQueryConsumer = null;
        }
        ResourceCloser.closeAll(null, this::stopAll,
                ResourceCloser.present(consumer, ContinuousQueryConsumer::close), pipelineExecutor::shutdownNow);
        LOG.info("event=stream_scheduler_closed");
    }

    public boolean isActive(String streamName) {
        return activePipelines.containsKey(streamName);
    }

    public int activeCount() {
        return activePipelines.size();
    }

    /**
     * Registers a continuous query for watermark-based auto-refresh.
     * Subscribes to {@link WatermarkTracker} for the source table and triggers
     * incremental refresh when watermark advances beyond the threshold.
     *
     * @param cqName          continuous query name
     * @param refreshInterval refresh interval (how often to check watermark advance)
     */
    public void registerContinuousQuery(String cqName, Duration refreshInterval) {
        if (closed.get() || paused.get()) {
            LOG.warn("event=continuous_query_rejected cqName={} reason={}",
                    cqName, closed.get() ? "scheduler_closed" : "scheduler_paused");
            return;
        }
        continuousQueries.put(cqName, refreshInterval);
        LOG.info("event=continuous_query_registered cqName={} refreshInterval={}", cqName, refreshInterval);
    }

    /**
     * Unregisters a continuous query.
     *
     * @param cqName continuous query name
     */
    public void unregisterContinuousQuery(String cqName) {
        Duration removed = continuousQueries.remove(cqName);
        if (removed != null) {
            LOG.info("event=continuous_query_unregistered cqName={}", cqName);
        }
    }

    /**
     * Returns the number of registered continuous queries.
     */
    public int continuousQueryCount() {
        return continuousQueries.size();
    }

    private static int parseIntProperty(Map<String, String> props, String key, int defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
