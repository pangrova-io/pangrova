package org.pangrova.coordinator.streaming;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.streaming.StreamLifecycleState;
import org.pangrova.metadata.streaming.StreamState;
import org.pangrova.spi.streaming.CdcRecordConverter;
import org.pangrova.spi.streaming.CdcSourceReader;
import org.pangrova.spi.streaming.CdcWriteSink;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.pangrova.streaming.cdc.CdcPipelineConfig;
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

    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    /**
     * Holds a pipeline loop together with its target table name,
     * so {@link #stop(String)} can call {@code WatermarkTracker.removeTable()}.
     * BUG-D22 fix.
     */
    record PipelineEntry(CdcPipelineLoop loop, String targetTable) {}

    private final StreamSchedulerContext deps;
    private final ConcurrentHashMap<String, PipelineEntry> activePipelines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Duration> continuousQueries = new ConcurrentHashMap<>();
    private final ExecutorService pipelineExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** BUG-D16 fix: prevents new pipelines from starting between stopAll and resume. */
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private volatile String coordinatorId = "";
    /** Optional consumer that drives execution of registered continuous queries. */
    private volatile ContinuousQueryConsumer continuousQueryConsumer;

    public StreamScheduler(StreamSchedulerContext deps) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.pipelineExecutor = ManagedExecutorFactory.newFixedThreadPool(
                "pangrova-cdc-pipeline", deps.config().pipelineThreadPoolSize());
    }

    // Package-private constructor for testing with custom executor
    StreamScheduler(StreamSchedulerContext deps, ExecutorService executor) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.pipelineExecutor = Objects.requireNonNull(executor, "executor must not be null");
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

    public void setCoordinatorId(String coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    /**
     * Starts a pipeline for the given stream state.
     * If the stream's coordinatorId doesn't match, re-acquires it.
     */
    public void start(StreamState state) {
        if (closed.get()) {
            LOG.warn("event=stream_start_rejected stream={} reason=scheduler_closed", state.streamName());
            return;
        }
        // BUG-D16 fix: reject new pipelines while paused (leadership loss in progress)
        if (paused.get()) {
            LOG.warn("event=stream_start_rejected stream={} reason=scheduler_paused", state.streamName());
            return;
        }

        // Re-acquisition check: ensure this coordinator owns the stream
        StreamState currentState = state;
        if (!coordinatorId.isEmpty() && !coordinatorId.equals(state.coordinatorId())) {
            StreamState reacquired = state.withReacquisition(coordinatorId);
            try {
                deps.stateStore().saveStateIfEpoch(reacquired, state.epoch());
                currentState = reacquired;
                LOG.info("event=stream_reacquired stream={} newCoordinatorId={} newEpoch={}",
                        currentState.streamName(), coordinatorId, currentState.epoch());
            } catch (Exception e) {
                LOG.warn("event=stream_reacquisition_failed stream={} error={}",
                        state.streamName(), e.getMessage());
                return;
            }
        }

        // Effectively final for use in lambda below
        final StreamState effectiveState = currentState;
        String streamName = effectiveState.streamName();
        if (activePipelines.containsKey(streamName)) {
            LOG.info("event=stream_already_active stream={}", streamName);
            return;
        }

        CdcSourceReader reader = deps.sourceReaderFactory().apply(effectiveState);
        CdcWriteSink writeSink = deps.writeSinkFactory().apply(effectiveState);

        // BUG-013 fix: Load offsets here but defer seekToOffsets to CdcPipelineLoop.run()
        // so it executes on the pipeline thread. CdcSourceReader (e.g., KafkaConsumer)
        // is not thread-safe — seeking on the scheduler thread while the pipeline thread
        // polls would cause ConcurrentModificationException.
        Map<TopicPartitionOffset, Long> offsets = deps.offsetStore().loadOffsets(streamName);
        if (!offsets.isEmpty()) {
            LOG.info("event=stream_offset_loaded stream={} partitions={}", streamName, offsets.size());
        }

        CdcRecordConverter converter = deps.converterRegistry().get(effectiveState.sourceFormat());
        StreamConfig config = deps.config();

        int batchSize = parseIntProperty(effectiveState.streamProperties(), "batch_size", config.defaultBatchSize());
        int maxFailures = parseIntProperty(effectiveState.streamProperties(), "max_consecutive_failures",
                config.defaultMaxConsecutiveFailures());

        CdcPipelineConfig pipelineConfig = new CdcPipelineConfig(
                streamName,
                effectiveState.targetTable(),
                batchSize,
                Duration.ofSeconds(1),
                500L,
                maxFailures,
                5,
                Duration.parse(config.defaultFlushInterval()));

        CdcPipelineListener listener = ex -> {
            LOG.error("event=pipeline_failed stream={} error={}", streamName, ex.getMessage());
            StreamState failed = effectiveState.withError(ex.getMessage());
            deps.stateStore().saveState(failed);
            activePipelines.remove(streamName);
        };

        CdcQuotaEnforcer quotaEnforcer = new CdcQuotaEnforcer(
                config.defaultMaxConcurrentWrites(), config.defaultMaxPendingBytes());

        CdcPipelineLoopContext loopDeps = new CdcPipelineLoopContext(
                reader, converter, writeSink, quotaEnforcer,
                deps.watermarkTracker(), deps.maintenanceScheduler(),
                new CdcPipelineRuntimeHooks(listener, BackoffStrategy.retry(100, 30_000), null));

        CdcPipelineLoop loop = new CdcPipelineLoop(pipelineConfig, loopDeps, offsets);
        PipelineEntry entry = new PipelineEntry(loop, effectiveState.targetTable());
        PipelineEntry existing = activePipelines.putIfAbsent(streamName, entry);
        if (existing != null) {
            LOG.info("event=stream_already_active_concurrent stream={}", streamName);
            return;
        }
        pipelineExecutor.submit(loop);

        LOG.info("event=stream_pipeline_started stream={} target={}", streamName, effectiveState.targetTable());
    }

    /**
     * Stops a pipeline for the given stream.
     * BUG-D22 fix: also removes watermark tracking state for the target table.
     */
    public void stop(String streamName) {
        PipelineEntry entry = activePipelines.remove(streamName);
        if (entry != null) {
            entry.loop().stop();
            deps.watermarkTracker().removeTable(entry.targetTable());
            LOG.info("event=stream_pipeline_stopped stream={} targetTableRemoved={}",
                    streamName, entry.targetTable());
        }
    }

    /**
     * Resumes all RUNNING streams from the state store.
     * Called on HA leader election.
     */
    public void resumeAll() {
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
        paused.set(true);
        LOG.info("event=stream_stop_all count={}", activePipelines.size());
        for (Map.Entry<String, PipelineEntry> entry : activePipelines.entrySet()) {
            entry.getValue().loop().stop();
            deps.watermarkTracker().removeTable(entry.getValue().targetTable());
        }
        activePipelines.clear();
    }

    /**
     * Resumes accepting new pipelines after a stopAll.
     * Called on re-election as leader.
     */
    public void resume() {
        paused.set(false);
        LOG.info("event=stream_scheduler_resumed");
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        stopAll();
        ContinuousQueryConsumer cqConsumer = continuousQueryConsumer;
        if (cqConsumer != null) {
            cqConsumer.close();
            continuousQueryConsumer = null;
        }
        pipelineExecutor.shutdownNow();
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
