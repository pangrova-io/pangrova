package org.pangrova.streaming.core.cdc;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.connector.spi.data.write.model.WriteResult;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConversionException;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.core.commit.CdcCommitCoordinator;
import org.pangrova.streaming.core.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.core.commit.SourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.streaming.core.watermark.WatermarkPropagator;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Core CDC pipeline loop: poll → convert → write → commitOffsets → updateWatermark.
 *
 * <p>With a commit coordinator, each cycle persists its complete source checkpoint,
 * publishes the target write, marks the checkpoint committed, and acknowledges the
 * source. Recovery uses exact recorded topics and partitions. An ambiguous catalog
 * publication blocks progress pending reconciliation; catalog and JDBC commits
 * are not one transaction. Without a commit coordinator, the caller explicitly
 * selects at-least-once write/acknowledgement behavior.</p>
 *
 * <p>The exact polled batch is retained across failures. A later batch must never
 * advance source offsets past a failed write. Poison batches stop the pipeline
 * after the configured retry limit instead of discarding records.</p>
 *
 * <h2>Runtime Behavior</h2>
 *
 * <p>Each instance runs on a dedicated thread managed by the StreamScheduler.
 * The loop integrates with {@link CdcQuotaEnforcer} for backpressure,
 * {@link org.pangrova.streaming.core.watermark.WatermarkTracker} for watermark tracking,
 * and {@link org.pangrova.streaming.core.maintenance.MaintenanceScheduler} for compaction scheduling.</p>
 *
 * <p>Concurrency strategy: the {@code running} flag is volatile for cross-thread visibility
 * (stop() called from scheduler thread, run() executes on pipeline thread).
 * {@code consecutiveFailures} is thread-confined to the pipeline thread and is
 * not accessed from other threads.</p>
 *
 * @see TopicPartitionOffset
 * @see org.pangrova.streaming.api.cdc.CdcWriteSink
 * @see org.pangrova.streaming.api.cdc.CdcSourceReader
 */
@ThreadSafe
public final class CdcPipelineLoop implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CdcPipelineLoop.class);

    private final CdcPipelineConfig config;
    private final CdcPipelineLoopContext deps;
    private final CdcTwoPhaseCommitter twoPhaseCommitter;
    private final Map<TopicPartitionOffset, Long> initialOffsets;
    private volatile boolean running = true;
    private volatile Thread pipelineThread;
    private final Object threadLifecycleLock = new Object();
    private int consecutiveFailures;

    /**
     * Per-offset retry counter. Tracks how many times the pipeline has failed
     * on the same set of offsets (i.e., without making forward progress).
     * Reset to 0 whenever offsets advance (successful cycle).
     * Thread-confined to the pipeline thread.
     */
    private int retriesAtCurrentOffset;

    /**
     * Offsets attempted in the current (in-flight) cycle. Captured at the start
     * of executeCycle so that handleFailure can compare against the previous
     * failed offsets and only increment {@link #retriesAtCurrentOffset} when
     * the same offsets keep failing (true poison-pill scenario).
     * Empty when no batch has been polled yet (e.g., poll itself failed).
     * Thread-confined to the pipeline thread.
     */
    private Map<TopicPartitionOffset, Long> currentCycleOffsets = Map.of();

    // Retain the exact polled batch until publication and acknowledgement succeed.
    // Polling again after a write failure can advance past uncommitted records.
    private List<SourceRecord> pendingBatch = List.of();

    /**
     * Offsets that caused the previous failure. Compared against
     * {@link #currentCycleOffsets} in handleFailure to distinguish
     * "stuck at same offset" (true poison pill) from "many distinct
     * batches all failed" (transient sink failure across forward-progressing offsets).
     * Thread-confined to the pipeline thread.
     */
    private Map<TopicPartitionOffset, Long> lastFailedOffsets = Map.of();

    public CdcPipelineLoop(CdcPipelineConfig config, CdcPipelineLoopContext deps) {
        this(config, deps, Map.of());
    }

    /**
     * @param initialOffsets offsets to seek to before the first poll cycle.
     *                       BUG-013 fix: seek is deferred to run() so it executes
     *                       on the pipeline thread, avoiding cross-thread access to
     *                       the non-thread-safe CdcSourceReader (e.g., KafkaConsumer).
     */
    public CdcPipelineLoop(CdcPipelineConfig config, CdcPipelineLoopContext deps,
                            Map<TopicPartitionOffset, Long> initialOffsets) {
        this.config = config;
        this.deps = deps;
        this.twoPhaseCommitter = new CdcTwoPhaseCommitter(config, deps);
        this.initialOffsets = initialOffsets != null ? Map.copyOf(initialOffsets) : Map.of();
    }

    @Override
    public void run() {
        synchronized (threadLifecycleLock) {
            pipelineThread = Thread.currentThread();
        }
        LOG.info("event=pipeline_started stream={} target={}",
                config.streamName(), config.targetTable());
        try {
            if (!running || !restoreCheckpoint()) {
                return;
            }
            runPipelineLoop();
        } finally {
            pendingBatch = List.of();
            // F-STREAM-04: close the source reader on the pipeline thread. CdcSourceReader
            // (e.g. KafkaConsumer) is not thread-safe, so it must be closed by the same thread
            // that polls it — stop() only signals + interrupts. Without this the Kafka consumer
            // connection leaks after the pipeline stops.
            try {
                closeSourceReaderQuietly();
            } finally {
                try {
                    deps.writeSink().close();
                } catch (Exception failure) {
                    LOG.warn("event=cdc_write_sink_close_error stream={}", config.streamName(), failure);
                } finally {
                    synchronized (threadLifecycleLock) {
                        pipelineThread = null;
                        running = false;
                    }
                }
            }
        }
        LOG.info("event=pipeline_stopped stream={}", config.streamName());
    }

    private boolean restoreCheckpoint() {
        try {
            recoverInFlightTransactions();
            restoreInitialOffsets();
            return true;
        } catch (RuntimeException failure) {
            running = false;
            LOG.error("event=pipeline_recovery_failed stream={}", config.streamName(), failure);
            deps.listener().onFailed(failure);
            return false;
        }
    }

    private void closeSourceReaderQuietly() {
        try {
            deps.sourceReader().close();
        } catch (Exception e) {
            LOG.warn("event=cdc_source_reader_close_error stream={} error={}",
                    config.streamName(), e.getMessage());
        }
    }

    /**
     * Recovers any in-flight 2PC transactions from a prior crash and resolves
     * the authoritative start offsets from the JDBC commit state store.
     * Called before the pipeline loop begins.
     */
    private void recoverInFlightTransactions() {
        CdcCommitCoordinator coordinator = deps.commitCoordinator();
        if (coordinator == null) {
            return;
        }
        SourceId source = config.sourceId();
        coordinator.recover(source);
        LOG.info("event=pipeline_2pc_recovery_complete stream={}", config.streamName());
    }

    private void restoreInitialOffsets() {
        Map<TopicPartitionOffset, Long> offsets = new java.util.HashMap<>(initialOffsets);
        // Durable publication checkpoints take precedence over cached startup offsets.
        CdcCommitCoordinator coordinator = deps.commitCoordinator();
        if (coordinator != null) {
            SourceId source = config.sourceId();
            offsets.putAll(coordinator.findLastCommittedOffsets(source));
        }
        if (!offsets.isEmpty()) {
            deps.sourceReader().seekToOffsets(Map.copyOf(offsets));
            LOG.info("event=pipeline_offset_restored stream={} partitions={}",
                    config.streamName(), offsets.size());
        }
    }

    private void runPipelineLoop() {
        while (running) {
            if (deps.quotaEnforcer().shouldPause()) {
                sleepQuietly(config.pollPauseIntervalMs());
                continue;
            }
            try {
                executeCycle();
            } catch (RuntimeException e) {
                handleFailure(e);
                if (!running) {
                    return;
                }
            }
        }
    }

    /**
     * Signals the pipeline to stop after the current cycle completes.
     *
     * <p>BUG-D23 fix: In addition to setting the running flag, interrupts the
     * pipeline thread so that blocking operations (pollBatch, writeAndNotify)
     * are promptly unblocked rather than waiting for their full timeout.</p>
     */
    public void stop() {
        synchronized (threadLifecycleLock) {
            running = false;
            Thread t = pipelineThread;
            if (t != null) {
                t.interrupt();
            }
        }
    }

    /**
     * Returns whether the pipeline is still running.
     */
    public boolean isRunning() {
        return running;
    }

    private void executeCycle() {
        // Reset before pollBatch so a poll failure is not attributed to a stale
        // set of offsets from a previous cycle.
        currentCycleOffsets = Map.of();

        if (pendingBatch.isEmpty()) {
            pendingBatch = List.copyOf(deps.sourceReader().pollBatch(
                    config.batchSize(), config.pollTimeout()));
        }
        List<SourceRecord> batch = pendingBatch;
        if (batch.isEmpty()) {
            return;
        }

        // Capture offsets up-front so handleFailure can attribute a write
        // failure to the specific offsets that were in flight.
        CdcBatchCheckpoint checkpoint = CdcBatchCheckpoint.fromRecords(batch);
        currentCycleOffsets = checkpoint.nextOffsets();

        List<CdcEnvelope> envelopes = convertBatch(batch);
        if (deps.commitCoordinator() != null) {
            twoPhaseCommitter.commitWithTwoPhaseProtocol(envelopes, checkpoint);
            propagateWatermark(envelopes);
        } else {
            if (!envelopes.isEmpty()) {
                writeAndNotify(envelopes);
            }
            deps.sourceReader().commitOffsets(currentCycleOffsets);
            propagateWatermark(envelopes);
        }

        pendingBatch = List.of();
        consecutiveFailures = 0;
        retriesAtCurrentOffset = 0;
        lastFailedOffsets = Map.of();
        deps.retryBackoff().reset();
    }

    private List<CdcEnvelope> convertBatch(List<SourceRecord> batch) {
        List<byte[]> payloads = batch.stream()
            .map(SourceRecord::value)
            .toList();
        return deps.converter().convertBatch(
            payloads,
            (payload, ex) -> {
                if (ex instanceof CdcRecordConversionException conversionEx) {
                    throw conversionEx;
                }
                throw new CdcRecordConversionException(
                    "CDC format conversion failed for stream=" + config.streamName()
                        + " format=" + deps.converter().formatId(),
                    ex);
            });
    }

    private void writeAndNotify(List<CdcEnvelope> envelopes) {
        long estimatedBytes = twoPhaseCommitter.estimateBatchBytes(envelopes);
        deps.quotaEnforcer().onWriteStart(estimatedBytes);
        try {
            CdcWriteResult result = deps.writeSink().writeBatch(envelopes);

            WriteResult maintenanceResult = new WriteResult(
                config.targetTable(),
                result.rowsWritten(),
                result.dataFilesProduced(),
                0,
                0,
                Instant.now());
            deps.maintenanceScheduler().onWriteComplete(config.targetTable(), maintenanceResult);
        } finally {
            deps.quotaEnforcer().onWriteComplete(estimatedBytes);
        }
    }

    private void handleFailure(RuntimeException e) {
        consecutiveFailures++;
        // Only count this as a same-offset retry when the in-flight offsets
        // match the previously failed offsets — i.e., a real poison-pill
        // scenario. When offsets advance between failures (transient sink
        // failure across distinct batches), reset the per-offset counter so
        // only consecutiveFailures governs the failure trigger.
        if (!currentCycleOffsets.isEmpty() && currentCycleOffsets.equals(lastFailedOffsets)) {
            retriesAtCurrentOffset++;
        } else {
            retriesAtCurrentOffset = currentCycleOffsets.isEmpty() ? 0 : 1;
            lastFailedOffsets = currentCycleOffsets;
        }
        LOG.error("event=pipeline_cycle_failed stream={} consecutiveFailures={} retriesAtCurrentOffset={} error={}",
                config.streamName(), consecutiveFailures, retriesAtCurrentOffset, e.getMessage(), e);

        if (retriesAtCurrentOffset >= config.maxRetriesPerOffset()) {
            LOG.error("event=pipeline_poison_offset stream={} retriesAtOffset={}",
                    config.streamName(), retriesAtCurrentOffset);
            running = false;
            deps.listener().onFailed(e);
            return;
        }

        if (consecutiveFailures >= config.maxConsecutiveFailures()) {
            LOG.error("event=pipeline_failed stream={} totalFailures={}",
                    config.streamName(), consecutiveFailures);
            running = false;
            deps.listener().onFailed(e);
            return;
        }
        sleepQuietly(deps.retryBackoff().next());
    }

    /**
     * Propagates watermark through the CDC ingest path using WatermarkPropagator.
     * Computes the pass-through watermark from the batch's max event time and
     * updates the tracker.
     */
    private void propagateWatermark(List<CdcEnvelope> envelopes) {
        if (envelopes.isEmpty()) {
            return;
        }
        Instant maxEventTime = envelopes.stream()
            .map(CdcEnvelope::eventTime)
            .max(Instant::compareTo)
            .orElse(null);
        if (maxEventTime != null) {
            Instant propagated = WatermarkPropagator.propagatePassThrough(maxEventTime);
            deps.watermarkTracker().updateWatermark(config.targetTable(), propagated);
        }
    }

    /**
     * Sleeps for the given duration; if the thread is interrupted the interrupt
     * flag is restored and the pipeline is signalled to stop.
     */
    private void sleepQuietly(long millis) {
        ThreadUtils.sleepQuietly(millis);
        if (Thread.currentThread().isInterrupted()) {
            running = false;
        }
    }
}
