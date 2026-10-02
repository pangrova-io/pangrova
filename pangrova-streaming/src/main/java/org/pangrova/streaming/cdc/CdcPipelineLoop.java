package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcRecordConversionException;
import org.pangrova.spi.streaming.CdcWriteResult;
import org.pangrova.spi.streaming.SourceRecord;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.pangrova.streaming.cdc.commit.CdcCommitCoordinator;
import org.pangrova.streaming.cdc.commit.SourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.streaming.watermark.WatermarkPropagator;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Core CDC pipeline loop: poll → convert → write → commitOffsets → updateWatermark.
 *
 * <h2>Delivery Guarantee: At-Least-Once</h2>
 *
 * <p>This pipeline provides <strong>at-least-once</strong> delivery semantics, not exactly-once.
 * Each cycle writes converted CDC envelopes to the lake table via
 * {@link CdcPipelineLoopContext#writeSink()} and then commits source offsets via
 * {@link org.pangrova.spi.streaming.CdcSourceReader#commitOffsets(Map)}. The write-then-commit
 * ordering is intentional: it guarantees no data loss, at the cost of possible duplicates.</p>
 *
 * <h3>Crash Window (BUG-D08)</h3>
 *
 * <p>A crash or forced shutdown between {@code writeAndNotify} and {@code commitOffsets} creates
 * a duplicate-producing window:</p>
 * <ol>
 *   <li>{@code writeAndNotify(envelopes)} succeeds — data is durably written to the lake table.</li>
 *   <li>Process crashes (OOM, kill -9, coordinator failover, etc.).</li>
 *   <li>{@code commitOffsets} never executes — source offsets are not advanced.</li>
 *   <li>On restart, {@link org.pangrova.spi.streaming.CdcSourceReader} replays from the last
 *       committed offset, re-delivering the same records.</li>
 *   <li>The pipeline writes the same envelopes again — duplicates now exist in the lake table.</li>
 * </ol>
 *
 * <h3>Impact on Append-Only Tables</h3>
 *
 * <p>For append-only target tables (INSERT-only CDC streams), duplicates are <em>silent</em>:
 * no constraint violation, no error, no log entry. Downstream consumers (queries, aggregations,
 * materialized views) will observe inflated row counts and incorrect aggregation results.
 * Upsert-style tables with primary keys may naturally deduplicate via overwrite semantics,
 * but append-only tables have no such safety net.</p>
 *
 * <h3>Deduplication Recommendations</h3>
 *
 * <p>To achieve effectively-once semantics on top of at-least-once delivery:</p>
 * <ul>
 *   <li><strong>Dedup key:</strong> Use the composite key
 *       {@code (topic, partition, offset)} from {@link TopicPartitionOffset} as a deduplication
 *       identifier in the connector write path. Each source record carries a unique
 *       topic-partition-offset triple that is stable across replays.</li>
 *   <li><strong>Connector-level dedup:</strong> The {@link org.pangrova.spi.streaming.CdcWriteSink}
 *       implementation (or the underlying connector PageSink) can maintain a high-water-mark
 *       per partition and skip records whose offset is at or below the mark.</li>
 *   <li><strong>Query-time dedup:</strong> As a lighter alternative, downstream queries can
 *       deduplicate using {@code ROW_NUMBER() OVER (PARTITION BY dedup_key ORDER BY offset DESC)}
 *       or equivalent, at the cost of query-time overhead.</li>
 * </ul>
 *
 * <h3>Future Work: Exactly-Once via Idempotent Writes</h3>
 *
 * <p>A future enhancement could achieve exactly-once by co-locating offset commits with data
 * writes in a single atomic transaction (e.g., writing offsets into the lake table metadata
 * alongside the data files). This requires connector-level support for transactional offset
 * storage and is out of scope for the current implementation.</p>
 *
 * <h2>Runtime Behavior</h2>
 *
 * <p>Each instance runs on a dedicated thread managed by the StreamScheduler.
 * The loop integrates with {@link CdcQuotaEnforcer} for backpressure,
 * {@link org.pangrova.streaming.watermark.WatermarkTracker} for watermark tracking,
 * and {@link org.pangrova.streaming.maintenance.MaintenanceScheduler} for compaction scheduling.</p>
 *
 * <p>Concurrency strategy: the {@code running} flag is volatile for cross-thread visibility
 * (stop() called from scheduler thread, run() executes on pipeline thread).
 * {@code consecutiveFailures} is thread-confined to the pipeline thread and is
 * not accessed from other threads.</p>
 *
 * @see TopicPartitionOffset
 * @see org.pangrova.spi.streaming.CdcWriteSink
 * @see org.pangrova.spi.streaming.CdcSourceReader
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
        pipelineThread = Thread.currentThread();
        LOG.info("event=pipeline_started stream={} target={}",
                config.streamName(), config.targetTable());
        try {
            recoverInFlightTransactions();
            restoreInitialOffsets();
            runPipelineLoop();
        } finally {
            // F-STREAM-04: close the source reader on the pipeline thread. CdcSourceReader
            // (e.g. KafkaConsumer) is not thread-safe, so it must be closed by the same thread
            // that polls it — stop() only signals + interrupts. Without this the Kafka consumer
            // connection leaks after the pipeline stops.
            closeSourceReaderQuietly();
        }
        LOG.info("event=pipeline_stopped stream={}", config.streamName());
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
        SourceId source = new SourceId(config.streamName());
        coordinator.recover(source);
        LOG.info("event=pipeline_2pc_recovery_complete stream={}", config.streamName());
    }

    private void restoreInitialOffsets() {
        if (!initialOffsets.isEmpty()) {
            deps.sourceReader().seekToOffsets(initialOffsets);
            LOG.info("event=pipeline_offset_restored stream={} partitions={}",
                    config.streamName(), initialOffsets.size());
            return;
        }
        // When no explicit initial offsets and a commit coordinator is available,
        // restore from the JDBC state store. Ambiguous catalog publications
        // must have been reconciled before this path is reached.
        CdcCommitCoordinator coordinator = deps.commitCoordinator();
        if (coordinator != null) {
            SourceId source = new SourceId(config.streamName());
            Map<TopicPartitionOffset, Long> jdbcOffsets =
                    coordinator.findLastCommittedOffsets(source);
            if (!jdbcOffsets.isEmpty()) {
                deps.sourceReader().seekToOffsets(jdbcOffsets);
                LOG.info("event=pipeline_offset_restored_from_jdbc stream={} partitions={}",
                        config.streamName(), jdbcOffsets.size());
            }
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
        running = false;
        Thread t = pipelineThread;
        if (t != null) {
            t.interrupt();
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

        List<SourceRecord> batch = deps.sourceReader().pollBatch(
            config.batchSize(), config.pollTimeout());
        if (batch.isEmpty()) {
            return;
        }

        // Capture offsets up-front so handleFailure can attribute a write
        // failure to the specific offsets that were in flight.
        currentCycleOffsets = extractMaxOffsets(batch);

        List<CdcEnvelope> envelopes = convertBatch(batch);
        // F-STREAM-04: Propagate watermark INSIDE the 2PC path (atomically with
        // offset commit) to prevent watermark regression on crash between 2PC and
        // propagateWatermark. For non-2PC path, propagate after write.
        if (!envelopes.isEmpty()) {
            if (deps.commitCoordinator() != null) {
                twoPhaseCommitter.commitWithTwoPhaseProtocol(envelopes, currentCycleOffsets);
                // Watermark is now persisted as part of 2PC commit phase
            } else {
                writeAndNotify(envelopes);
                deps.sourceReader().commitOffsets(currentCycleOffsets);
                propagateWatermark(envelopes);
            }
        } else {
            // No envelopes after conversion (all dead-lettered); still commit offsets
            deps.sourceReader().commitOffsets(currentCycleOffsets);
        }

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

    private Map<TopicPartitionOffset, Long> extractMaxOffsets(List<SourceRecord> batch) {
        Map<TopicPartitionOffset, Long> offsets = new HashMap<>();
        for (SourceRecord record : batch) {
            TopicPartitionOffset key = new TopicPartitionOffset(record.topic(), record.partition());
            offsets.merge(key, record.offset() + 1, Math::max);
        }
        return offsets;
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
